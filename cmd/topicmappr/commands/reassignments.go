package commands

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/DataDog/kafka-kit/v4/kafkazk"
	"github.com/DataDog/kafka-kit/v4/mapper"

	"github.com/spf13/cobra"
)

// reassignmentBundle holds a reassignment PartitionMap along with some input
// parameters used to generate the map and expected results in broker storage
// usage if the map were to be applied.
type reassignmentBundle struct {
	// The expected broker free storage range.
	storageRange float64
	// The expected broker free storage std. deviation.
	stdDev float64
	// The tolerance value used in the storage based partition reassignment.
	tolerance float64
	// The reassignment PartitionMap.
	partitionMap *mapper.PartitionMap
	// Partition relocations that constitute the reassignment.
	relocations map[int][]relocation
	// The brokers that the PartitionMap is assigning brokers to.
	brokers mapper.BrokerMap
}

type reassignParams struct {
	brokers                []int
	localityScoped         bool
	maxMetadataAge         int
	optimizeLeadership     bool
	partitionLimit         int
	partitionSizeThreshold int
	storageThreshold       float64
	storageThresholdGB     float64
	tolerance              float64
	topics                 []*regexp.Regexp
	topicsExclude          []*regexp.Regexp
	requireNewBrokers      bool
	verbose                bool
}

func (s reassignParams) UseFixedTolerance() bool { return s.tolerance != 0.00 }

func reassignParamsFromCmd(cmd *cobra.Command) (params reassignParams) {
	brokers, _ := cmd.Flags().GetString("brokers")
	params.brokers = brokerStringToSlice(brokers)
	localityScoped, _ := cmd.Flags().GetBool("locality-scoped")
	params.localityScoped = localityScoped
	maxMetadataAge, _ := cmd.Flags().GetInt("metrics-age")
	params.maxMetadataAge = maxMetadataAge
	optimizeLeadership, _ := cmd.Flags().GetBool("optimize-leadership")
	params.optimizeLeadership = optimizeLeadership
	partitionLimit, _ := cmd.Flags().GetInt("partition-limit")
	params.partitionLimit = partitionLimit
	partitionSizeThreshold, _ := cmd.Flags().GetInt("partition-size-threshold")
	params.partitionSizeThreshold = partitionSizeThreshold
	storageThreshold, _ := cmd.Flags().GetFloat64("storage-threshold")
	params.storageThreshold = storageThreshold
	storageThresholdGB, _ := cmd.Flags().GetFloat64("storage-threshold-gb")
	params.storageThresholdGB = storageThresholdGB
	tolerance, _ := cmd.Flags().GetFloat64("tolerance")
	params.tolerance = tolerance
	topics, _ := cmd.Flags().GetString("topics")
	params.topics = topicRegex(topics)
	topicsExclude, _ := cmd.Flags().GetString("topics-exclude")
	params.topicsExclude = topicRegex(topicsExclude)
	verbose, _ := cmd.Flags().GetBool("verbose")
	params.verbose = verbose
	return params
}

func reassign(params reassignParams, zk kafkazk.Handler) ([]*mapper.PartitionMap, []error) {
	// Get broker and partition metadata.
	if err := checkMetaAge(zk, params.maxMetadataAge); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	brokerMeta, errs := getBrokerMeta(zk, true)
	if errs != nil && brokerMeta == nil {
		for _, e := range errs {
			fmt.Println(e)
		}
		os.Exit(1)
	}
	partitionMeta, err := getPartitionMeta(zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Get the current partition map.
	partitionMapIn, err := kafkazk.PartitionMapFromZK(params.topics, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Exclude any topics that are pending deletion.
	pending, err := stripPendingDeletes(partitionMapIn, zk)
	if err != nil {
		fmt.Println("Error fetching topics pending deletion")
	}

	// Exclude any explicit exclusions.
	excluded := removeTopics(partitionMapIn, params.topicsExclude)

	// Print topics matched to input params.
	printTopics(partitionMapIn)

	// Print if any topics were excluded due to pending deletion.
	printExcludedTopics(pending, excluded)

	// Get a broker map.
	brokersIn := mapper.BrokerMapFromPartitionMap(partitionMapIn, brokerMeta, false)

	// Validate all broker params, get a copy of the broker IDs targeted for
	// partition offloading.
	if errs := validateBrokers(params.brokers, brokersIn, brokerMeta, params.requireNewBrokers); len(errs) > 0 {
		for _, e := range errs {
			fmt.Println(e)
		}
		os.Exit(1)
	}

	// Get offload targets.
	offloadTargets := determineOffloadTargets(params, brokersIn)

	// Sort offloadTargets by storage free ascending.
	sort.Sort(offloadTargetsBySize{t: offloadTargets, bm: brokersIn})

	// Generate reassignmentBundles for a rebalance.
	results := computeReassignmentBundles(
		params,
		partitionMapIn,
		partitionMeta,
		brokersIn,
		offloadTargets,
	)

	// Merge all results into a slice.
	resultsByRange := []reassignmentBundle{}
	for r := range results {
		resultsByRange = append(resultsByRange, r)
	}

	// Sort the rebalance results by range ascending.
	sort.Slice(resultsByRange, func(i, j int) bool {
		switch {
		case resultsByRange[i].storageRange < resultsByRange[j].storageRange:
			return true
		case resultsByRange[i].storageRange > resultsByRange[j].storageRange:
			return false
		}

		return resultsByRange[i].stdDev < resultsByRange[j].stdDev
	})

	// Chose the results with the lowest range.
	m := resultsByRange[0]
	partitionMapOut, brokersOut, relos := m.partitionMap, m.brokers, m.relocations

	// Print parameters used for rebalance decisions.
	printReassignmentParams(params, resultsByRange, brokersIn, m.tolerance)

	// Optimize leaders.
	if params.optimizeLeadership {
		partitionMapOut.OptimizeLeaderFollower()
	}

	// Print planned relocations.
	printPlannedRelocations(offloadTargets, relos, partitionMeta)

	// Print map change results.
	printMapChanges(partitionMapIn, partitionMapOut)

	// Print broker assignment statistics.
	errs = printBrokerAssignmentStats(partitionMapIn, partitionMapOut, brokersIn, brokersOut, true, 1.0)

	// Ignore no-ops; rebalances will naturally have a high percentage of these.
	partitionMapIn, partitionMapOut = skipReassignmentNoOps(partitionMapIn, partitionMapOut)

	return []*mapper.PartitionMap{partitionMapOut}, errs

}

// computeReassignmentBundles takes computeReassignmentBundlesParams and returns
// a chan reassignmentBundle. The channel will either contain a single reassignmentBundle
// if a fixed computeReassignmentBundlesParams.tolerance value (non 0.00) is
// specified, otherwise it will contain a series of reassignmentBundle for multiple
// interval values. When generating a series, the results are computed in parallel.
func computeReassignmentBundles(
	params reassignParams,
	partitionMap *mapper.PartitionMap,
	partitionMeta mapper.PartitionMetaMap,
	brokerMap mapper.BrokerMap,
	offloadTargets []int,
) chan reassignmentBundle {
	otm := map[int]struct{}{}
	for _, id := range offloadTargets {
		otm[id] = struct{}{}
	}

	results := make(chan reassignmentBundle, 100)
	wg := &sync.WaitGroup{}

	// Compute a reassignmentBundle output for all tolerance values 0.01..0.99 in parallel.
	for i := 0.01; i < 0.99; i += 0.01 {
		// Whether we're using a fixed tolerance (non 0.00 value) set via flag or
		// interval values.
		var tol float64

		if params.UseFixedTolerance() {
			tol = params.tolerance
		} else {
			tol = i
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			partitionMap := partitionMap.Copy()

			// Bundle planRelocationsForBrokerParams.
			relocationParams := planRelocationsForBrokerParams{
				relos:                  map[int][]relocation{},
				mappings:               partitionMap.Mappings(),
				brokers:                brokerMap.Copy(),
				partitionMeta:          partitionMeta,
				plan:                   relocationPlan{},
				topPartitionsLimit:     params.partitionLimit,
				partitionSizeThreshold: params.partitionSizeThreshold,
				offloadTargetsMap:      otm,
				tolerance:              tol,
				localityScoped:         params.localityScoped,
				verbose:                params.verbose,
			}

			// Iterate over offload targets, planning at most one relocation per iteration.
			// Continue this loop until no more relocations can be planned.
			for exhaustedCount := 0; exhaustedCount < len(offloadTargets); {
				relocationParams.pass++
				for _, sourceID := range offloadTargets {
					// Update the source broker ID
					relocationParams.sourceID = sourceID

					relos := planRelocationsForBroker(relocationParams)

					// If no relocations could be planned, increment the exhaustion counter.
					if relos == 0 {
						exhaustedCount++
					}
				}
			}

			// Update the partition map with the relocation plan.
			applyRelocationPlan(partitionMap, relocationParams.plan)

			// Insert the reassignmentBundle.
			results <- reassignmentBundle{
				storageRange: relocationParams.brokers.StorageRange(),
				stdDev:       relocationParams.brokers.StorageStdDev(),
				tolerance:    tol,
				partitionMap: partitionMap,
				relocations:  relocationParams.relos,
				brokers:      relocationParams.brokers,
			}

		}()

		// Break early if we're using a fixed tolerance value.
		if params.UseFixedTolerance() {
			break
		}
	}

	wg.Wait()
	close(results)

	return results
}

/**
getPartitionMapChunks Breaks a reassignment into a series of sequential, smaller reassignments.
For large reassignments that may take a while, or risky operations that may require downtime in between, a chunked reassignment can be used.
This will generate a series of partition maps that will converge on the desired state. To minimize data transfer,
partitions are only moved to replicas in the desired final state.

The original design was intended for downscaling operations, to remove partitions from one (or three) brokers at a time,
without overwhelming whatever brokers are remaining in the cluster.
*/
func getPartitionMapChunks(finalMap *mapper.PartitionMap, initialMap *mapper.PartitionMap, brokerIds mapper.BrokerList, chunkStepSize int) []*mapper.PartitionMap {
	var intermediateMap = initialMap.Copy()
	var out []*mapper.PartitionMap
	brokerIds.SortByIDDesc()

	// Skipping the Stub broker which has an ID of int max and will be the first broker.
	for i := 1; i < len(brokerIds); i += chunkStepSize {
		// Select the brokers we will move data from for this chunk
		var chunkBrokers = map[int]struct{}{}
		for j := 0; j < chunkStepSize && i+j < len(brokerIds); j++ {
			chunkBrokers[brokerIds[i+j].ID] = struct{}{}
		}

		// Go through the current map, and any partitions with replicas in our chunked brokers for this iteration
		// will be moved this time.
		var tempMap = intermediateMap.Copy()
		for pIndex, p := range intermediateMap.Partitions {
			for rIndex, r := range p.Replicas {
				if _, correctReplica := chunkBrokers[r]; correctReplica {
					// This replica needs to be switched with one from the final map
					if len(tempMap.Partitions[pIndex].Replicas) != len(finalMap.Partitions[pIndex].Replicas) {
						fmt.Println("Chunked reassignment cannot be used when reducing or increasing replication factor. Exiting.")
						os.Exit(1)
					}
					tempMap.Partitions[pIndex].Replicas[rIndex] = finalMap.Partitions[pIndex].Replicas[rIndex]
				}
			}
		}

		var brokers []string
		for key := range chunkBrokers {
			brokers = append(brokers, strconv.Itoa(key))
		}
		// Don't return noop maps
		if equal, _ := tempMap.Equal(intermediateMap); !equal {
			out = append(out, tempMap)
			fmt.Printf("\n\nChanges for partition map chunk %d for brokers %s", len(out), strings.Join(brokers, ","))
			printMapChanges(intermediateMap, tempMap)
		} else {
			fmt.Printf("\n\nSkipping noop map output for brokers %s", strings.Join(brokers, ","))
		}
		intermediateMap = tempMap
	}

	return out
}

func validateBrokers(
	newBrokers []int,
	currentBrokers mapper.BrokerMap,
	bm mapper.BrokerMetaMap,
	newBrokersRequired bool,
) []error {
	// No broker changes are permitted in rebalance other than new broker additions.
	fmt.Println("\nValidating broker list:")

	// Update the current BrokerList with
	// the provided broker list.
	c, msgs := currentBrokers.Update(newBrokers, bm)
	for m := range msgs {
		fmt.Printf("%s%s\n", indent, m)
	}

	if c.Changes() {
		fmt.Printf("%s-\n", indent)
	}

	// Check if any referenced brokers are marked as having missing/partial metrics data.
	if errs := ensureBrokerMetrics(currentBrokers, bm); len(errs) > 0 {
		return errs
	}

	switch {
	case c.Missing > 0, c.OldMissing > 0, c.Replace > 0:
		return []error{fmt.Errorf("reassignment only allows broker additions")}
	case c.New > 0:
		fmt.Printf("%s%d additional brokers added\n", indent, c.New)
		fmt.Printf("%s-\n", indent)
		fmt.Printf("%sOK\n", indent)
	case newBrokersRequired:
		return []error{fmt.Errorf("reassignment requires additional brokers\n")}

	}
	return nil
}

func determineOffloadTargets(params reassignParams, brokers mapper.BrokerMap) []int {
	var offloadTargets []int

	var selectorMethod bytes.Buffer
	selectorMethod.WriteString("Brokers targeted for partition offloading ")

	// Switch on the target selection method. If a storage threshold in gigabytes
	// is specified, prefer this. Otherwise, use the percentage below mean threshold.
	var f mapper.BrokerFilterFn
	if params.storageThresholdGB > 0.00 {
		selectorMethod.WriteString(fmt.Sprintf("(< %.2fGB storage free)", params.storageThresholdGB))

		// Get all non-new brokers with a StorageFree below the storage threshold in GB.
		f = func(b *mapper.Broker) bool {
			if !b.New && b.StorageFree < params.storageThresholdGB*div {
				return true
			}
			return false
		}

	} else if params.storageThreshold > 0.00 {
		// Find brokers where the storage free is t % below the harmonic mean.
		selectorMethod.WriteString(fmt.Sprintf("(>= %.2f%% threshold below hmean)", params.storageThreshold*100))
		f = mapper.BelowMeanFn(params.storageThreshold, brokers.HMean)
	} else {
		// Specifying 0 targets all non-new brokers, this is a scale up
		f = func(b *mapper.Broker) bool { return !b.New }
	}

	matches := brokers.Filter(f)
	for _, b := range matches {
		offloadTargets = append(offloadTargets, b.ID)
	}

	fmt.Printf("\n%s:\n", selectorMethod.String())

	return offloadTargets
}
