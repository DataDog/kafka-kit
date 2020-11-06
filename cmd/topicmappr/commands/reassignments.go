package commands

import (
	"sync"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
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
	partitionMap *kafkazk.PartitionMap
	// Partition relocations that constitute the reassignment.
	relocations map[int][]relocation
	// The brokers that the PartitionMap is assigning brokers to.
	brokers kafkazk.BrokerMap
}

type computeReassignmentBundlesParams struct {
	offloadTargets         []int
	tolerance              float64
	partitionMap           *kafkazk.PartitionMap
	partitionMeta          kafkazk.PartitionMetaMap
	brokerMap              kafkazk.BrokerMap
	partitionLimit         int
	partitionSizeThreshold int
	localityScoped         bool
	verbose                bool
}

// computeReassignmentBundles takes computeReassignmentBundlesParams and returns
// a chan reassignmentBundle. The channel will either contain a single reassignmentBundle
// if a fixed computeReassignmentBundlesParams.tolerance value (non 0.00) is
// specified, otherwise it will contain a series of reassignmentBundle for multiple
// interval values. When generating a series, the results are computed in parallel.
func computeReassignmentBundles(params computeReassignmentBundlesParams) chan reassignmentBundle {
	otm := map[int]struct{}{}
	for _, id := range params.offloadTargets {
		otm[id] = struct{}{}
	}

	results := make(chan reassignmentBundle, 100)
	wg := &sync.WaitGroup{}

	// Compute a reassignmentBundle output for all tolerance values 0.01..0.99 in parallel.
	for i := 0.01; i < 0.99; i += 0.01 {
		// Whether we're using a fixed tolerance (non 0.00 value) set via flag or
		// interval values.
		var tol float64
		var fixedTolerance bool

		if params.tolerance == 0.00 {
			tol = i
		} else {
			tol = params.tolerance
			fixedTolerance = true
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			partitionMap := params.partitionMap.Copy()

			// Bundle planRelocationsForBrokerParams.
			relocationParams := planRelocationsForBrokerParams{
				relos:                  map[int][]relocation{},
				mappings:               partitionMap.Mappings(),
				brokers:                params.brokerMap.Copy(),
				partitionMeta:          params.partitionMeta,
				plan:                   relocationPlan{},
				topPartitionsLimit:     params.partitionLimit,
				partitionSizeThreshold: params.partitionSizeThreshold,
				offloadTargetsMap:      otm,
				tolerance:              tol,
			}

			// Iterate over offload targets, planning at most one relocation per iteration.
			// Continue this loop until no more relocations can be planned.
			for exhaustedCount := 0; exhaustedCount < len(params.offloadTargets); {
				relocationParams.pass++
				for _, sourceID := range params.offloadTargets {
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
		if fixedTolerance {
			break
		}
	}

	wg.Wait()
	close(results)

	return results
}
