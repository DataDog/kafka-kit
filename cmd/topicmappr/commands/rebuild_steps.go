package commands

import (
	"fmt"
	"os"

	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
	"github.com/DataDog/kafka-kit/v4/kafkazk"
	"github.com/DataDog/kafka-kit/v4/mapper"
)

func runRebuild(params rebuildParams, ka kafkaadmin.KafkaAdmin, zk kafkazk.Handler) ([]*mapper.PartitionMap, []error) {
	// General flow:
	// 1) A PartitionMap is formed (either unmarshaled from the literal
	//   map input via --rebuild-map or generated from ZooKeeper Metadata
	//   for topics matching --topics).
	// 2) A BrokerMap is formed from brokers found in the PartitionMap
	//   along with any new brokers provided via the --brokers param.
	// 3) The PartitionMap and BrokerMap are fed to a rebuild
	//   function. Missing brokers, brokers marked for replacement,
	//   and all other placements are performed, returning a new
	//   PartitionMap.
	// 4) Differences between the original and new PartitionMap
	//   are detected and reported.
	// 5) The new PartitionMap is split by topic. Map(s) are written.

	// In addition to the global topic regex, we have leader-evac topic regex as well.
	var evacTopics []string
	var err error
	if len(params.leaderEvacTopics) != 0 {
		evacTopics, err = zk.GetTopics(params.leaderEvacTopics)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	// Fetch broker metadata.
	var withMetrics bool
	if params.placement == "storage" {
		if err := checkMetaAge(zk, params.maxMetadataAge); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		withMetrics = true
	}

	var brokerMeta mapper.BrokerMetaMap
	var errs []error
	if params.useMetadata {
		if brokerMeta, errs = getBrokerMeta(zk, withMetrics); errs != nil && brokerMeta == nil {
			for _, e := range errs {
				fmt.Println(e)
			}
			os.Exit(1)
		}
	}

	// Fetch partition metadata.
	var partitionMeta mapper.PartitionMetaMap
	if params.placement == "storage" {
		if partitionMeta, err = getPartitionMeta(zk); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	// Build a partition map either from literal map text input or by fetching the
	// map data from ZooKeeper. Store a copy of the original.
	partitionMapIn, pending, excluded := getPartitionMap(params, zk)
	originalMap := partitionMapIn.Copy()

	// Get a list of affected topics.
	printTopics(partitionMapIn)

	// Print if any topics were excluded due to pending deletion or explicit
	// exclusion.
	printExcludedTopics(pending, excluded)

	brokers, bs := getBrokers(params, partitionMapIn, brokerMeta)
	brokersOrig := brokers.Copy()

	if bs.Changes() {
		fmt.Printf("%s-\n", indent)
	}

	// Check if any referenced brokers are marked as having
	// missing/partial metrics data.
	if params.useMetadata {
		if errs := ensureBrokerMetrics(brokers, brokerMeta); len(errs) > 0 {
			for _, e := range errs {
				fmt.Println(e)
			}
			os.Exit(1)
		}
	}

	// Create substitution affinities.
	affinities := getSubAffinities(params, brokers, brokersOrig, partitionMapIn)

	if affinities != nil {
		fmt.Printf("%s-\n", indent)
	}

	// Print changes, actions.
	printChangesActions(params, bs)

	// Apply any replication factor settings.
	updateReplicationFactor(params, partitionMapIn)

	// Build a new map using the provided list of brokers. This is OK to run even
	// when a no-op is intended.
	partitionMapOut, errs := buildMap(params, partitionMapIn, partitionMeta, brokers, affinities)

	// Optimize leaders.
	if params.optimizeLeadership {
		partitionMapOut.OptimizeLeaderFollower()
	}

	// Count missing brokers as a warning.
	if bs.Missing > 0 {
		errs = append(errs, fmt.Errorf("%d provided brokers not found in ZooKeeper", bs.Missing))
	}

	// Count missing rack info as warning
	if bs.RackMissing > 0 {
		errs = append(
			errs, fmt.Errorf("%d provided broker(s) do(es) not have a rack.id defined", bs.RackMissing),
		)
	}

	var outputMaps []*mapper.PartitionMap
	// Generate phased map if enabled.
	if params.phasedReassignment {
		outputMaps = append(outputMaps, phasedReassignment(originalMap, partitionMapOut))
	}

	partitionMapOut = evacuateLeadership(*partitionMapOut, params.leaderEvacBrokers, evacTopics)

	// Print map change results.
	printMapChanges(originalMap, partitionMapOut)

	// Print broker assignment statistics.
	errs = append(
		errs,
		printBrokerAssignmentStats(originalMap, partitionMapOut, brokersOrig, brokers, params.placement == "storage", params.partitionSizeFactor)...,
	)

	// Skip no-ops if configured.
	if params.skipNoOps {
		originalMap, partitionMapOut = skipReassignmentNoOps(originalMap, partitionMapOut)
	}

	// If this is a getPartitionMapChunks operation, break it up into smaller operations and list those as intermediate maps.
	if params.chunkStepSize > 0 {
		fmt.Printf("\n\nGenerating reassignments in chunks %d brokers at a time: \n\n", params.chunkStepSize)
		mapChunks := getPartitionMapChunks(partitionMapOut, originalMap, brokersOrig.List(), params.chunkStepSize)
		for _, chunk := range mapChunks {
			outputMaps = append(outputMaps, chunk)
		}
	} else {
		outputMaps = append(outputMaps, partitionMapOut)
	}

	return outputMaps, errs
}

// *References to metrics metadata persisted in ZooKeeper, see:
// https://github.com/DataDog/kafka-kit/tree/master/cmd/metricsfetcher#data-structures)

// getPartitionMap returns a map of of partition, topic config (particuarly what
// brokers compose every replica set) for all topics specified. A partition map
// is either built from a string literal input (json from off-the-shelf Kafka
// tools output) provided via the ---map-string flag, or, by building a map based
// on topic config found in ZooKeeper for all topics matching input provided
// via the --topics flag. Two []string are returned; topics excluded due to
// pending deletion and topics explicitly excluded (via the --topics-exclude
// flag), respectively.
func getPartitionMap(params rebuildParams, zk kafkazk.Handler) (*mapper.PartitionMap, []string, []string) {

	switch {
	// The map was provided as text.
	case params.mapString != "":
		// Get a deserialized map.
		pm, err := mapper.PartitionMapFromString(params.mapString)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		// Exclude topics explicitly listed.
		et := removeTopics(pm, params.topicsExclude)
		return pm, []string{}, et
	// The map needs to be fetched via ZooKeeper metadata for all specified topics.
	case len(params.topics) > 0:
		pm, err := kafkazk.PartitionMapFromZK(params.topics, zk)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Exclude any topics that are pending deletion.
		pd, err := stripPendingDeletes(pm, zk)
		if err != nil {
			fmt.Println("Error fetching topics pending deletion")
		}

		// Exclude topics explicitly listed.
		et := removeTopics(pm, params.topicsExclude)

		return pm, pd, et
	}

	return nil, nil, nil
}

// getSubAffinities, if enabled via --sub-affinity, takes reference broker maps
// and a partition map and attempts to return a complete SubstitutionAffinities.
func getSubAffinities(params rebuildParams, bm mapper.BrokerMap, bmo mapper.BrokerMap, pm *mapper.PartitionMap) mapper.SubstitutionAffinities {
	var affinities mapper.SubstitutionAffinities

	if params.subAffinity && !params.forceRebuild {
		var err error
		affinities, err = bm.SubstitutionAffinities(pm)
		if err != nil {
			fmt.Printf("Substitution affinity error: %s\n", err.Error())
			os.Exit(1)
		}
	}

	// Print whether any affinities were inferred.
	for a, b := range affinities {
		var inferred string
		if bmo[a].Missing {
			inferred = "(inferred)"
		}
		fmt.Printf("%sSubstitution affinity: %d -> %d %s\n", indent, a, b.ID, inferred)
	}

	return affinities
}

// getBrokers takes a PartitionMap and BrokerMetaMap and returns a BrokerMap
// along with a BrokerStatus. These two structures hold metadata describing
// broker state (rack IDs, whether they need to be replaced, newly provided, etc.)
// and general statistics.
// - The BrokerMap is later used in map rebuild time as the canonical source of
//   broker state. Brokers that need to be removed (either because they were not
//   registered in ZooKeeper or were removed from the --brokers list) are determined here.
// - The BrokerStatus is used for purely informational output, such as how many missing
//   brokers were discovered or newly provided (i.e. specified in the --brokers flag but
//   not previously holding any partitions for any partitions of the referenced topics
//   being rebuilt by topicmappr)
func getBrokers(params rebuildParams, pm *mapper.PartitionMap, bm mapper.BrokerMetaMap) (mapper.BrokerMap, *mapper.BrokerStatus) {
	fmt.Printf("\nBroker change summary:\n")

	// Get a broker map of the brokers in the current partition map.
	// If meta data isn't being looked up, brokerMeta will be empty.
	brokers := mapper.BrokerMapFromPartitionMap(pm, bm, params.forceRebuild)

	// Update the currentBrokers list with the provided broker list.
	bs, msgs := brokers.Update(params.brokers, bm)
	for m := range msgs {
		fmt.Printf("%s%s\n", indent, m)
	}

	return brokers, bs
}

// printChangesActions takes a BrokerStatus and prints out information output
// describing changes in broker counts and liveness.
func printChangesActions(params rebuildParams, bs *mapper.BrokerStatus) {
	change := bs.New - bs.Replace

	// Print broker change summary.
	fmt.Printf("%sReplacing %d, added %d, missing %d, total count changed by %d\n",
		indent, bs.Replace, bs.New, bs.Missing+bs.OldMissing, change)

	// Determine actions.
	actions := make(chan string, 5)

	if change >= 0 && bs.Replace > 0 {
		actions <- fmt.Sprintf("Rebuild topic with %d broker(s) marked for replacement", bs.Replace)
	}

	if change > 0 && bs.Replace == 0 {
		actions <- fmt.Sprintf("Expanding/rebalancing topic with %d additional broker(s) (this is a no-op unless --force-rebuild is specified)", bs.New)
	}

	if change < 0 {
		actions <- fmt.Sprintf("Shrinking topic by %d broker(s)", -change)
	}

	if params.forceRebuild {
		actions <- fmt.Sprintf("Force rebuilding map")
	}

	if params.replication > 0 {
		actions <- fmt.Sprintf("Setting replication factor to %d", params.replication)
	}

	if params.optimizeLeadership {
		actions <- fmt.Sprintf("Optimizing leader/follower ratios")
	}

	close(actions)

	// Print action.
	fmt.Printf("\nAction:\n")

	if len(actions) == 0 {
		fmt.Printf("%sno-op\n", indent)
		return
	}

	for a := range actions {
		fmt.Printf("%s%s\n", indent, a)
	}
}

// updateReplicationFactor takes a PartitionMap and normalizes the replica set
// length to an optionally provided value.
func updateReplicationFactor(params rebuildParams, pm *mapper.PartitionMap) {
	// If the replication factor is changed, the partition map input needs to have
	// stub brokers appended (r factor increase) or existing brokers removed
	// (r factor decrease).
	if params.replication > 0 {
		pm.SetReplication(params.replication)
	}
}

// buildMap takes an input PartitionMap, rebuild parameters, and all partition/broker
// metadata structures required to generate the output PartitionMap. A []string of
// warnings / advisories is returned if any are encountered.
func buildMap(params rebuildParams, pm *mapper.PartitionMap, pmm mapper.PartitionMetaMap, bm mapper.BrokerMap, af mapper.SubstitutionAffinities) (*mapper.PartitionMap, errors) {
	rebuildParams := mapper.RebuildParams{
		PMM:              pmm,
		BM:               bm,
		Strategy:         params.placement,
		Optimization:     params.optimize,
		PartnSzFactor:    params.partitionSizeFactor,
		MinUniqueRackIDs: params.minRackIds,
	}
	if af != nil {
		rebuildParams.Affinities = af
	}

	// If we're doing a force rebuild, the input map must have all brokers stripped out.
	// A few notes about doing force rebuilds:
	// - Map rebuilds should always be called on a stripped PartitionMap copy.
	// - The BrokerMap provided in the Rebuild call should have
	//   been built from the original PartitionMap, not the stripped map.
	// - A force rebuild assumes that all partitions will be lifted from
	//   all brokers and repositioned. This means you should call the SubStorageAll
	//   method on the BrokerMap if we're doing a "storage" placement strategy. The
	//   SubStorageAll takes a PartitionMap and PartitionMetaMap. The PartitionMap
	//   is used to find partition to broker relationships so that the storage used
	//   can be readded to the broker's StorageFree value. The amount to be readded,
	//   the size of the partition, is referenced from the PartitionMetaMap.

	if params.forceRebuild {
		// Get a stripped map that we'll call rebuild on.
		partitionMapInStripped := pm.Strip()
		// If the storage placement strategy is being used,
		// update the broker StorageFree values.
		if params.placement == "storage" {
			err := rebuildParams.BM.SubStorage(pm, pmm, mapper.AllBrokersFn)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}

		// Rebuild.
		return partitionMapInStripped.Rebuild(rebuildParams)
	}

	// Update the StorageFree only on brokers marked for replacement.
	if params.placement == "storage" {
		err := rebuildParams.BM.SubStorage(pm, pmm, mapper.ReplacedBrokersFn)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	// Rebuild directly on the input map.
	return pm.Rebuild(rebuildParams)
}

// phasedReassignment takes the input map (the current ISR states) and the
// output map (the results of the topicmappr input parameters / computation)
// and prepends the current leaders as the leaders of the output map.
func phasedReassignment(pm1, pm2 *mapper.PartitionMap) *mapper.PartitionMap {
	// Get topics from output partition map.
	topics := pm2.Topics()

	var phase1pm = pm2.Copy()

	// Get ReplicaSets from the input map for each topic.
	for _, topic := range topics {
		// Get the original (current) replica sets.
		rs := pm1.ReplicaSets(topic)
		// For each topic in the output partition map, prepend
		// the leader from the original replica set.
		for i, partn := range phase1pm.Partitions {
			if partn.Topic == topic {
				// There's scenarios we could be prepending the existing leader; i.e.
				// if this isn't a force-rebuild or completely new broker pool, it's
				// possible that the current partition didn't get mapped to a new
				// broker. If the before/after replica set is [1001] -> [1001], we'd
				// end up with [1001,1001] here. Check if the old leader is already
				// in the replica set.
				leader := rs[partn.Partition][0]
				if notInReplicaSet(leader, partn.Replicas) {
					phase1pm.Partitions[i].Replicas = append([]int{leader}, partn.Replicas...)
				}
			}
		}
	}

	return phase1pm
}

func notInReplicaSet(id int, rs []int) bool {
	for i := range rs {
		if rs[i] == id {
			return false
		}
	}

	return true
}

// evacuateLeadership For the given set of topics, makes sure that the given brokers are not
// leaders of any partitions. If we have any partitions that only have replicas from the
// evac broker list, we will fail.
func evacuateLeadership(partitionMapIn mapper.PartitionMap, evacBrokers []int, evacTopics []string) *mapper.PartitionMap {
	// evacuation algo starts here
	partitionMapOut := partitionMapIn.Copy()

	// make a lookup map of topics
	topicsMap := map[string]struct{}{}
	for _, topic := range evacTopics {
		topicsMap[topic] = struct{}{}
	}

	// make a lookup map of topics
	brokersMap := map[int]struct{}{}
	for _, b := range evacBrokers {
		brokersMap[b] = struct{}{}
	}

	// TODO What if we only want to evacuate a subset of partitions?
	// For now, problem brokers is the bigger use case.

	// swap leadership for all broker/partition/topic combos
	for i, p := range partitionMapIn.Partitions {
		// check the topic is one of the target topics
		if _, correctTopic := topicsMap[p.Topic]; !correctTopic {
			continue
		}

		// check the leader to see if its one of the evac brokers
		if _, contains := brokersMap[p.Replicas[0]]; !contains {
			continue
		}

		for j, replica := range p.Replicas {
			// If one of the replicas is not being leadership evacuated, use that one and swap.
			if _, contains := brokersMap[replica]; !contains {
				newLeader := p.Replicas[j]
				partitionMapOut.Partitions[i].Replicas[j] = p.Replicas[0]
				partitionMapOut.Partitions[i].Replicas[0] = newLeader
				break
			}

			// If we've tried every replica, but they are all being leader evac'd.
			if replica == p.Replicas[len(p.Replicas)-1] {
				fmt.Println("[ERROR] trying to evict all replicas at once")
				os.Exit(1)
			}
		}
	}

	return partitionMapOut
}
