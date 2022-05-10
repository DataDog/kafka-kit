package commands

import (
	"fmt"
	"regexp"
	"time"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
	"github.com/DataDog/kafka-kit/v3/mapper"
)

// checkMetaAge checks the age of the stored partition and broker storage
// metrics data against the tolerated metrics age parameter.
func checkMetaAge(zk kafkazk.Handler, maxAge int) error {
	age, err := zk.MaxMetaAge()
	if err != nil {
		return fmt.Errorf("Error fetching metrics metadata: %s\n", err)
	}

	if age > time.Duration(maxAge)*time.Minute {
		return fmt.Errorf("Metrics metadata is older than allowed: %s\n", age)
	}
	return nil
}

// getBrokerMeta returns a map of brokers and broker metadata for those
// registered in ZooKeeper. Optionally, metrics metadata persisted in ZooKeeper
// (via an external mechanism*) can be merged into the metadata.
func getBrokerMeta(zk kafkazk.Handler, m bool) (mapper.BrokerMetaMap, []error) {
	return zk.GetAllBrokerMeta(m)
}

// ensureBrokerMetrics takes a map of reference brokers and a map of discovered
// broker metadata. Any non-missing brokers in the broker map must be present
// in the broker metadata map and have a non-true MetricsIncomplete value.
func ensureBrokerMetrics(bm mapper.BrokerMap, bmm mapper.BrokerMetaMap) []error {
	errs := []error{}
	for id, b := range bm {
		// Missing brokers won't be found in the brokerMeta.
		if !b.Missing && id != mapper.StubBrokerID && bmm[id].MetricsIncomplete {
			errs = append(errs, fmt.Errorf("Metrics not found for broker %d\n", id))
		}
	}
	return errs
}

// getPartitionMeta returns a map of topic, partition metadata persisted in
// ZooKeeper (via an external mechanism*). This is primarily partition size
// metrics data used for the storage placement strategy.
func getPartitionMeta(zk kafkazk.Handler) (mapper.PartitionMetaMap, error) {
	return zk.GetAllPartitionMeta()
}

// stripPendingDeletes takes a partition map and zk handler. It looks up any
// topics in a pending delete state and removes them from the provided partition
// map, returning a list of topics removed.
func stripPendingDeletes(pm *mapper.PartitionMap, zk kafkazk.Handler) ([]string, error) {
	// Get pending deletions.
	pd, err := zk.GetPendingDeletion()

	if len(pd) == 0 {
		return []string{}, err
	}

	// Convert to a series of literal regex.
	var re []*regexp.Regexp
	for _, topic := range pd {
		r := regexp.MustCompile(fmt.Sprintf(`^%s$`, topic))
		re = append(re, r)
	}

	// Update the PartitionMap and return a list of removed topic names.
	return removeTopics(pm, re), err
}

// removeTopics takes a PartitionMap and []*regexp.Regexp of topic name patters.
// Any topic names that match any provided pattern will be removed from the
// PartitionMap and a []string of topics that were found and removed is returned.
func removeTopics(pm *mapper.PartitionMap, r []*regexp.Regexp) []string {
	var removedNames []string

	if len(r) == 0 {
		return removedNames
	}

	// Create a new PartitionList, populate non-removed topics, substitute the
	// existing PartitionList in the PartitionMap.
	newPL := mapper.PartitionList{}

	// Track what's removed.
	removed := map[string]struct{}{}

	// Traverse the partition map.
	for _, p := range pm.Partitions {
		for i, re := range r {
			// If the topic matches any regex pattern, add it to the removed set.
			if re.MatchString(p.Topic) {
				removed[p.Topic] = struct{}{}
				break
			}

			// We've checked all patterns.
			if i == len(r)-1 {
				// Else, it wasn't marked for removal; add it to the new PartitionList.
				newPL = append(newPL, p)
			}
		}
	}

	pm.Partitions = newPL

	for t := range removed {
		removedNames = append(removedNames, t)
	}

	return removedNames
}
