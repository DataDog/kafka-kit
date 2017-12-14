package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
)

// Partition maps the partition objects
// in the Kafka topic mapping syntax.
type Partition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Replicas  []int  `json:"replicas"`
}

type partitionList []Partition

// partitionMap maps the
// Kafka topic mapping syntax.
type partitionMap struct {
	Version    int           `json:"version"`
	Partitions partitionList `json:"partitions"`
}

// Satisfy the sort interface for partitionList.

func (p partitionList) Len() int      { return len(p) }
func (p partitionList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p partitionList) Less(i, j int) bool {
	if p[i].Topic < p[j].Topic {
		return true
	}
	if p[i].Topic > p[j].Topic {
		return false
	}

	return p[i].Partition < p[j].Partition
}

func newPartitionMap() *partitionMap {
	return &partitionMap{Version: 1}
}

// Rebuild takes a brokerMap and traverses
// the partition map, replacing brokers marked removal
// with the best available candidate.
func (pm *partitionMap) rebuild(bm brokerMap) (*partitionMap, []string) {
	sort.Sort(pm.Partitions)

	newMap := newPartitionMap()
	// We need a filtered list for
	// usage sorting and exclusion
	// of nodes marked for removal.
	bl := bm.filteredList()

	var errs []string

	pass := 0
	// For each partition partn in the
	// partitions list:
pass:
	skipped := 0
	for n, partn := range pm.Partitions {
		// If this is the first pass, create
		// the new partition.
		if pass == 0 {
			newP := Partition{Partition: partn.Partition, Topic: partn.Topic}
			newMap.Partitions = append(newMap.Partitions, newP)
		}

		// Build a brokerList from the
		// IDs in the old replica set to
		// get a *constraints.
		replicaSet := brokerList{}
		for _, bid := range partn.Replicas {
			replicaSet = append(replicaSet, bm[bid])
		}
		// Add existing brokers in the
		// new replica set as well.
		for _, bid := range newMap.Partitions[n].Replicas {
			replicaSet = append(replicaSet, bm[bid])
		}

		constraints := mergeConstraints(replicaSet)

		// The number of needed passes may vary;
		// e.g. if most replica sets have a len
		// of 2 and a few with a len of 3, we have
		// to do 3 passes while skipping some
		// on final passes.
		if pass > len(partn.Replicas)-1 {
			skipped++
			continue
		}

		// Get the broker ID we're
		// either going to move into
		// the new map or replace.
		bid := partn.Replicas[pass]

		// If the broker ID is marked as replace
		// in the broker map, get a new ID.
		if bm[bid].replace {
			// Fetch the best candidate and append.
			newBroker, err := bl.bestCandidate(constraints)
			if err != nil {
				// Append any caught errors.
				errString := fmt.Sprintf("%s p%d: %s", partn.Topic, partn.Partition, err.Error())
				errs = append(errs, errString)
				continue
			}

			newMap.Partitions[n].Replicas = append(newMap.Partitions[n].Replicas, newBroker.id)
		} else {
			// Otherwise keep the broker where it is.
			newMap.Partitions[n].Replicas = append(newMap.Partitions[n].Replicas, bid)
		}

	}

	pass++
	// Check if we need more passes.
	// If we've just counted as many skips
	// as there are partitions to handle,
	// we have nothing left to do.
	if skipped < len(pm.Partitions) {
		goto pass
	}

	return newMap, errs
}

// partitionMapFromString takes a json encoded string
// and returns a *partitionMap.
func partitionMapFromString(s string) (*partitionMap, error) {
	pm := newPartitionMap()

	err := json.Unmarshal([]byte(s), &pm)
	if err != nil {
		errString := fmt.Sprintf("Error parsing topic map: %s", err.Error())
		return nil, errors.New(errString)
	}

	return pm, nil
}

// partitionMapFromZK takes a slice of regexp
// and finds all matching topics for each. A
// merged *partitionMap of all matching topic
// maps is returned.
func partitionMapFromZK(t []*regexp.Regexp) (*partitionMap, error) {
	// Get a list of topic names from ZK
	// matching the provided list.
	topicsToRebuild, err := getTopics(zkc, t)
	if err != nil {
		return nil, err
	}

	// Err if no matching topics were found.
	if len(topicsToRebuild) == 0 {
		var b bytes.Buffer
		b.WriteString("No topics found matching: ")
		for n, t := range Config.rebuildTopics {
			b.WriteString(fmt.Sprintf("/%s/", t))
			if n < len(Config.rebuildTopics)-1 {
				b.WriteString(", ")
			}
		}

		return nil, errors.New(b.String())
	}

	// Get a partition map for each topic.
	pmapMerged := newPartitionMap()
	for _, t := range topicsToRebuild {
		pmap, err := getPartitionMap(zkc, t)
		if err != nil {
			return nil, err
		}

		// Merge multiple maps.
		pmapMerged.Partitions = append(pmapMerged.Partitions, pmap.Partitions...)
	}

	return pmapMerged, nil
}

// setReplication ensures that replica sets
// is reset to the replication factor r. Sets
// exceeding r are truncated, sets below r
// are extended with stub brokers.
func (pm *partitionMap) setReplication(r int) {
	for n, p := range pm.Partitions {
		l := len(p.Replicas)

		switch {
		// Truncate replicas beyond r.
		case l > r:
			pm.Partitions[n].Replicas = p.Replicas[:r]
		// Add stub brokers to meet r.
		case l < r:
			r := make([]int, r-l)
			pm.Partitions[n].Replicas = append(p.Replicas, r...)
		}
	}
}

// copy returns a copy of a *partitionMap.
func (pm *partitionMap) copy() *partitionMap {
	cpy := newPartitionMap()

	for _, p := range pm.Partitions {
		part := Partition{
			Topic:     p.Topic,
			Partition: p.Partition,
			Replicas:  make([]int, len(p.Replicas)),
		}

		copy(part.Replicas, p.Replicas)
		cpy.Partitions = append(cpy.Partitions, part)
	}

	return cpy
}

// Equal checks the equality betwee two partition maps.
// Equality requires that the total order is exactly
// the same.
func (pm *partitionMap) equal(pm2 *partitionMap) bool {
	// Crude checks.
	switch {
	case len(pm.Partitions) != len(pm2.Partitions):
		return false
	case pm.Version != pm2.Version:
		return false
	}

	// Iterative comparison.
	for i, p1 := range pm.Partitions {
		p2 := pm2.Partitions[i]
		switch {
		case p1.Topic != p2.Topic:
			return false
		case p1.Partition != p2.Partition:
			return false
		case len(p1.Replicas) != len(p2.Replicas):
			return false
		}
		// This is fine...
		for n := range p1.Replicas {
			if p1.Replicas[n] != p2.Replicas[n] {
				return false
			}
		}
	}

	return true
}

// strip takes a partitionMap and returns a
// copy where all broker ID references are replaced
// with the stub broker with ID 0 where the replace
// field is set to true. This ensures that the
// entire map is rebuilt, even if the provided broker
// list matches what's already in the map.
func (pm *partitionMap) strip() *partitionMap {
	stripped := newPartitionMap()

	// Copy each partition sans the replicas list.
	// The make([]int, ...) defaults the replica set to
	// ID 0, which is a default stub broker with replace
	// set to true.
	for _, p := range pm.Partitions {
		part := Partition{
			Topic:     p.Topic,
			Partition: p.Partition,
			Replicas:  make([]int, len(p.Replicas)),
		}

		stripped.Partitions = append(stripped.Partitions, part)
	}

	return stripped
}

// writeMap takes a *partitionMap and writes a JSON
// text file to the provided path.
func writeMap(pm *partitionMap, path string) error {
	// Marshal.
	out, err := json.Marshal(pm)
	if err != nil {
		return err
	}

	mapOut := string(out)

	// Write file.
	err = ioutil.WriteFile(path+".json", []byte(mapOut+"\n"), 0644)
	if err != nil {
		return err
	}

	return nil
}

// useStats returns a map of broker IDs
// to brokerUseStats; each contains a count
// of leader and follower partition assignments.
func (pm *partitionMap) useStats() map[int]*brokerUseStats {
	stats := map[int]*brokerUseStats{}
	// Get counts.
	for _, p := range pm.Partitions {
		for i, b := range p.Replicas {
			if _, exists := stats[b]; !exists {
				stats[b] = &brokerUseStats{}
			}
			// Idx 0 for each replica set
			// is a leader assignment.
			if i == 0 {
				stats[b].leader++
			} else {
				stats[b].follower++
			}
		}
	}

	return stats
}
