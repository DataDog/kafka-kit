package kafkazk

import (
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

type partitionList []Partition // XXX pointers.

// PartitionMap maps the
// Kafka topic mapping syntax.
type PartitionMap struct {
	Version    int           `json:"version"`
	Partitions partitionList `json:"partitions"`
}

// NewPartitionMap returns an empty *PartitionMap.
func NewPartitionMap() *PartitionMap {
	return &PartitionMap{Version: 1}
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

// PartitionMap sorty by partition size.

type partitionsBySize struct {
	pl partitionList
	pm PartitionMetaMap
}

func (p partitionsBySize) Len() int      { return len(p.pl) }
func (p partitionsBySize) Swap(i, j int) { p.pl[i], p.pl[j] = p.pl[j], p.pl[i] }
func (p partitionsBySize) Less(i, j int) bool {
	s1, _ := p.pm.Size(p.pl[i])
	s2, _ := p.pm.Size(p.pl[j])

	if s1 > s2 {
		return true
	}
	if s1 < s2 {
		return false
	}

	return p.pl[i].Partition < p.pl[j].Partition
}

// PartitionMeta holds partition metadata.
type PartitionMeta struct {
	Size float64 // In bytes.
}

// PartitionMetaMap is a mapping of topic,
// partition number to PartitionMeta.
type PartitionMetaMap map[string]map[int]*PartitionMeta

// NewPartitionMetaMap returns an empty PartitionMetaMap.
func NewPartitionMetaMap() PartitionMetaMap {
	return map[string]map[int]*PartitionMeta{}
}

// Size takes a Partition and returns the
// size. An error is returned if the partition
// isn't in the PartitionMetaMap.
func (pmm PartitionMetaMap) Size(p Partition) (float64, error) {
	// Check for the topic.
	t, exists := pmm[p.Topic]
	if !exists {
		errS := fmt.Sprintf("Topic %s not found in partition metadata", p.Topic)
		return 0.00, errors.New(errS)
	}

	// Check for the partition.
	partn, exists := t[p.Partition]
	if !exists {
		errS := fmt.Sprintf("Partition %d not found in partition metadata", p.Partition)
		return 0.00, errors.New(errS)
	}

	return partn.Size, nil
}

// Rebuild takes a BrokerMap and rebuild strategy.
// It then traverses the partition map, replacing brokers marked removal
// with the best available candidate based on the selected
// rebuild strategy. A rebuilt *PartitionMap and []string of
// errors is returned.
func (pm *PartitionMap) Rebuild(bm BrokerMap, pmm PartitionMetaMap, strategy string) (*PartitionMap, []string) {
	switch strategy {
	// Standard sort.
	case "count":
		sort.Sort(pm.Partitions)
	// Sort by size.
	case "storage":
		s := partitionsBySize{
			pl: pm.Partitions,
			pm: pmm,
		}
		sort.Sort(partitionsBySize(s))
	default:
		return nil, []string{
			fmt.Sprintf("Invalid rebuild strategy '%s'", strategy),
		}
	}
	newMap := NewPartitionMap()

	// We need a filtered list for
	// usage sorting and exclusion
	// of nodes marked for removal.
	bl := bm.filteredList()

	var errs []string
	var pass int

	// Check if we need more passes.
	// If we've just counted as many skips
	// as there are partitions to handle,
	// we have nothing left to do.
	for skipped := 0; skipped < len(pm.Partitions); {
		for n, partn := range pm.Partitions {
			// If this is the first pass, create
			// the new partition.
			if pass == 0 {
				newP := Partition{Partition: partn.Partition, Topic: partn.Topic}
				newMap.Partitions = append(newMap.Partitions, newP)
			}

			// The number of needed passes may vary;
			// e.g. if most replica sets have a len
			// of 2 and a few with a len of 3, we have
			// to do 3 passes while skipping some
			// on final passes.
			if pass > len(partn.Replicas)-1 {
				skipped++
				continue
			}

			// Get the current Broker ID
			// for the current pass.
			bid := partn.Replicas[pass]

			// If the current broker isn't
			// marked for removal, just add it
			// to the same position in the new map.
			if !bm[bid].Replace {
				newMap.Partitions[n].Replicas = append(newMap.Partitions[n].Replicas, bid)
			} else {
				// Otherwise, we need to find a replacement.

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

				// Populate a constraints.
				constraints := mergeConstraints(replicaSet)

				// Add any necessary meta from current partition
				// to the constraints.
				if strategy == "storage" {
					s, err := pmm.Size(partn)
					if err != nil {
						errString := fmt.Sprintf("%s p%d: %s", partn.Topic, partn.Partition, err.Error())
						errs = append(errs, errString)
						continue
					}

					constraints.requestSize = s
				}

				// Fetch the best candidate and append.
				newBroker, err := bl.bestCandidate(constraints, strategy, int64(pass*n+1))

				if err != nil {
					// Append any caught errors.
					errString := fmt.Sprintf("%s p%d: %s", partn.Topic, partn.Partition, err.Error())
					errs = append(errs, errString)
					continue
				}

				newMap.Partitions[n].Replicas = append(newMap.Partitions[n].Replicas, newBroker.ID)
			}
		}

		// Increment the pass.
		pass++

	}

	// Final check to ensure that no
	// replica sets were somehow set to 0.
	for _, partn := range newMap.Partitions {
		if len(partn.Replicas) == 0 {
			errString := fmt.Sprintf("%s p%d: configured to zero replicas", partn.Topic, partn.Partition)
			errs = append(errs, errString)
		}
	}

	sort.Sort(newMap.Partitions)

	return newMap, errs
}

// PartitionMapFromString takes a json encoded string
// and returns a *PartitionMap.
func PartitionMapFromString(s string) (*PartitionMap, error) {
	pm := NewPartitionMap()

	err := json.Unmarshal([]byte(s), &pm)
	if err != nil {
		errString := fmt.Sprintf("Error parsing topic map: %s", err.Error())
		return nil, errors.New(errString)
	}

	sort.Sort(pm.Partitions)

	return pm, nil
}

// PartitionMapFromZK takes a slice of regexp
// and finds all matching topics for each. A
// merged *PartitionMap of all matching topic
// maps is returned.
func PartitionMapFromZK(t []*regexp.Regexp, zk Handler) (*PartitionMap, error) {
	// Get a list of topic names from Handler
	// matching the provided list.
	topicsToRebuild, err := zk.GetTopics(t)
	if err != nil {
		return nil, err
	}

	// Err if no matching topics were found.
	if len(topicsToRebuild) == 0 {
		errS := fmt.Sprintf("No topics found matching: %s", t)
		return nil, errors.New(errS)
	}

	// Get a partition map for each topic.
	pmapMerged := NewPartitionMap()
	for _, t := range topicsToRebuild {
		pmap, err := zk.GetPartitionMap(t)
		if err != nil {
			return nil, err
		}

		// Merge multiple maps.
		pmapMerged.Partitions = append(pmapMerged.Partitions, pmap.Partitions...)
	}

	sort.Sort(pmapMerged.Partitions)

	return pmapMerged, nil
}

// SetReplication ensures that replica sets
// is reset to the replication factor r. Sets
// exceeding r are truncated, sets below r
// are extended with stub brokers.
func (pm *PartitionMap) SetReplication(r int) {
	// 0 is a no-op.
	if r == 0 {
		return
	}

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

// Copy returns a copy of a *PartitionMap.
func (pm *PartitionMap) Copy() *PartitionMap {
	cpy := NewPartitionMap()

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
func (pm *PartitionMap) equal(pm2 *PartitionMap) (bool, error) {
	// Crude checks.
	switch {
	case len(pm.Partitions) != len(pm2.Partitions):
		return false, errors.New("partitions len")
	case pm.Version != pm2.Version:
		return false, errors.New("version")
	}

	// Iterative comparison.
	for i, p1 := range pm.Partitions {
		p2 := pm2.Partitions[i]
		switch {
		case p1.Topic != p2.Topic:
			return false, errors.New("topic order")
		case p1.Partition != p2.Partition:
			return false, errors.New("partition order")
		case len(p1.Replicas) != len(p2.Replicas):
			return false, errors.New("replica list")
		}
		// This is fine...
		for n := range p1.Replicas {
			if p1.Replicas[n] != p2.Replicas[n] {
				return false, errors.New("replica")
			}
		}
	}

	return true, nil
}

// Strip takes a PartitionMap and returns a
// copy where all broker ID references are replaced
// with the stub broker with ID 0 where the replace
// field is set to true. This ensures that the
// entire map is rebuilt, even if the provided broker
// list matches what's already in the map.
func (pm *PartitionMap) Strip() *PartitionMap {
	Stripped := NewPartitionMap()

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

		Stripped.Partitions = append(Stripped.Partitions, part)
	}

	return Stripped
}

// WriteMap takes a *PartitionMap and writes a JSON
// text file to the provided path.
func WriteMap(pm *PartitionMap, path string) error {
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

// UseStats returns a map of broker IDs
// to BrokerUseStats; each contains a count
// of leader and follower partition assignments.
func (pm *PartitionMap) UseStats() map[int]*BrokerUseStats {
	stats := map[int]*BrokerUseStats{}
	// Get counts.
	for _, p := range pm.Partitions {
		for i, b := range p.Replicas {
			if _, exists := stats[b]; !exists {
				stats[b] = &BrokerUseStats{}
			}
			// Idx 0 for each replica set
			// is a leader assignment.
			if i == 0 {
				stats[b].Leader++
			} else {
				stats[b].Follower++
			}
		}
	}

	return stats
}
