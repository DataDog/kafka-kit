package kafkazk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"regexp"
	"sort"
)

// Partition represents the Kafka partition structure.
type Partition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Replicas  []int  `json:"replicas"`
}

// PartitionList is a []Partition.
type PartitionList []Partition

// PartitionMap represents the Kafka partition map structure.
type PartitionMap struct {
	Version    int           `json:"version"`
	Partitions PartitionList `json:"partitions"`
}

// NewPartitionMap returns an empty *PartitionMap.
func NewPartitionMap() *PartitionMap {
	return &PartitionMap{Version: 1}
}

// Satisfy the sort interface for PartitionList.

func (p PartitionList) Len() int      { return len(p) }
func (p PartitionList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p PartitionList) Less(i, j int) bool {
	if p[i].Topic < p[j].Topic {
		return true
	}
	if p[i].Topic > p[j].Topic {
		return false
	}

	return p[i].Partition < p[j].Partition
}

// PartitionMap sort by partition size.

type partitionsBySize struct {
	pl PartitionList
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

// SortBySize takes a PartitionMetaMap and sorts the PartitionList
// by partition size.
func (p PartitionList) SortBySize(m PartitionMetaMap) {
	sort.Sort(partitionsBySize{pl: p, pm: m})
}

// PartitionMeta holds partition metadata.
type PartitionMeta struct {
	Size float64 // In bytes.
}

// PartitionMetaMap is a mapping of topic, partition number to PartitionMeta.
type PartitionMetaMap map[string]map[int]*PartitionMeta

// NewPartitionMetaMap returns an empty PartitionMetaMap.
func NewPartitionMetaMap() PartitionMetaMap {
	return map[string]map[int]*PartitionMeta{}
}

// Size takes a Partition and returns the size. An error is returned if
// the partition isn't in the PartitionMetaMap.
func (pmm PartitionMetaMap) Size(p Partition) (float64, error) {
	// Check for the topic.
	t, exists := pmm[p.Topic]
	if !exists {
		return 0.00, fmt.Errorf("Topic %s not found in partition metadata", p.Topic)
	}

	// Check for the partition.
	partn, exists := t[p.Partition]
	if !exists {
		return 0.00, fmt.Errorf("Partition %d not found in partition metadata", p.Partition)
	}

	return partn.Size, nil
}

// RebuildParams holds required parameters to call the Rebuild
// method on a *PartitionMap.
type RebuildParams struct {
	pm            *PartitionMap
	PMM           PartitionMetaMap
	BM            BrokerMap
	Strategy      string
	Optimization  string
	Affinities    SubstitutionAffinities
	PartnSzFactor float64
}

// NewRebuildParams initializes a RebuildParams.
func NewRebuildParams() RebuildParams {
	return RebuildParams{
		PartnSzFactor: 1.00,
	}
}

// SimpleLeaderOptimization is a naive leadership optimization algorithm.
// It gets leadership counts for all brokers in the partition map and
// shuffles partition replica sets for those holding brokers with below
// average leadership.
func (pm *PartitionMap) SimpleLeaderOptimization() {
	stats := pm.UseStats()

	// Get avg.
	var t float64
	for _, b := range stats {
		t += float64(b.Leader)
	}

	// Brute force with multiple iterations.
	for i := 0; i < len(pm.Partitions); i++ {
		avg := t / float64(len(stats))
		belowAvg := map[int]struct{}{}

		// Get IDs below avg. leadership counts.
		for _, b := range stats {
			if float64(b.Leader) == 0 || (avg-float64(b.Leader))/avg > 0.40 {
				belowAvg[b.ID] = struct{}{}
			}
		}

		if len(belowAvg) == 0 {
			return
		}

		// Shuffle all replica sets containing
		// below average brokers.
		f := func(p Partition) bool {
			for _, id := range p.Replicas {
				if _, exists := belowAvg[id]; exists {
					delete(belowAvg, id)
					return true
				}
			}
			return false
		}

		pm.shuffle(f)
	}
}

// Rebuild takes a BrokerMap and rebuild strategy. It then traverses the
// partition map, replacing brokers marked removal with the best available
// candidate based on the selected rebuild strategy. A rebuilt *PartitionMap
// and []error of errors is returned.
func (pm *PartitionMap) Rebuild(params RebuildParams) (*PartitionMap, []error) {
	var newMap *PartitionMap
	var errs []error

	params.pm = pm

	switch params.Strategy {
	case "count":
		// Standard sort
		sort.Sort(params.pm.Partitions)
		// Perform placements.
		newMap, errs = placeByPosition(params)
	case "storage":
		// Sort by size.
		s := partitionsBySize{
			pl: params.pm.Partitions,
			pm: params.PMM,
		}
		sort.Sort(partitionsBySize(s))
		// Perform placements. The placement method
		// depends on the choosen optimization param.
		switch params.Optimization {
		case "distribution":
			newMap, errs = placeByPosition(params)
		case "storage":
			newMap, errs = placeByPartition(params)
			// Shuffle replica sets. placeByPartition suffers from suboptimal
			// leadership distribution because of the requirement to choose all
			// brokers for each partition at a time (in contrast to placeByPosition).
			// Shuffling has proven so far to distribute leadership even though
			// it's purely by probability. Eventually, write a real optimizer.
			newMap.shuffle(func(_ Partition) bool { return true })
		// Invalid optimization.
		default:
			return nil, []error{fmt.Errorf("Invalid optimization '%s'", params.Optimization)}
		}
	// Invalid placement.
	default:
		return nil, []error{fmt.Errorf("Invalid rebuild strategy '%s'", params.Strategy)}
	}

	// Final sort.
	sort.Sort(newMap.Partitions)

	return newMap, errs
}

// placeByPosition builds a PartitionMap by doing placements for all
// partitions, one broker index at a time. For instance, if all partitions
// required a broker set length of 3 (aka a replication factor of 3), we'd
// do all placements in 3 passes. The first pass would be leaders for all
// partitions, the second pass would be the first follower, and the third
// pass would be the second follower. This placement pattern is optimal
// for the count strategy.
func placeByPosition(params RebuildParams) (*PartitionMap, []error) {
	newMap := NewPartitionMap()

	// We need a filtered list for usage sorting and exclusion
	// of nodes marked for removal.
	f := func(b *Broker) bool {
		if b.Replace {
			return false
		}
		return true
	}

	bl := params.BM.Filter(f).List()

	var errs []error
	var pass int

	// Check if we need more passes.
	// If we've just counted as many skips
	// as there are partitions to handle,
	// we have nothing left to do.
	for skipped := 0; skipped < len(params.pm.Partitions); {
		for n, partn := range params.pm.Partitions {
			// If this is the first pass, create
			// the new partition.
			if pass == 0 {
				newPartn := Partition{Partition: partn.Partition, Topic: partn.Topic}
				newMap.Partitions = append(newMap.Partitions, newPartn)
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
			if !params.BM[bid].Replace {
				newMap.Partitions[n].Replicas = append(newMap.Partitions[n].Replicas, bid)
			} else {
				// Otherwise, we need to find a replacement.

				// Build a BrokerList from the
				// IDs in the old replica set to
				// get a *constraints.
				replicaSet := BrokerList{}
				for _, bid := range partn.Replicas {
					replicaSet = append(replicaSet, params.BM[bid])
				}
				// Add existing brokers in the
				// new replica set as well.
				for _, bid := range newMap.Partitions[n].Replicas {
					replicaSet = append(replicaSet, params.BM[bid])
				}

				// Populate a constraints.
				constraints := MergeConstraints(replicaSet)

				// Add any necessary meta from current partition
				// to the constraints.
				if params.Strategy == "storage" {
					s, err := params.PMM.Size(partn)
					if err != nil {
						e := fmt.Errorf("%s p%d: %s", partn.Topic, partn.Partition, err.Error())
						errs = append(errs, e)
						continue
					}

					constraints.requestSize = s * params.PartnSzFactor
				}

				// Fetch the best candidate and append.
				var replacement *Broker
				var err error

				// If we're using the count method, check if a
				// substitution affinity is set for this broker.
				affinity := params.Affinities.Get(bid)
				if params.Strategy == "count" && affinity != nil {
					replacement = affinity
					// Ensure the replacement passes constraints.
					// This is usually checked at the time of building
					// a substitution affinities map, but in scenarios
					// where the replacement broker was completely missing
					// from ZooKeeper, its rack ID is unknown and a suitable
					// sub has to be inferred. We're checking that it passes
					// here in case the inference logic is faulty.
					if passes := constraints.passes(replacement); !passes {
						err = ErrNoBrokers
					}
				} else {
					// Otherwise, use the standard
					// constraints based selector.
					replacement, err = bl.BestCandidate(constraints, params.Strategy, int64(pass*n+1))
				}

				if err != nil {
					// Append any caught errors.
					e := fmt.Errorf("%s p%d: %s", partn.Topic, partn.Partition, err.Error())
					errs = append(errs, e)
					continue
				}

				// Add the replacement to the map.
				newMap.Partitions[n].Replicas = append(newMap.Partitions[n].Replicas, replacement.ID)
			}
		}

		// Increment the pass.
		pass++

	}

	// Final check to ensure that no
	// replica sets were somehow set to 0.
	for _, partn := range newMap.Partitions {
		if len(partn.Replicas) == 0 {
			e := fmt.Errorf("%s p%d: configured to zero replicas", partn.Topic, partn.Partition)
			errs = append(errs, e)
		}
	}

	// Return map, errors.
	return newMap, errs
}

func placeByPartition(params RebuildParams) (*PartitionMap, []error) {
	newMap := NewPartitionMap()

	// We need a filtered list for usage sorting and exclusion
	// of nodes marked for removal.
	f := func(b *Broker) bool {
		if b.Replace {
			return false
		}
		return true
	}

	bl := params.BM.Filter(f).List()

	var errs []error

	for _, partn := range params.pm.Partitions {
		// Create the partition in
		// the new map.
		newPartn := Partition{Partition: partn.Partition, Topic: partn.Topic}

		// Map over each broker from the original
		// partition replica list to the new,
		// selecting replacemnt for those marked
		// for replacement.
		for _, bid := range partn.Replicas {
			// If the current broker isn't
			// marked for removal, just add it
			// to the same position in the new map.
			if !params.BM[bid].Replace {
				newPartn.Replicas = append(newPartn.Replicas, bid)
			} else {
				// Otherwise, we need to find a replacement.

				// Build a BrokerList from the
				// IDs in the old replica set to
				// get a *constraints.
				replicaSet := BrokerList{}
				for _, bid := range partn.Replicas {
					replicaSet = append(replicaSet, params.BM[bid])
				}
				// Add existing brokers in the
				// new replica set as well.
				for _, bid := range newPartn.Replicas {
					replicaSet = append(replicaSet, params.BM[bid])
				}

				// Populate a constraints.
				constraints := MergeConstraints(replicaSet)

				// Add any necessary meta from current partition
				// to the constraints.
				if params.Strategy == "storage" {
					s, err := params.PMM.Size(partn)
					if err != nil {
						e := fmt.Errorf("%s p%d: %s", partn.Topic, partn.Partition, err.Error())
						errs = append(errs, e)
						continue
					}

					constraints.requestSize = s * params.PartnSzFactor
				}

				// Fetch the best candidate and append.
				replacement, err := bl.BestCandidate(constraints, params.Strategy, 1)

				if err != nil {
					// Append any caught errors.
					e := fmt.Errorf("%s p%d: %s", partn.Topic, partn.Partition, err.Error())
					errs = append(errs, e)
					continue
				}

				newPartn.Replicas = append(newPartn.Replicas, replacement.ID)
			}
		}

		// Add the partition to the
		// new map.
		newMap.Partitions = append(newMap.Partitions, newPartn)
	}

	// Final check to ensure that no
	// replica sets were somehow set to 0.
	for _, partn := range newMap.Partitions {
		if len(partn.Replicas) == 0 {
			e := fmt.Errorf("%s p%d: configured to zero replicas", partn.Topic, partn.Partition)
			errs = append(errs, e)
		}
	}

	// Return map, errors.
	return newMap, errs
}

// LocalitiesAvailable takes a broker map and broker and returns a []string
// of localities that are unused by any of the brokers in any replica sets that
// the reference broker was found in. This is done by building a set of all
// localities observed across all replica sets and a set of all localities
// observed in replica sets containing the reference broker,
// then returning the diff.
func (pm *PartitionMap) LocalitiesAvailable(bm BrokerMap, b *Broker) []string {
	all := map[string]struct{}{}
	reference := map[string]struct{}{}

	// Traverse the partition map and
	// gather localities.
	for _, partn := range pm.Partitions {

		localities := map[string]struct{}{}
		var containsRef bool

		for _, replica := range partn.Replicas {
			// Check if this is a replica set
			// that contains the reference
			if replica == b.ID {
				containsRef = true
				// We shouldn't have the reference
				// broker's locality since it's
				// missing; the purpose of this
				// entire method is to infer it
				// or a compatible locality. Skip.
				continue
			}

			// Add the locality.
			l := bm[replica].Locality
			if l != "" {
				localities[l] = struct{}{}
			}
		}

		// Populate into the appropriate
		// set.
		if containsRef {
			for k := range localities {
				reference[k] = struct{}{}
			}
		} else {
			for k := range localities {
				all[k] = struct{}{}
			}
		}
	}

	// Get the diff between the all set
	// and the reference set.
	diff := []string{}
	for l := range all {
		if _, used := reference[l]; !used {
			diff = append(diff, l)
		}
	}

	sort.Strings(diff)

	return diff
}

func (pm *PartitionMap) shuffle(f func(Partition) bool) {
	var s int
	for n := range pm.Partitions {
		if f(pm.Partitions[n]) {
			rand.Seed(int64(s << 20))
			s++
			rand.Shuffle(len(pm.Partitions[n].Replicas), func(i, j int) {
				pm.Partitions[n].Replicas[i], pm.Partitions[n].Replicas[j] = pm.Partitions[n].Replicas[j], pm.Partitions[n].Replicas[i]
			})
		}
	}
}

// PartitionMapFromString takes a json encoded string and returns a *PartitionMap.
func PartitionMapFromString(s string) (*PartitionMap, error) {
	pm := NewPartitionMap()

	err := json.Unmarshal([]byte(s), &pm)
	if err != nil {
		return nil, fmt.Errorf("Error parsing partition map: %s", err.Error())
	}

	sort.Sort(pm.Partitions)

	return pm, nil
}

// PartitionMapFromZK takes a slice of regexp and finds all matching topics for
// each. A merged *PartitionMap of all matching topic maps is returned.
func PartitionMapFromZK(t []*regexp.Regexp, zk Handler) (*PartitionMap, error) {
	// Get a list of topic names from Handler
	// matching the provided list.
	topicsToRebuild, err := zk.GetTopics(t)
	if err != nil {
		return nil, err
	}

	// Err if no matching topics were found.
	if len(topicsToRebuild) == 0 {
		return nil, fmt.Errorf("No topics found matching: %s", t)
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

// SetReplication ensures that replica sets is reset to the replication
// factor r. Sets exceeding r are truncated, sets below r are extended
// with stub brokers.
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

// Equal checks the equality betwee two partition maps. Equality requires
// that the total order is exactly the same.
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

		for n := range p1.Replicas {
			if p1.Replicas[n] != p2.Replicas[n] {
				return false, errors.New("replica")
			}
		}
	}

	return true, nil
}

// Strip takes a PartitionMap and returns a copy where all broker ID
// references are replaced with the stub broker (ID == StubBrokerID) with
// the replace field is set to true. This ensures that the entire map is
// rebuilt, even if the provided broker list matches what's already in the map.
func (pm *PartitionMap) Strip() *PartitionMap {
	Stripped := NewPartitionMap()

	// Copy each partition sans the replicas list.
	// The new replica list is all stub brokers.
	for _, p := range pm.Partitions {
		var stubs = make([]int, len(p.Replicas))
		for i := range stubs {
			stubs[i] = StubBrokerID
		}

		part := Partition{
			Topic:     p.Topic,
			Partition: p.Partition,
			Replicas:  stubs,
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

// UseStats returns a map of broker IDs to BrokerUseStats; each
// contains a count of leader and follower partition assignments.
func (pm *PartitionMap) UseStats() []*BrokerUseStats {
	smap := map[int]*BrokerUseStats{}
	// Get counts.
	for _, p := range pm.Partitions {
		for i, b := range p.Replicas {
			if _, exists := smap[b]; !exists {
				smap[b] = &BrokerUseStats{
					ID: b,
				}
			}
			// Idx 0 for each replica set
			// is a leader assignment.
			if i == 0 {
				smap[b].Leader++
			} else {
				smap[b].Follower++
			}
		}
	}

	stats := BrokerUseStatsList{}
	for _, b := range smap {
		stats = append(stats, b)
	}

	sort.Sort(stats)

	return stats
}

// Equal defines equalty between two Partition objects
// as an equality of topic, partition and replicas.
func (p Partition) Equal(p2 Partition) bool {
	switch {
	case p.Topic != p2.Topic:
		return false
	case p.Partition != p2.Partition:
		return false
	case len(p.Replicas) != len(p2.Replicas):
		return false
	}

	for i := range p.Replicas {
		if p.Replicas[i] != p2.Replicas[i] {
			return false
		}
	}

	return true
}
