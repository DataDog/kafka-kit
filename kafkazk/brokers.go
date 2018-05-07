package kafkazk

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
)

// BrokerMetaMap is a map of broker IDs
// to BrokerMeta metadata fetched from
// ZooKeeper. Currently, just the rack
// field is retrieved.
type BrokerMetaMap map[int]*BrokerMeta

// BrokerMeta holds metadata that
// describes a broker, used in satisfying
// constraints.
type BrokerMeta struct {
	Rack        string  `json:"rack"`
	StorageFree float64 // In bytes.
}

// BrokerMetricsMap holds a mapping of broker
// ID to BrokerMetrics.
type BrokerMetricsMap map[int]*BrokerMetrics

// BrokerMetrics holds broker metric
// data fetched from ZK.
type BrokerMetrics struct {
	StorageFree float64
}

// BrokerUseStats holds counts
// of partition ownership.
type BrokerUseStats struct {
	ID       int
	Leader   int
	Follower int
}

// BrokerUseStatsList is a slice of *BrokerUseStats.
type BrokerUseStatsList []*BrokerUseStats

func (b BrokerUseStatsList) Len() int           { return len(b) }
func (b BrokerUseStatsList) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b BrokerUseStatsList) Less(i, j int) bool { return b[i].ID < b[j].ID }

// BrokerStatus summarizes change counts
// from an input and output broker list.
type BrokerStatus struct {
	New        int
	Missing    int
	OldMissing int
	Replace    int
}

// Broker is used for internal
// metadata / accounting.
type Broker struct {
	ID          int
	Locality    string
	Used        int
	StorageFree float64
	Replace     bool
	New         bool
}

// BrokerMap holds a mapping of
// broker IDs to *Broker.
type BrokerMap map[int]*Broker

// brokerList is a slice of
// brokers for sorting by used count.
type brokerList []*Broker

// Wrapper types for sort by
// methods.
type brokersByCount brokerList
type brokersByStorage brokerList

// Satisfy the sort interface for brokerList types.

// By used field value.
func (b brokersByCount) Len() int      { return len(b) }
func (b brokersByCount) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b brokersByCount) Less(i, j int) bool {
	if b[i].Used < b[j].Used {
		return true
	}
	if b[i].Used > b[j].Used {
		return false
	}

	return b[i].ID < b[j].ID
}

// By StorageFree value.
func (b brokersByStorage) Len() int      { return len(b) }
func (b brokersByStorage) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b brokersByStorage) Less(i, j int) bool {
	if b[i].StorageFree > b[j].StorageFree {
		return true
	}
	if b[i].StorageFree < b[j].StorageFree {
		return false
	}

	return b[i].ID < b[j].ID
}

// SortPseudoShuffle takes a brokerList and performs a
// sort by count. For each sequence of brokers with
// equal counts, the sub-slice is pseudo random shuffled
// using the provided seed value s.
func (b brokerList) SortPseudoShuffle(seed int64) {
	sort.Sort(brokersByCount(b))

	if len(b) <= 2 {
		return
	}

	rand.Seed(seed)

	s := 0
	stop := len(b) - 1
	currVal := b[0].Used

	// For each continuous run of
	// a given Used value, shuffle
	// that range of the slice.
	for k := range b {
		switch {
		case b[k].Used != currVal:
			currVal = b[k].Used
			rand.Shuffle(len(b[s:k]), func(i, j int) {
				b[s:k][i], b[s:k][j] = b[s:k][j], b[s:k][i]
			})
			s = k
		case k == stop:
			rand.Shuffle(len(b[s:]), func(i, j int) {
				b[s:][i], b[s:][j] = b[s:][j], b[s:][i]
			})
		}
	}
}

// Update takes a BrokerMap and a []int
// of broker IDs and adds them to the BrokerMap,
// returning the count of marked for replacement,
// newly included, and brokers that weren't found
// in ZooKeeper.
func (b BrokerMap) Update(bl []int, bm BrokerMetaMap) *BrokerStatus {
	bs := &BrokerStatus{}

	// Build a map from the new broker list.
	newBrokers := map[int]bool{}
	for _, broker := range bl {
		newBrokers[broker] = true
	}

	// Do an initial pass on existing brokers
	// and see if any are missing in ZooKeeper.
	if len(bm) > 0 {
		for id := range b {
			// Skip reserved ID 0.
			if id == 0 {
				continue
			}

			if _, exist := bm[id]; !exist {
				fmt.Printf("%sPrevious broker %d missing\n",
					indent, id)
				b[id].Replace = true
				// If this broker is missing and was provided in
				// the broker list, consider it a "missing provided broker".
				if _, ok := newBrokers[id]; len(bm) > 0 && ok {
					bs.Missing++
				} else {
					bs.OldMissing++
				}
			}
		}
	}

	// Set the replace flag for existing brokers
	// not in the new broker map.
	for _, broker := range b {
		// Broker ID 0 is a special stub
		// ID used for internal purposes.
		// Skip it.
		if broker.ID == 0 {
			continue
		}

		if _, ok := newBrokers[broker.ID]; !ok {
			bs.Replace++
			b[broker.ID].Replace = true
			fmt.Printf("%sBroker %d marked for removal\n",
				indent, broker.ID)
		}
	}

	// Merge new brokers with existing brokers.
	for id := range newBrokers {
		// Don't overwrite existing (which will be most brokers).
		if b[id] == nil {
			// Skip metadata lookups if
			// meta is not being used.
			if len(bm) == 0 {
				b[id] = &Broker{
					Used:    0,
					ID:      id,
					Replace: false,
					New:     true,
				}
				bs.New++
				continue
			}

			// Else check the broker against
			// the broker metadata map.
			if meta, exists := bm[id]; exists {
				b[id] = &Broker{
					Used:        0,
					ID:          id,
					Replace:     false,
					Locality:    meta.Rack,
					StorageFree: meta.StorageFree,
					New:         true,
				}
				bs.New++
			} else {
				bs.Missing++
				fmt.Printf("%sBroker %d not found in ZooKeeper\n",
					indent, id)
			}
		}
	}

	// Log new brokers.
	for _, broker := range b {
		if broker.New {
			fmt.Printf("%sNew broker %d\n", indent, broker.ID)
		}
	}

	return bs
}

// SubstitutionAffinities finds all brokers marked for replacement and for
// each broker, it creates an exclusive association with a newly provided broker.
// In the rebuild stage, each to-be-replaced broker will be only replaced with the
// affinity it's associated with. A given new broker can only be an affinity
// for a single outgoing broker. An error is returned if a complete
// mapping of affinities cannot be constructed (e.g. two brokers are
// marked for replacement but only one new replacement was provided
// and substitution affinities is enabled).
func (b BrokerMap) SubstitutionAffinities() (SubstitutionAffinities, error) {
	replace := map[*Broker]interface{}{}
	new := map[*Broker]interface{}{}
	affinities := SubstitutionAffinities{}

	// Map replacements and new brokers.
	for _, broker := range b {
		if broker.ID == 0 {
			continue
		}

		if broker.Replace {
			replace[broker] = nil
		}

		if broker.New {
			new[broker] = nil
		}
	}

	// Check if we have enough new nodes
	// to cover replacements.
	if len(new) < len(replace) {
		return nil, errors.New("Insufficient number of new brokers")
	}

	// For each broker being replaced, find
	// replacement with the same Rack ID.
	for broker := range replace {
		match, err := constraintsMatch(broker, new)
		if err != nil {
			return affinities, err
		}

		affinities[broker.ID] = match
	}

	return affinities, nil
}

// constraintsMatch takes a *Broker and a map[*Broker]interface{}.
// The map is traversed for a broker that matches the constraints
// of the provided broker. If one is available, it's removed from
// the map and returned. Otherwise, an error is returned.
func constraintsMatch(b *Broker, bm map[*Broker]interface{}) (*Broker, error) {
	for broker := range bm {
		if broker.Locality == b.Locality {
			delete(bm, b)
			return broker, nil
		}
	}

	// No match was found.
	errS := fmt.Sprintf("Insufficient free brokers for locality %s", b.Locality)
	return nil, errors.New(errS)
}

// SubStorageAll takes a PartitionMap + PartitionMetaMap and adds
// the size of each partition back to the StorageFree value
// of any broker it was originally mapped to.
// This is used in a force rebuild where the assumption
// is that partitions will be lifted and repositioned.
func (b BrokerMap) SubStorageAll(pm *PartitionMap, pmm PartitionMetaMap) error {
	// Get the size of each partition.
	for _, partn := range pm.Partitions {
		size, err := pmm.Size(partn)
		if err != nil {
			return err
		}

		// Add this size back to the
		// StorageFree for all mapped brokers.
		for _, bid := range partn.Replicas {
			if broker, exists := b[bid]; exists {
				broker.StorageFree += size
			} else {
				errS := fmt.Sprintf("Broker %d not found in broker map", bid)
				return errors.New(errS)
			}
		}
	}

	return nil
}

// SubStorageReplacements works similarly to SubStorageAll except
// that storage usage is only subtraced from brokers marked for replacement.
func (b BrokerMap) SubStorageReplacements(pm *PartitionMap, pmm PartitionMetaMap) error {
	// Get the size of each partition.
	for _, partn := range pm.Partitions {
		size, err := pmm.Size(partn)
		if err != nil {
			return err
		}

		// Add this size back to the
		// StorageFree for all mapped brokers.
		for _, bid := range partn.Replicas {
			broker, exists := b[bid]
			if exists && broker.Replace {
				broker.StorageFree += size
			}
			if !exists {
				errS := fmt.Sprintf("Broker %d not found in broker map", bid)
				return errors.New(errS)
			}
		}
	}

	return nil
}

// filteredList converts a BrokerMap to a brokerList,
// excluding nodes marked for replacement.
func (b BrokerMap) filteredList() brokerList {
	bl := brokerList{}

	for broker := range b {
		if !b[broker].Replace {
			bl = append(bl, b[broker])
		}
	}

	return bl
}

// BrokerMapFromTopicMap creates a BrokerMap
// from a topicMap. Counts occurance is counted.
// TODO can we remove marked for replacement here too?
func BrokerMapFromTopicMap(pm *PartitionMap, bm BrokerMetaMap, force bool) BrokerMap {
	bmap := BrokerMap{}
	// For each partition.
	for _, partition := range pm.Partitions {
		// For each broker in the
		// partition replica set.
		for _, id := range partition.Replicas {
			// If the broker isn't in the
			// broker map, add it.
			if bmap[id] == nil {
				// If we're doing a force rebuid, replace
				// should be set to true.
				bmap[id] = &Broker{Used: 0, ID: id, Replace: false}
			}

			// Track use scoring unless we're
			// doing a force rebuild. In this case,
			// we're treating existing brokers the same
			// as new brokers (which start with a score of 0).
			if !force {
				bmap[id].Used++
			}

			// Add metadata if we have it.
			if meta, exists := bm[id]; exists {
				bmap[id].Locality = meta.Rack
				bmap[id].StorageFree = meta.StorageFree
			}
		}
	}

	// Broker ID 0 is used for --force-rebuild.
	// We request a Stripped map which replaces
	// all existing brokers with the fake broker
	// with ID set for replacement.
	bmap[0] = &Broker{Used: 0, ID: 0, Replace: true}

	return bmap
}

// MappedBrokers takes a PartitionMap and returns a
// new BrokerMap that only includes brokers found
// in the partition map holding a partition.
func (b BrokerMap) MappedBrokers(pm *PartitionMap) BrokerMap {
	bmap := BrokerMap{}

	ids := map[int]interface{}{}

	// Get all IDs.
	for _, partition := range pm.Partitions {
		for _, id := range partition.Replicas {
			ids[id] = nil
		}
	}

	// For each ID that's in the BrokerMap,
	// add to the new BrokerMap.
	for id := range ids {
		if _, exists := b[id]; exists {
			bmap[id] = &Broker{
				ID:          id,
				Locality:    b[id].Locality,
				Used:        b[id].Used,
				StorageFree: b[id].StorageFree,
				Replace:     b[id].Replace,
			}
		}
	}

	return bmap
}

// NonReplacedBrokers returns a copy of a BrokerMap
// that excludes all brokers marked for replacement.
func (b BrokerMap) NonReplacedBrokers() BrokerMap {
	bmap := BrokerMap{}

	// For each ID that's in the BrokerMap
	// and not marked for replacement,
	// add to the new BrokerMap.
	for id := range b {
		if !b[id].Replace {
			bmap[id] = &Broker{
				ID:          id,
				Locality:    b[id].Locality,
				Used:        b[id].Used,
				StorageFree: b[id].StorageFree,
			}
		}
	}

	return bmap
}

// StorageDiff takes two BrokerMaps and returns
// a per broker ID diff in storage as a [2]float64:
// [absolute, percentage] diff.
func (b BrokerMap) StorageDiff(b2 BrokerMap) map[int][2]float64 {
	d := map[int][2]float64{}

	for bid := range b {
		if _, exist := b2[bid]; !exist {
			continue
		}

		diff := b2[bid].StorageFree - b[bid].StorageFree
		p := diff / b[bid].StorageFree * 100
		d[bid] = [2]float64{diff, p}
	}

	return d
}

// StorageRangeSpread returns the range spread
// of free storage for all brokers in the BrokerMap.
func (b BrokerMap) StorageRangeSpread() float64 {
	// Get the high/low StorageFree values.
	h, l := 0.00, math.MaxFloat64

	for id := range b {
		if id == 0 {
			continue
		}

		v := b[id].StorageFree

		// Update the high/low.
		if v > h {
			h = v
		}

		if v < l {
			l = v
		}
	}

	// Return range spread.
	return (h - l) / l * 100
}

// StorageStdDev returns the standard deviation
// of free storage for all brokers in the BrokerMap.
func (b BrokerMap) StorageStdDev() float64 {
	var m float64
	var t float64
	var s float64
	var l float64

	for id := range b {
		if id == 0 {
			continue
		}
		l++
		t += b[id].StorageFree
	}

	m = t / l

	for id := range b {
		if id == 0 {
			continue
		}
		s += math.Pow(m-b[id].StorageFree, 2)
	}

	msq := s / l

	return math.Sqrt(msq)
}

// Copy returns a copy of a BrokerMap.
func (b BrokerMap) Copy() BrokerMap {
	c := BrokerMap{}
	for id, br := range b {
		c[id] = &Broker{
			ID:          br.ID,
			Locality:    br.Locality,
			Used:        br.Used,
			StorageFree: br.StorageFree,
			Replace:     br.Replace,
		}
	}

	return c
}

// BrokerStringToSlice takes a broker list
// as a string and returns a []int of
// broker IDs.
func BrokerStringToSlice(s string) []int {
	ids := map[int]bool{}
	var info int

	parts := strings.Split(s, ",")
	is := []int{}

	// Iterate and convert
	// each broker ID.
	for _, p := range parts {
		i, err := strconv.Atoi(strings.TrimSpace(p))
		// Err and exit on bad input.
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if ids[i] {
			fmt.Printf("ID %d supplied as duplicate, excluding\n", i)
			info++
			continue
		}

		ids[i] = true
		is = append(is, i)
	}

	// Formatting purposes.
	if info > 0 {
		fmt.Println()
	}

	return is
}
