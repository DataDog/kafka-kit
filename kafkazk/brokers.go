package kafkazk

import (
	"errors"
	"fmt"
	"os"
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
	Leader   int
	Follower int
}

// BrokerStatus summarizes change counts
// from an input and output broker list.
type BrokerStatus struct {
	New        int
	Missing    int
	OldMissing int
	Replace    int
}

// broker is used for internal
// metadata / accounting.
type broker struct {
	id          int
	locality    string
	used        int
	storageFree float64
	replace     bool
}

// BrokerMap holds a mapping of
// broker IDs to *broker.
type BrokerMap map[int]*broker

// brokerList is a slice of
// brokers for sorting by used count.
type brokerList []*broker

// Wrapper types for sort by
// methods.
type byCount brokerList
type byStorage brokerList

// Satisfy the sort interface for brokerList types.

// By used field value.
func (b byCount) Len() int      { return len(b) }
func (b byCount) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byCount) Less(i, j int) bool {
	if b[i].used < b[j].used {
		return true
	}
	if b[i].used > b[j].used {
		return false
	}

	return b[i].id < b[j].id
}

// By storageFree value.
func (b byStorage) Len() int      { return len(b) }
func (b byStorage) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byStorage) Less(i, j int) bool {
	if b[i].storageFree > b[j].storageFree {
		return true
	}
	if b[i].storageFree < b[j].storageFree {
		return false
	}

	return b[i].id < b[j].id
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
				b[id].replace = true
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
		if broker.id == 0 {
			continue
		}

		if _, ok := newBrokers[broker.id]; !ok {
			bs.Replace++
			b[broker.id].replace = true
			fmt.Printf("%sBroker %d marked for removal\n",
				indent, broker.id)
		}
	}

	// Merge new brokers with existing brokers.
	for id := range newBrokers {
		// Don't overwrite existing (which will be most brokers).
		if b[id] == nil {
			// Skip metadata lookups if
			// meta is not being used.
			if len(bm) == 0 {
				b[id] = &broker{
					used:    0,
					id:      id,
					replace: false,
				}
				bs.New++
				continue
			}

			// Else check the broker against
			// the broker metadata map.
			if meta, exists := bm[id]; exists {
				b[id] = &broker{
					used:     0,
					id:       id,
					replace:  false,
					locality: meta.Rack,
				}
				bs.New++
			} else {
				bs.Missing++
				fmt.Printf("%sBroker %d not found in ZooKeeper\n",
					indent, id)
			}
		}
	}

	return bs
}

// SubStorage takes a PartitionMap + PartitionMetaMap and adds
// the size of each partition back to the storageFree value
// of any broker it was originally mapped to.
// This is used in a force rebuild where the assumption
// is that partitions will be lifted and repositioned.
func (b BrokerMap) SubStorage(pm *PartitionMap, pmm PartitionMetaMap) error {
	// Get the size of each partition.
	for _, partn := range pm.Partitions {
		size, err := pmm.Size(partn)
		if err != nil {
			return err
		}

		// Add this size back to the
		// storageFree for all mapped brokers.
		for _, bid := range partn.Replicas {
			if b, exists := b[bid]; exists {
				b.storageFree += size
			} else {
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
		if !b[broker].replace {
			bl = append(bl, b[broker])
		}
	}

	return bl
}

// BrokerMapFromTopicMap creates a BrokerMap
// from a topicMap. Counts occurance is counted.
// XXX can we remove marked for replacement here too?
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
				bmap[id] = &broker{used: 0, id: id, replace: false}
			}

			// Track use scoring unless we're
			// doing a force rebuild. In this case,
			// we're treating existing brokers the same
			// as new brokers (which start with a score of 0).
			if !force {
				bmap[id].used++
			}

			// Add metadata if we have it.
			if meta, exists := bm[id]; exists {
				bmap[id].locality = meta.Rack
				bmap[id].storageFree = meta.StorageFree
			}
		}
	}

	// Broker ID 0 is used for --force-rebuild.
	// We request a Stripped map which replaces
	// all existing brokers with the fake broker
	// with ID set for replacement.
	bmap[0] = &broker{used: 0, id: 0, replace: true}

	return bmap
}

// Copy returns a copy of a BrokerMap.
func (b BrokerMap) Copy() BrokerMap {
	c := BrokerMap{}
	for id, br := range b {
		c[id] = &broker{
			id:          br.id,
			locality:    br.locality,
			used:        br.used,
			storageFree: br.storageFree,
			replace:     br.replace,
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
