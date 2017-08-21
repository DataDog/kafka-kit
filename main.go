package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	Config struct {
		partitions   int
		replicas     int
		rebuildMap   string
		rebuildTopic string
		brokers      []int
		useMeta      bool
		zkAddr       string
		zkPrefix     string
	}

	zkc = &zkConfig{}

	errNoBrokers = errors.New("No additional brokers that meet contstraints")
)

const (
	indent = "  "
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

// broker is used for internal
// metadata / accounting.
type broker struct {
	id       int
	locality string
	used     int
	replace  bool
}

// brokerMap holds a mapping of
// broker IDs to *broker.
type brokerMap map[int]*broker

// brokerList is a slice of
// brokers for sorting by used count.
type brokerList []*broker

// Satisfy the sort interface for brokerList.

func (b brokerList) Len() int      { return len(b) }
func (b brokerList) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b brokerList) Less(i, j int) bool {
	if b[i].used < b[j].used {
		return true
	}
	if b[i].used > b[j].used {
		return false
	}

	return b[i].id < b[j].id
}

type constraints struct {
	locality map[string]bool
	id       map[int]bool
}

func newConstraints() *constraints {
	return &constraints{
		locality: make(map[string]bool),
		id:       make(map[int]bool),
	}
}

func init() {
	fmt.Println()
	flag.StringVar(&Config.rebuildMap, "rebuild-map", "", "Rebuild a topic map")
	flag.StringVar(&Config.rebuildTopic, "rebuild-topic", "", "Rebuild a topic by lookup in ZooKeeper")
	flag.BoolVar(&Config.useMeta, "use-meta", true, "use broker metadata for constraints")
	flag.StringVar(&Config.zkAddr, "zk-addr", "localhost:2181", "ZooKeeper connect string (for broker metadata)")
	flag.StringVar(&Config.zkPrefix, "zk-prefix", "", "ZooKeeper namespace prefix")
	brokers := flag.String("brokers", "", "new brokers list")

	flag.Parse()

	// Sanity check params.
	switch {
	case Config.rebuildMap == "" && Config.rebuildTopic == "":
		fmt.Println("Must specify either -rebuild-map or -rebuild-topic")
		defaultsAndExit()
	case len(*brokers) == 0:
		fmt.Println("Broker list cannot be empty")
		defaultsAndExit()
	}

	Config.brokers = brokerStringToSlice(*brokers)
}

func defaultsAndExit() {
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	// ZooKeeper init.
	if Config.useMeta || Config.rebuildTopic != "" {
		// ZooKeeper config params.
		zkc = &zkConfig{
			ConnectString: Config.zkAddr,
			Prefix:        Config.zkPrefix}

		// Init the ZK client.
		err := initZK(zkc)
		if err != nil {
			fmt.Printf("Error connecting to ZooKeeper: %s\n", err)
			os.Exit(1)
		}
	}

	// Fetch broker metadata.
	var brokerMetadata brokerMetaMap
	if Config.useMeta {
		var err error
		// Fetch broker metadata.
		brokerMetadata, err = getAllBrokerMeta(zkc)
		if err != nil {
			fmt.Printf("Error fetching metadata: %s\n", err)
			os.Exit(1)
		}
	}

	partitionMapIn := newPartitionMap()

	switch {
	case Config.rebuildMap != "":
		err := json.Unmarshal([]byte(Config.rebuildMap), &partitionMapIn)
		if err != nil {
			fmt.Printf("Error parsing topic map: %s\n", err)
			os.Exit(1)
		}
	case Config.rebuildTopic != "":
		pmap, err := partitionMapFromZk(zkc, Config.rebuildTopic)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		partitionMapIn = pmap
	}

	// Get a broker map of the brokers in the current topic map.
	brokers := brokerMapFromTopicMap(partitionMapIn, brokerMetadata)

	// Update the currentBrokers list with
	// the provided broker list.
	brokers.update(Config.brokers, brokerMetadata)

	// Build a new map using the provided list of brokers.
	partitionMapOut, warns := partitionMapIn.rebuild(brokers)

	// Sort by topic, partition.
	sort.Sort(partitionMapIn.Partitions)
	sort.Sort(partitionMapOut.Partitions)

	// TODO scan partition lists
	// and ensure they're the same
	// topic, partition.

	// Print advisory warnings.
	fmt.Println("\nWARN:")
	if len(warns) > 0 {
		sort.Strings(warns)
		for _, e := range warns {
			fmt.Printf("%s%s\n", indent, e)
		}
	} else {
		fmt.Printf("%s[none]\n", indent)
	}

	// Get a status string of what's changed.
	fmt.Println("\nChanges:")
	for i := range partitionMapIn.Partitions {
		change := whatChanged(partitionMapIn.Partitions[i].Replicas,
			partitionMapOut.Partitions[i].Replicas)

		fmt.Printf("%s%s p%d: %v -> %v %s\n",
			indent,
			partitionMapIn.Partitions[i].Topic,
			partitionMapIn.Partitions[i].Partition,
			partitionMapIn.Partitions[i].Replicas,
			partitionMapOut.Partitions[i].Replicas,
			change)
	}

	// Print the new partition map.
	fmt.Println("\nNew partition map:\n")
	out, err := json.Marshal(partitionMapOut)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(string(out))
}

// Rebuild takes a partition map and a brokerMap. It
// traverses the partition map and replaces brokers marked
// for removal with the best available candidate.
func (pm partitionMap) rebuild(bm brokerMap) (*partitionMap, []string) {
	newMap := newPartitionMap()
	// We need a filtered list for
	// usage sorting and exclusion
	// of nodes marked for removal.
	bl := bm.filteredList()

	var errs []string

	// For each partition item in the
	// partitions list.
	for _, item := range pm.Partitions {
		newP := Partition{Partition: item.Partition, Topic: item.Topic}

		// Build a brokerList from the
		// IDs in the replica set to
		// get a *constraints.
		replicaSet := brokerList{}
		for _, bid := range item.Replicas {
			replicaSet = append(replicaSet, bm[bid])
		}
		constraints := mergeConstraints(replicaSet)

		// For each replica in the partition item.
		for n, bid := range item.Replicas {
			isLeader := false
			if n == 0 {
				isLeader = true
			}
			// If the broker ID is marked as replace
			// in the broker map, get a new ID.
			if bm[bid].replace {
				// Fetch the best candidate, append.
				newBroker, err := bl.bestCandidate(constraints, isLeader)
				if err != nil {
					// Append any caught errors.
					errString := fmt.Sprintf("Partition %d: %s", item.Partition, err.Error())
					errs = append(errs, errString)
					continue
				}

				newP.Replicas = append(newP.Replicas, newBroker.id)
			} else {
				// Otherwise keep the broker where it is.
				newP.Replicas = append(newP.Replicas, bid)
			}
		}

		// Append the partition item to the new partitionMap.
		newMap.Partitions = append(newMap.Partitions, newP)
	}

	return newMap, errs
}

// bestCandidate takes a *constraints
// and returns the *broker with the lowest used
// count that satisfies all constraints.
func (b brokerList) bestCandidate(c *constraints, l bool) (*broker, error) {
	sort.Sort(b)

	var score int
	if l {
		score = 2
	} else {
		score = 1
	}

	var candidate *broker

	// Iterate over candidates.
	for _, candidate = range b {
		// Candidate passes, return.
		if c.passes(candidate) {
			c.add(candidate)
			candidate.used += score

			return candidate, nil
		}
	}

	// List exhausted, no brokers passed.
	return nil, errNoBrokers
}

// mergeConstraints takes a brokerlist and
// builds a *constraints by merging the
// attributes of all brokers from the supplied list.
func mergeConstraints(bl brokerList) *constraints {
	c := newConstraints()

	for _, b := range bl {
		// Don't merge in attributes
		// from nodes that will be removed.
		if b.replace {
			continue
		}

		if b.locality != "" {
			c.locality[b.locality] = true
		}

		c.id[b.id] = true
	}

	return c
}

// update takes a brokerMap and a []int
// of broker IDs and adds them to the brokerMap.
func (b brokerMap) update(bl []int, bm brokerMetaMap) {
	// Build a map from the new broker list.
	newBrokers := map[int]bool{}
	for _, broker := range bl {
		newBrokers[broker] = true
	}

	// Set the replace flag for existing brokers
	// not in the new broker map.
	for _, broker := range b {
		if _, ok := newBrokers[broker.id]; !ok {
			b[broker.id].replace = true
			fmt.Printf("broker %d marked for replacement\n", broker.id)
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
			} else {
				fmt.Printf("broker %d not found in ZooKeeper\n", id)
			}
		}
	}
}

// filteredList converts a brokerMap to a brokerList,
// excluding nodes marked for replacement.
func (b brokerMap) filteredList() brokerList {
	bl := brokerList{}

	for broker := range b {
		if !b[broker].replace {
			bl = append(bl, b[broker])
		}
	}

	return bl
}

// add takes a *broker and adds its
// attributes to the *constraints.
func (c *constraints) add(b *broker) {
	if b.locality != "" {
		c.locality[b.locality] = true
	}

	c.id[b.id] = true
}

// passes takes a *broker and returns
// whether or not it passes constraints.
func (c *constraints) passes(b *broker) bool {
	switch {
	// Fail if the candidate is one of the
	// IDs already in the replica set.
	case c.id[b.id]:
		return false
	// Fail if the candidate is in any of
	// the existing replica set localities.
	case c.locality[b.locality]:
		return false
	}

	return true
}

// brokerMapFromTopicMap creates a brokerMap
// from a topicMap. Counts occurance is counted.
// TODO can we remove marked for replacement here too?
func brokerMapFromTopicMap(pm *partitionMap, bm brokerMetaMap) brokerMap {
	bmap := brokerMap{}
	// For each partition.
	for _, partition := range pm.Partitions {
		// For each broker in the
		// partition replica set.
		for n, id := range partition.Replicas {
			// Add a point if the
			// broker is a leader.
			var score int
			if n == 0 {
				score = 2
			} else {
				score = 1
			}
			// If the broker isn't in the
			// broker map, add it.
			if bmap[id] == nil {
				bmap[id] = &broker{used: score, id: id, replace: false}
			} else {
				// Else increment used.
				bmap[id].used += score
			}

			// Add metadata if we have it.
			if meta, exists := bm[id]; exists {
				bmap[id].locality = meta.Rack
			}
		}
	}

	return bmap
}

// whatChanged takes a before and after broker
// replica set and returns a string describing
// what changed.
// TODO this needs to handle different length inputs.
func whatChanged(s1 []int, s2 []int) string {
	a, b := make([]int, len(s1)), make([]int, len(s1))
	copy(a, s1)
	copy(b, s2)

	var changed bool

	for i := range a {
		if a[i] != b[i] {
			changed = true
		}
	}

	// Nothing changed.
	if !changed {
		return ""
	}

	sort.Ints(a)
	sort.Ints(b)

	samePostSort := true

	// The replica set was changed.
	// Sort it and figure out how.

	for i := range a {
		if a[i] != b[i] {
			samePostSort = false
		}
	}

	// If we're here, the replica set
	// was the same but in a different order.
	if samePostSort {
		return "preferred leader"
	}

	// Otherwise, a broker was added
	// or removed.
	return "replaced broker"

}

// brokerStringToSlice takes a broker list
// as a string and returns a []int of
// broker IDs.
func brokerStringToSlice(s string) []int {
	ids := map[int]bool{}
	var info int

	parts := strings.Split(s, ",")
	is := []int{}

	// Iterate and convert
	// each broker ID.
	for _, p := range parts {
		i, err := strconv.Atoi(p)
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
