package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
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
	log.SetOutput(ioutil.Discard)

	fmt.Fprintln(os.Stderr)
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
		fmt.Fprintln(os.Stderr, "Must specify either -rebuild-map or -rebuild-topic")
		defaultsAndExit()
	case len(*brokers) == 0:
		fmt.Fprintln(os.Stderr, "Broker list cannot be empty")
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
			fmt.Fprintf(os.Stderr, "Error connecting to ZooKeeper: %s\n", err)
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
			fmt.Fprintf(os.Stderr, "Error fetching metadata: %s\n", err)
			os.Exit(1)
		}
	}

	partitionMapIn := newPartitionMap()

	switch {
	case Config.rebuildMap != "":
		err := json.Unmarshal([]byte(Config.rebuildMap), &partitionMapIn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing topic map: %s\n", err)
			os.Exit(1)
		}
	case Config.rebuildTopic != "":
		pmap, err := partitionMapFromZk(zkc, Config.rebuildTopic)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		partitionMapIn = pmap
	}

	fmt.Fprintf(os.Stderr, "Broker summary:\n")

	// Get a broker map of the brokers in the current topic map.
	brokers := brokerMapFromTopicMap(partitionMapIn, brokerMetadata)

	// Update the currentBrokers list with
	// the provided broker list.
	replace, added := brokers.update(Config.brokers, brokerMetadata)
	change := added - replace

	fmt.Fprintf(os.Stderr, "%sReplacing %d, added %d, total count changed by %d\n",
		indent, replace, added, change)

	// Build a new map using the provided list of brokers.
	partitionMapOut, warns := partitionMapIn.rebuild(brokers)

	// Sort by topic, partition.
	sort.Sort(partitionMapIn.Partitions)
	sort.Sort(partitionMapOut.Partitions)

	// TODO scan partition lists
	// and ensure they're the same
	// topic, partition.

	// Print advisory warnings.
	fmt.Fprintln(os.Stderr, "\nWARN:")
	if len(warns) > 0 {
		sort.Strings(warns)
		for _, e := range warns {
			fmt.Fprintf(os.Stderr, "%s%s\n", indent, e)
		}
	} else {
		fmt.Fprintf(os.Stderr, "%s[none]\n", indent)
	}

	// Get a status string of what's changed.
	fmt.Fprintln(os.Stderr, "\nChanges:")
	for i := range partitionMapIn.Partitions {
		change := whatChanged(partitionMapIn.Partitions[i].Replicas,
			partitionMapOut.Partitions[i].Replicas)

		fmt.Fprintf(os.Stderr, "%s%s p%d: %v -> %v %s\n",
			indent,
			partitionMapIn.Partitions[i].Topic,
			partitionMapIn.Partitions[i].Partition,
			partitionMapIn.Partitions[i].Replicas,
			partitionMapOut.Partitions[i].Replicas,
			change)
	}

	// Print the new partition map.
	fmt.Fprintln(os.Stderr, "\nNew partition map:\n")
	out, err := json.Marshal(partitionMapOut)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Println(string(out))
}

// Rebuild takes a brokerMap and traverses
// the partition map, replacing brokers marked
// for removal with the best available candidate.
func (pm partitionMap) rebuild(bm brokerMap) (*partitionMap, []string) {
	newMap := newPartitionMap()
	// We need a filtered list for
	// usage sorting and exclusion
	// of nodes marked for removal.
	bl := bm.filteredList()

	var errs []string

	pass := 0
	// For each partition partn in the
	// partitions list.
start:
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
				errString := fmt.Sprintf("Partition %d: %s", partn.Partition, err.Error())
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
		goto start
	}

	return newMap, errs
}

// bestCandidate takes a *constraints
// and returns the *broker with the lowest used
// count that satisfies all constraints.
func (b brokerList) bestCandidate(c *constraints) (*broker, error) {
	sort.Sort(b)

	var candidate *broker

	// Iterate over candidates.
	for _, candidate = range b {
		// Candidate passes, return.
		if c.passes(candidate) {
			c.add(candidate)
			candidate.used++

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
// of broker IDs and adds them to the brokerMap,
// returning the count of marked for replacement and
// newly included brokers.
func (b brokerMap) update(bl []int, bm brokerMetaMap) (int, int) {
	// Build a map from the new broker list.
	newBrokers := map[int]bool{}
	for _, broker := range bl {
		newBrokers[broker] = true
	}

	// Set the replace flag for existing brokers
	// not in the new broker map.
	marked := 0
	for _, broker := range b {
		if _, ok := newBrokers[broker.id]; !ok {
			marked++
			b[broker.id].replace = true
			fmt.Fprintf(os.Stderr, "%s%d marked for removal\n",
				indent, broker.id)
		}
	}

	// Merge new brokers with existing brokers.
	new := 0
	for id := range newBrokers {
		// Don't overwrite existing (which will be most brokers).
		if b[id] == nil {
			new++
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
				new--
				fmt.Fprintf(os.Stderr, "broker %d not found in ZooKeeper\n", id)
			}
		}
	}

	return marked, new
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
		for _, id := range partition.Replicas {
			// If the broker isn't in the
			// broker map, add it.
			if bmap[id] == nil {
				bmap[id] = &broker{used: 0, id: id, replace: false}
			} else {
				// Else increment used.
				bmap[id].used++
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
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		if ids[i] {
			fmt.Fprintf(os.Stderr, "ID %d supplied as duplicate, excluding\n", i)
			info++
			continue
		}

		ids[i] = true
		is = append(is, i)
	}

	// Formatting purposes.
	if info > 0 {
		fmt.Fprintln(os.Stderr)
	}

	return is
}
