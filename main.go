package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	validTopicNameChar = `[a-zA-Z0-9\\._\\-]`
	indent             = "  "
)

var (
	Config struct {
		rebuildMap    string
		rebuildTopics []*regexp.Regexp
		brokers       []int
		useMeta       bool
		zkAddr        string
		zkPrefix      string
		outFile       string
		ignoreWarns   bool
	}

	zkc = &zkConfig{}
	// Characters allowed in Kafka topic names
	topicNormalChar, _ = regexp.Compile(`[a-zA-Z0-9\\._\\-]`)

	errNoBrokers = errors.New("No additional brokers that meet constraints")
)

// TODO make references to topic map vs
// broker map consistent, e.g. types vs
// func names.

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

type brokerUseStats struct {
	leader   int
	follower int
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
	topics := flag.String("rebuild-topics", "", "Rebuild topics (comma delim list) by lookup in ZooKeeper")
	flag.BoolVar(&Config.useMeta, "use-meta", true, "Use broker metadata as constraints")
	flag.StringVar(&Config.zkAddr, "zk-addr", "localhost:2181", "ZooKeeper connect string (for broker metadata or rebuild-topic lookups)")
	flag.StringVar(&Config.zkPrefix, "zk-prefix", "", "ZooKeeper namespace prefix")
	flag.StringVar(&Config.outFile, "out-file", "", "Output map to file")
	flag.BoolVar(&Config.ignoreWarns, "ignore-warns", false, "Whether a map should be produced if warnings are emitted")
	brokers := flag.String("brokers", "", "Broker list to rebuild topic partition map with")

	flag.Parse()

	// Sanity check params.
	switch {
	case Config.rebuildMap == "" && *topics == "":
		fmt.Fprintln(os.Stderr, "Must specify either -rebuild-map or -rebuild-topics")
		defaultsAndExit()
	case len(*brokers) == 0:
		fmt.Fprintln(os.Stderr, "Broker list cannot be empty")
		defaultsAndExit()
	}

	Config.brokers = brokerStringToSlice(*brokers)
	topicNames := strings.Split(*topics, ",")

	// Determine if regexp was provided in the topic
	// name. If not, set the topic name to ^name$.
	for n, t := range topicNames {
		if !containsRegex(t) {
			topicNames[n] = fmt.Sprintf(`^%s$`, t)
		}
	}

	// Ensure all topic regex compiles.
	for _, t := range topicNames {
		r, err := regexp.Compile(t)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid topic regex: %s\n", t)
			os.Exit(1)
		}

		Config.rebuildTopics = append(Config.rebuildTopics, r)
	}
}

func containsRegex(t string) bool {
	// Check each character of the
	// topic name. If it doesn't contain
	// a legal Kafka topic name character, we're
	// going to assume it's regex.
	for _, c := range t {
		if !topicNormalChar.MatchString(string(c)) {
			return true
		}
	}

	return false
}

func defaultsAndExit() {
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	// ZooKeeper init.
	if Config.useMeta || len(Config.rebuildTopics) > 0 {
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

	// General flow:
	// 1) partitionMap formed from topic data (provided or via zk).
	// brokerMap is build from brokers found in input
	// partitionMap + any new brokers provided from the
	// --brokers param.
	// 2) New partitionMap from origial map rebuild with updated
	// the updated brokerMap; marked brokers are removed and newly
	// provided brokers are swapped in where possible.
	// 3) New map is possibly expanded/rebalanced.
	// 4) Final map output.

	// Fetch broker metadata.
	var brokerMetadata brokerMetaMap
	if Config.useMeta {
		var err error
		brokerMetadata, err = getAllBrokerMeta(zkc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching metadata: %s\n", err)
			os.Exit(1)
		}
	}

	partitionMapIn := newPartitionMap()

	// Build a topic map with either
	// explicit input or by fetching the
	// map data from ZooKeeper.
	switch {
	case Config.rebuildMap != "":
		err := json.Unmarshal([]byte(Config.rebuildMap), &partitionMapIn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing topic map: %s\n", err)
			os.Exit(1)
		}
	case len(Config.rebuildTopics) > 0:
		// Get a list of topic names from ZK
		// matching the provided list.
		topicsToRebuild, err := getTopics(zkc, Config.rebuildTopics)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		pmapMerged := newPartitionMap()
		// Get a partition map for each topic.
		for _, t := range topicsToRebuild {
			pmap, err := partitionMapFromZk(zkc, t)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			// Merge multiple maps.
			pmapMerged.Partitions = append(pmapMerged.Partitions, pmap.Partitions...)
		}

		partitionMapIn = pmapMerged
	}

	// Get a list of affected topics.
	topics := map[string]bool{}
	for _, p := range partitionMapIn.Partitions {
		topics[p.Topic] = true
	}

	fmt.Fprintf(os.Stderr, "Topics:\n")
	for t := range topics {
		fmt.Fprintf(os.Stderr, "%s%s\n", indent, t)
	}

	fmt.Fprintf(os.Stderr, "\nBroker change summary:\n")

	// Get a broker map of the brokers in the current topic map.
	// If meta data isn't being looked up, brokerMetadata will be empty.
	brokers := brokerMapFromTopicMap(partitionMapIn, brokerMetadata)

	// Update the currentBrokers list with
	// the provided broker list.
	replace, added := brokers.update(Config.brokers, brokerMetadata)
	change := added - replace

	// Print change summary.
	fmt.Fprintf(os.Stderr, "%sReplacing %d, added %d, total count changed by %d\n",
		indent, replace, added, change)

	// Print action.
	fmt.Fprintf(os.Stderr, "\nAction:\n")
	expand := false

	switch {
	case change >= 0 && replace > 0:
		fmt.Fprintf(os.Stderr, "%sRebuild topic with %d broker(s) marked for removal\n",
			indent, replace)
	case change > 0 && replace == 0:
		expand = true
		fmt.Fprintf(os.Stderr, "%sExpanding/rebalancing topic with %d broker(s)\n",
			indent, added)
	case change < 0:
		fmt.Fprintf(os.Stderr, "%sShrinking topic by %d broker(s)\n",
			indent, replace)
	default:
		fmt.Fprintf(os.Stderr, "%sno-op\n", indent)
	}

	// Build a new map using the provided list of brokers.
	// This is ok to run even when a no-op is intended.
	partitionMapOut, warns := partitionMapIn.rebuild(brokers)

	// If expand is set.
	if expand {
		//partitionMapOut.expand(brokers)
	}

	// TODO Rebalance.

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
	fmt.Fprintln(os.Stderr, "\nPartition map changes:")
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

	// Get a per-broker count of leader, follower
	// and total partition assignments.
	fmt.Fprintln(os.Stderr, "\nPartitions assigned:")
	useStats := partitionMapOut.useStats()
	for id, use := range useStats {
		fmt.Fprintf(os.Stderr, "%sBroker %d - leader: %d, follower: %d, total: %d\n",
			indent, id, use.leader, use.follower, use.leader+use.follower)
	}

	// Print the new partition map.
	fmt.Fprintln(os.Stderr, "\nNew partition map:")

	// Don't write the output if ignoreWarns is set.
	if !Config.ignoreWarns && len(warns) > 0 {
		fmt.Fprintf(os.Stderr,
			"%sWarnings encountered, partition map not created. Override with --ignore-warns.\n",
			indent)
		os.Exit(1)
	}

	out, err := json.Marshal(partitionMapOut)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	mapOut := string(out)

	// File output.
	if Config.outFile != "" {
		err := ioutil.WriteFile(Config.outFile, []byte(mapOut+"\n"), 0644)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		} else {
			fmt.Fprintf(os.Stderr, "%sMap written to %s\n\n", indent, Config.outFile)
		}
	}

	// Stdout.
	fmt.Println(mapOut)
}

// useStats returns a map of broker IDs
// to brokerUseStats; each contains a count
// of leader and follower partition assignments.
func (pm partitionMap) useStats() map[int]*brokerUseStats {
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
			fmt.Fprintf(os.Stderr, "%sBroker %d marked for removal\n",
				indent, broker.id)
		}
	}

	// Merge new brokers with existing brokers.
	new := 0
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
				new++
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
				new++
			} else {
				fmt.Fprintf(os.Stderr, "%sBroker %d not found in ZooKeeper\n",
					indent, id)
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
