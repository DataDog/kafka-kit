package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
)

const (
	indent = "  "
)

var (
	// Config holds configuration
	// parameters.
	Config struct {
		rebuildMap    string
		rebuildTopics []*regexp.Regexp
		brokers       []int
		useMeta       bool
		zkAddr        string
		zkPrefix      string
		outPath       string
		outFile       string
		ignoreWarns   bool
		forceRebuild  bool
		replication   int
	}

	zkc = &zkConfig{}
	// Characters allowed in Kafka topic names
	topicNormalChar, _ = regexp.Compile(`[a-zA-Z0-9_\\-]`)

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

type brokerStatus struct {
	new        int
	missing    int
	oldMissing int
	replace    int
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
	// Skip init in tests to avoid
	// errors as a result of the
	// sanity checks that follow
	// flag parsing.
	if flag.Lookup("test.v") != nil {
		return
	}

	log.SetOutput(ioutil.Discard)

	fmt.Println()
	flag.StringVar(&Config.rebuildMap, "rebuild-map", "", "Rebuild a topic map")
	topics := flag.String("rebuild-topics", "", "Rebuild topics (comma delim list) by lookup in ZooKeeper")
	flag.BoolVar(&Config.useMeta, "use-meta", true, "Use broker metadata as constraints")
	flag.StringVar(&Config.zkAddr, "zk-addr", "localhost:2181", "ZooKeeper connect string (for broker metadata or rebuild-topic lookups)")
	flag.StringVar(&Config.zkPrefix, "zk-prefix", "", "ZooKeeper namespace prefix")
	flag.StringVar(&Config.outPath, "out-path", "", "Path to write output map files to")
	flag.StringVar(&Config.outFile, "out-file", "", "If defined, write a combined map of all topics to a file")
	flag.BoolVar(&Config.ignoreWarns, "ignore-warns", false, "Whether a map should be produced if warnings are emitted")
	flag.BoolVar(&Config.forceRebuild, "force-rebuild", false, "Forces a rebuild even if all existing brokers are provided")
	flag.IntVar(&Config.replication, "replication", 0, "Change the replication factor")
	brokers := flag.String("brokers", "", "Broker list to rebuild topic partition map with")

	flag.Parse()

	// Sanity check params.
	switch {
	case Config.rebuildMap == "" && *topics == "":
		fmt.Println("Must specify either -rebuild-map or -rebuild-topics")
		defaultsAndExit()
	case len(*brokers) == 0:
		fmt.Println("Broker list cannot be empty")
		defaultsAndExit()
	}

	// Append trailing slash if not included.
	if Config.outPath != "" && !strings.HasSuffix(Config.outPath, "/") {
		Config.outPath = Config.outPath + "/"
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
			fmt.Printf("Invalid topic regex: %s\n", t)
			os.Exit(1)
		}

		Config.rebuildTopics = append(Config.rebuildTopics, r)
	}
}

// containsRegex takes a topic name
// reference and returns whether or not
// it should be interpreted as regex.
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
			fmt.Printf("Error connecting to ZooKeeper: %s\n", err)
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
			fmt.Printf("Error fetching broker metadata: %s\n", err)
			os.Exit(1)
		}
	}

	// Build a topic map with either
	// explicit input or by fetching the
	// map data from ZooKeeper.
	partitionMapIn := newPartitionMap()
	switch {
	case Config.rebuildMap != "":
		pm, err := partitionMapFromString(Config.rebuildMap)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		partitionMapIn = pm
	case len(Config.rebuildTopics) > 0:
		pm, err := partitionMapFromZK(Config.rebuildTopics)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		partitionMapIn = pm
	}

	// Get a list of affected topics.
	topics := map[string]bool{}
	for _, p := range partitionMapIn.Partitions {
		topics[p.Topic] = true
	}

	fmt.Printf("Topics:\n")
	for t := range topics {
		fmt.Printf("%s%s\n", indent, t)
	}

	fmt.Printf("\nBroker change summary:\n")

	// Get a broker map of the brokers in the current topic map.
	// If meta data isn't being looked up, brokerMetadata will be empty.
	brokers := brokerMapFromTopicMap(partitionMapIn, brokerMetadata, Config.forceRebuild)

	// Update the currentBrokers list with
	// the provided broker list.
	bs := brokers.update(Config.brokers, brokerMetadata)
	change := bs.new - bs.replace

	// Print change summary.
	fmt.Printf("%sReplacing %d, added %d, missing %d, total count changed by %d\n",
		indent, bs.replace, bs.new, bs.missing+bs.oldMissing, change)

	// Print action.
	fmt.Printf("\nAction:\n")

	switch {
	case change >= 0 && bs.replace > 0:
		fmt.Printf("%sRebuild topic with %d broker(s) marked for removal\n",
			indent, bs.replace)
	case change > 0 && bs.replace == 0:
		fmt.Printf("%sExpanding/rebalancing topic with %d additional broker(s) (this is a no-op unless --force-rebuild is specified)\n",
			indent, bs.new)
	case change < 0:
		fmt.Printf("%sShrinking topic by %d broker(s)\n",
			indent, -change)
	case Config.replication == 0:
		fmt.Printf("%sno-op\n", indent)
	}

	// Store a copy of the
	// original map.
	originalMap := partitionMapIn.copy()

	// If the replication factor is changed,
	// the partition map input needs to have stub
	// brokers appended (for r factor increase) or
	// existing brokers removed (for r factor decrease).
	if Config.replication > 0 {
		fmt.Printf("%sUpdating replication factor to %d\n",
			indent, Config.replication)

		partitionMapIn.setReplication(Config.replication)
	}

	// Build a new map using the provided list of brokers.
	// This is ok to run even when a no-op is intended.

	var partitionMapOut *partitionMap
	var warns []string

	// If we're doing a force rebuild, the input map
	// must have all brokers stripped out.
	if Config.forceRebuild {
		partitionMapInStripped := partitionMapIn.strip()
		partitionMapOut, warns = partitionMapInStripped.rebuild(brokers)
	} else {
		partitionMapOut, warns = partitionMapIn.rebuild(brokers)
	}

	// Sort by topic, partition.
	sort.Sort(partitionMapIn.Partitions)
	sort.Sort(partitionMapOut.Partitions)

	// Count missing brokers as a warning.
	if bs.missing > 0 {
		w := fmt.Sprintf("%d provided brokers not found in ZooKeeper\n", bs.missing)
		warns = append(warns, w)
	}

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

	// TODO scan partition lists
	// and ensure they're the same
	// topic, partition.

	// Get a status string of what's changed.
	fmt.Println("\nPartition map changes:")
	for i := range originalMap.Partitions {
		change := whatChanged(originalMap.Partitions[i].Replicas,
			partitionMapOut.Partitions[i].Replicas)

		fmt.Printf("%s%s p%d: %v -> %v %s\n",
			indent,
			originalMap.Partitions[i].Topic,
			originalMap.Partitions[i].Partition,
			originalMap.Partitions[i].Replicas,
			partitionMapOut.Partitions[i].Replicas,
			change)
	}

	// Get a per-broker count of leader, follower
	// and total partition assignments.
	fmt.Println("\nPartitions assigned:")
	useStats := partitionMapOut.useStats()
	for id, use := range useStats {
		fmt.Printf("%sBroker %d - leader: %d, follower: %d, total: %d\n",
			indent, id, use.leader, use.follower, use.leader+use.follower)
	}

	// Don't write the output if ignoreWarns is set.
	if !Config.ignoreWarns && len(warns) > 0 {
		fmt.Printf(
			"%sWarnings encountered, partition map not created. Override with --ignore-warns.\n",
			indent)
		os.Exit(1)
	}

	// Map per topic.
	tm := map[string]*partitionMap{}
	for _, p := range partitionMapOut.Partitions {
		if tm[p.Topic] == nil {
			tm[p.Topic] = newPartitionMap()
		}
		tm[p.Topic].Partitions = append(tm[p.Topic].Partitions, p)
	}

	fmt.Println("\nNew partition maps:")
	// Global map if set.
	if Config.outFile != "" {
		err := writeMap(partitionMapOut, Config.outPath+Config.outFile)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json [combined map]\n", indent, Config.outPath, Config.outFile)
		}
	}

	for t := range tm {
		err := writeMap(tm[t], Config.outPath+t)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json\n", indent, Config.outPath, t)
		}
	}
}

// whatChanged takes a before and after broker
// replica set and returns a string describing
// what changed.
func whatChanged(s1 []int, s2 []int) string {
	var changes []string

	a, b := make([]int, len(s1)), make([]int, len(s2))
	copy(a, s1)
	copy(b, s2)

	var lchanged bool
	var echanged bool

	// Check if the len is different.
	switch {
	case len(a) > len(b):
		lchanged = true
		changes = append(changes, "decreased replication")
	case len(a) < len(b):
		lchanged = true
		changes = append(changes, "increased replication")
	}

	// If the len is the same,
	// check elements.
	if !lchanged {
		for i := range a {
			if a[i] != b[i] {
				echanged = true
			}
		}
	}

	// Nothing changed.
	if !lchanged && !echanged {
		return "no-op"
	}

	// Determine what else changed.

	// Get smaller replica set len between
	// old vs new, then cap both to this len for
	// comparison.
	slen := int(math.Min(float64(len(a)), float64(len(b))))

	a = a[:slen]
	b = b[:slen]

	echanged = false
	for i := range a {
		if a[i] != b[i] {
			echanged = true
		}
	}

	sort.Ints(a)
	sort.Ints(b)

	samePostSort := true
	for i := range a {
		if a[i] != b[i] {
			samePostSort = false
		}
	}

	// If the broker lists changed but
	// are the same after sorting,
	// we've just changed the preferred
	// leader.
	if echanged && samePostSort {
		changes = append(changes, "preferred leader")
	}

	// If the broker lists changed and
	// aren't the same after sorting, we've
	// replaced a broker.
	if echanged && !samePostSort {
		changes = append(changes, "replaced broker")
	}

	// Construct change string.
	var buf bytes.Buffer
	for i, c := range changes {
		buf.WriteString(c)
		if i < len(changes)-1 {
			buf.WriteString(", ")
		}
	}

	return buf.String()
}
