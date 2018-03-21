package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/DataDog/topicmappr/kafkazk"
)

const (
	indent = "  "
)

var (
	// Characters allowed in Kafka topic names
	topicNormalChar, _ = regexp.Compile(`[a-zA-Z0-9_\\-]`)

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
		placement     string
	}
)

func init() {
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
	flag.StringVar(&Config.placement, "placement", "count", "Partition placement type: [count, storage]")
	brokers := flag.String("brokers", "", "Broker list to rebuild topic partition map with")

	flag.Parse()

	// Sanity check params.
	switch {
	case Config.rebuildMap == "" && *topics == "":
		fmt.Println("Must specify either -rebuild-map or -rebuild-topics")
		defaultsAndExit()
	case len(*brokers) == 0:
		fmt.Println("--brokers cannot be empty")
		defaultsAndExit()
	case Config.placement != "count" && Config.placement != "storage":
		fmt.Println("--placement must be either 'count' or 'storage'")
		defaultsAndExit()
	case Config.useMeta && Config.placement == "storage":
		fmt.Println("--placement=storage requires that --use-meta canot be false")
		defaultsAndExit()
	}

	// Append trailing slash if not included.
	if Config.outPath != "" && !strings.HasSuffix(Config.outPath, "/") {
		Config.outPath = Config.outPath + "/"
	}

	Config.brokers = kafkazk.BrokerStringToSlice(*brokers)
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

func main() {
	// ZooKeeper init.
	var zk kafkazk.Handler
	if Config.useMeta || len(Config.rebuildTopics) > 0 || Config.placement == "storage" {
		var err error
		zk, err = kafkazk.NewHandler(&kafkazk.Config{
			Connect: Config.zkAddr,
			Prefix:  Config.zkPrefix,
		})
		if err != nil {
			fmt.Printf("Error connecting to ZooKeeper: %s\n", err)
			os.Exit(1)
		}

		defer zk.Close()
	}

	// General flow:
	// 1) PartitionMap formed from topic data (provided or via zk).
	// A BrokerMap is built from brokers found in the input
	// PartitionMap + any new brokers provided from the
	// --brokers param.
	// 2) New PartitionMap from the origial map is rebuilt with
	// the BrokerMap; marked brokers are removed and newly
	// provided brokers are swapped in where possible.
	// 3) New map is possibly expanded/rebalanced.
	// 4) Final map output.

	// Fetch broker metadata.
	var brokerMetadata kafkazk.BrokerMetaMap
	if Config.useMeta {
		var err error

		// Whether or not we want to include
		// additional broker metrics metadata.
		var withMetrics bool
		if Config.placement == "storage" {
			withMetrics = true
		}

		brokerMetadata, err = zk.GetAllBrokerMeta(withMetrics)
		if err != nil {
			fmt.Printf("Error fetching broker metadata: %s\n", err)
			os.Exit(1)
		}
	}

	// Fetch partition metadata.
	var partitionMeta kafkazk.PartitionMetaMap
	if Config.placement == "storage" {
		partitionMeta = kafkazk.NewPartitionMetaMap()
	}

	// Build a topic map with either
	// text input or by fetching the
	// map data from ZooKeeper.
	partitionMapIn := kafkazk.NewPartitionMap()
	switch {
	// Provided as text.
	case Config.rebuildMap != "":
		pm, err := kafkazk.PartitionMapFromString(Config.rebuildMap)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		partitionMapIn = pm
	// Fetch from ZK.
	case len(Config.rebuildTopics) > 0:
		pm, err := kafkazk.PartitionMapFromZK(Config.rebuildTopics, zk)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		partitionMapIn = pm
	}

	// Order from ZooKeeper can be
	// random.
	sort.Sort(partitionMapIn.Partitions)

	// Store a copy of the
	// original map.
	originalMap := partitionMapIn.Copy()

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
	brokers := kafkazk.BrokerMapFromTopicMap(partitionMapIn, brokerMetadata, Config.forceRebuild)

	// Update the currentBrokers list with
	// the provided broker list.
	bs := brokers.Update(Config.brokers, brokerMetadata)
	change := bs.New - bs.Replace

	// Print change summary.
	fmt.Printf("%sReplacing %d, added %d, missing %d, total count changed by %d\n",
		indent, bs.Replace, bs.New, bs.Missing+bs.OldMissing, change)

	// Print action.
	fmt.Printf("\nAction:\n")

	switch {
	case change >= 0 && bs.Replace > 0:
		fmt.Printf("%sRebuild topic with %d broker(s) marked for removal\n",
			indent, bs.Replace)
	case change > 0 && bs.Replace == 0:
		fmt.Printf("%sExpanding/rebalancing topic with %d additional broker(s) (this is a no-op unless --force-rebuild is specified)\n",
			indent, bs.New)
	case change < 0:
		fmt.Printf("%sShrinking topic by %d broker(s)\n",
			indent, -change)
	case Config.replication == 0:
		fmt.Printf("%sno-op\n", indent)
	}

	// If the replication factor is changed,
	// the partition map input needs to have stub
	// brokers appended (for r factor increase) or
	// existing brokers removed (for r factor decrease).
	if Config.replication > 0 {
		fmt.Printf("%sUpdating replication factor to %d\n",
			indent, Config.replication)

		partitionMapIn.SetReplication(Config.replication)
	}

	// Build a new map using the provided list of brokers.
	// This is ok to run even when a no-op is intended.

	var partitionMapOut *kafkazk.PartitionMap
	var warns []string

	// If we're doing a force rebuild, the input map
	// must have all brokers stripped out.
	if Config.forceRebuild {
		partitionMapInStripped := partitionMapIn.Strip()
		partitionMapOut, warns = partitionMapInStripped.Rebuild(brokers, partitionMeta, Config.placement)
	} else {
		partitionMapOut, warns = partitionMapIn.Rebuild(brokers, partitionMeta, Config.placement)
	}

	// XXX If we use the storage placement strategy,
	// we can call an optimize pass in a separate
	// stage here. Rebuild is complex enough;
	// introducing single-pass optimization there
	// might not be the best. It's likely OK that
	// rebuild handles replacements and initial
	// storage based placement while honoring the
	// locality / replication constraints.
	// A dedicated optimization function can focus
	// on optimizations within a given locality.
	// For example, we can arbitrarily shuffle all
	// partitions within a rack.id value, then perform
	// an optimization for each value.

	// Sort by topic, partition.
	// XXX Partitions should now be sorted
	// at their origins. Confirm this.
	sort.Sort(partitionMapIn.Partitions)
	sort.Sort(partitionMapOut.Partitions)

	// Count missing brokers as a warning.
	if bs.Missing > 0 {
		w := fmt.Sprintf("%d provided brokers not found in ZooKeeper\n", bs.Missing)
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

	// XXX scan partition lists
	// and ensure they're the same
	// topic, partition.

	// Get a status string of what's changed.
	fmt.Println("\nPartition map changes:")
	for i := range originalMap.Partitions {
		change := kafkazk.WhatChanged(originalMap.Partitions[i].Replicas,
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
	UseStats := partitionMapOut.UseStats()
	for id, use := range UseStats {
		fmt.Printf("%sBroker %d - leader: %d, follower: %d, total: %d\n",
			indent, id, use.Leader, use.Follower, use.Leader+use.Follower)
	}

	// Don't write the output if ignoreWarns is set.
	if !Config.ignoreWarns && len(warns) > 0 {
		fmt.Printf(
			"%sWarnings encountered, partition map not created. Override with --ignore-warns.\n",
			indent)
		os.Exit(1)
	}

	// Map per topic.
	tm := map[string]*kafkazk.PartitionMap{}
	for _, p := range partitionMapOut.Partitions {
		if tm[p.Topic] == nil {
			tm[p.Topic] = kafkazk.NewPartitionMap()
		}
		tm[p.Topic].Partitions = append(tm[p.Topic].Partitions, p)
	}

	fmt.Println("\nNew partition maps:")
	// Global map if set.
	if Config.outFile != "" {
		err := kafkazk.WriteMap(partitionMapOut, Config.outPath+Config.outFile)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json [combined map]\n", indent, Config.outPath, Config.outFile)
		}
	}

	for t := range tm {
		err := kafkazk.WriteMap(tm[t], Config.outPath+t)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json\n", indent, Config.outPath, t)
		}
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
