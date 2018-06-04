# Overview
Topicmappr was created as a replacement for Kafka's provided `kafka-reassign-partition.sh` tool, providing additional enhancements:

**Deterministic Output**

Given the same input, topicmappr should always provide the same output map.

**Minimal Partition Movement**

Avoids reassigning partitions where movement isn't necessary, greatly reducing reassignment times and resource load for simple recoveries.

**Balancing Partition Placement With Constraints**

For each partition placement, topicmappr chooses the least-utilized candidate broker (based on partition counts or storage available, configurable via the `-placement` param) that satisfies the following constraints:

- the broker isn't already in the replica set
- the broker isn't in any of the existing replica set localities (using the Kafka `rack-id` parameter)

Provided enough brokers, topicmapper determines the appropriate leadership, follower and failure domain balance.

**Change Summaries**

An output of what's changed along with advisory notices (e.g. insufficient broker counts supplied to satisfy all constraints at the desired partition/replica count) that helps users make clear decisions.

Additional statistical output is included where available. For instance, broker-to-broker relationships are represented as node degree counts (where edges are defined as brokers that belong in a common replica set for any given partition). These values can be used as a probabilistic indicator of replication bandwidth; replacing a broker with more edges will likely replicate from more source brokers than one with fewer edges.

# Installation
- `go get github.com/DataDog/topicmappr`
- `go install github.com/DataDog/topicmappr/cmd/topicmappr`

Binary will be found at `$GOPATH/bin/topicmappr`

**Compatibility**

Tested with Go 1.10 (required), Kafka 0.1.x, ZooKeeper 3.4.x.

# Usage

## Flags

```
Usage of topicmappr:
  -brokers string
    	Broker list to rebuild topic partition map with [TOPICMAPPR_BROKERS]
  -force-rebuild
    	Forces a rebuild even if all existing brokers are provided [TOPICMAPPR_FORCE_REBUILD]
  -ignore-warns
    	Whether a map should be produced if warnings are emitted [TOPICMAPPR_IGNORE_WARNS]
  -optimize string
    	Optimization priority for storage placement: [distribution, storage] [TOPICMAPPR_OPTIMIZE] (default "distribution")
  -out-file string
    	If defined, write a combined map of all topics to a file [TOPICMAPPR_OUT_FILE]
  -out-path string
    	Path to write output map files to [TOPICMAPPR_OUT_PATH]
  -placement string
    	Partition placement type: [count, storage] [TOPICMAPPR_PLACEMENT] (default "count")
  -rebuild-map string
    	Rebuild a topic map [TOPICMAPPR_REBUILD_MAP]
  -rebuild-topics string
    	Rebuild topics (comma delim list) by lookup in ZooKeeper [TOPICMAPPR_REBUILD_TOPICS]
  -replication int
    	Set the replication factor [TOPICMAPPR_REPLICATION]
  -sub-affinity
    	Replacement broker substitution affinity [TOPICMAPPR_SUB_AFFINITY]
  -use-meta
    	Use broker metadata as constraints [TOPICMAPPR_USE_META] (default true)
  -verbose
    	Verbose information [TOPICMAPPR_VERBOSE]
  -zk-addr string
    	ZooKeeper connect string (for broker metadata or rebuild-topic lookups) [TOPICMAPPR_ZK_ADDR] (default "localhost:2181")
  -zk-metrics-prefix string
    	ZooKeeper namespace prefix (for Kafka metrics) [TOPICMAPPR_ZK_METRICS_PREFIX] (default "topicmappr")
  -zk-prefix string
    	ZooKeeper namespace prefix (for Kafka) [TOPICMAPPR_ZK_PREFIX]
```

## Managing and Repairing Topics

See the wiki [Usage Guide](https://github.com/DataDog/topicmappr/wiki/Usage-Guide) section for examples of common topic management tasks.
