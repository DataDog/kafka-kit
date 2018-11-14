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
- `go get -u github.com/DataDog/kafka-kit/...`

Binary will be found at `$GOPATH/bin/topicmappr`

**Compatibility**

Tested with Go 1.10+ (required), Kafka 0.10.x, ZooKeeper 3.4.x.

# Usage

## Commands

Most operations are performed through the `rebuild` command. Partial rebalances are performed through a dedicated `rebalance` command (beta).

```
Usage:
  topicmappr [command]

Available Commands:
  help        Help about any command
  rebalance   [BETA] Rebalance partition allotments among a set of topics and brokers
  rebuild     Rebuild a partition map for one or more topics

Flags:
  -h, --help               help for topicmappr
      --zk-addr string     ZooKeeper connect string (for broker metadata or rebuild-topic lookups) (default "localhost:2181") [TOPICMAPPR_ZK_ADDR]
      --zk-prefix string   ZooKeeper namespace prefix (for Kafka brokers) [TOPICMAPPR_ZK_PREFIX]

Use "topicmappr [command] --help" for more information about a command.
```



## rebuild usage

```
rebuild requires at least two inputs: a reference of
target topics and a list of broker IDs to which those topics should be mapped.
Target topics are provided as a comma delimited list of topic names and/or regex patterns
via the --topics parameter, which discovers matching topics in ZooKeeper (additionally,
the --zk-addr and --zk-prefix global flags should be set). Alternatively, a JSON map can be
provided via the --map-string flag. Target broker IDs are provided via the --broker flag.

Usage:
  topicmappr rebuild [flags]

Flags:
      --brokers string                Broker list to scope all partition placements to
      --force-rebuild                 Forces a complete map rebuild
  -h, --help                          help for rebuild
      --ignore-warns                  Produce a map even if warnings are encountered
      --map-string string             Rebuild a partition map provided as a string literal
      --optimize string               Optimization priority for the storage placement strategy: [distribution, storage] (default "distribution")
      --out-file string               If defined, write a combined map of all topics to a file
      --out-path string               Path to write output map files to
      --partition-size-factor float   Factor by which to multiply partition sizes when using storage placement (default 1)
      --placement string              Partition placement strategy: [count, storage] (default "count")
      --replication int               Normalize the topic replication factor across all replica sets (0 results in a no-op)
      --sub-affinity                  Replacement broker substitution affinity
      --topics string                 Rebuild topics (comma delim. list) by lookup in ZooKeeper
      --use-meta                      Use broker metadata in placement constraints (default true)
      --zk-metrics-prefix string      ZooKeeper namespace prefix for Kafka metrics (when using storage placement) (default "topicmappr")

Global Flags:
      --zk-addr string     ZooKeeper connect string (for broker metadata or rebuild-topic lookups) (default "localhost:2181") [TOPICMAPPR_ZK_ADDR]
      --zk-prefix string   ZooKeeper namespace prefix (for Kafka brokers) [TOPICMAPPR_ZK_PREFIX]
```

## rebalance usage

```
[BETA] Rebalance partition allotments among a set of topics and brokers

Usage:
  topicmappr rebalance [flags]

Flags:
      --brokers string             Broker list to scope all partition placements to
  -h, --help                       help for rebalance
      --locality-scoped            Disallow a relocation to traverse rack.id values among brokers (default true)
      --out-file string            If defined, write a combined map of all topics to a file
      --out-path string            Path to write output map files to
      --partition-limit int        Limit the number of top partitions by size eligible for relocation per broker (default 30)
      --storage-threshold float    Percent below the harmonic mean storage free to target for partition offload (default 0.2)
      --tolerance float            Percent distance from the mean storage free to limit storage scheduling (0 targets a brokers) (default 0.1)
      --topics string              Rebuild topics (comma delim. list) by lookup in ZooKeeper
      --verbose                    Verbose output
      --zk-metrics-prefix string   ZooKeeper namespace prefix for Kafka metrics (when using storage placement) (default "topicmappr")

Global Flags:
      --zk-addr string     ZooKeeper connect string (for broker metadata or rebuild-topic lookups) (default "localhost:2181") [TOPICMAPPR_ZK_ADDR]
      --zk-prefix string   ZooKeeper namespace prefix (for Kafka brokers) [TOPICMAPPR_ZK_PREFIX]
```

## Managing and Repairing Topics

See the wiki [Usage Guide](https://github.com/DataDog/kafka-kit/wiki/Usage-Guide) section for examples of common topic management tasks.
