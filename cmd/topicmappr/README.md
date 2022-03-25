# Overview
Topicmappr replaces and extends the `kafka-reassign-partition` tool bundled with Kafka. Notable features:

**Cluster Storage Rebalancing, Partition Bin-packing**

Topicmappr can be used to balance storage utilization among brokers by positioning partitions based on size and broker storage capacity. This can be used to relocate data from the most to least utilized brokers, scale up clusters with redistribution of partitions from existing brokers to new brokers, and from-scratch optimal placement using a first-fit descending bin-packing algorithm.

Configurable storage bounds combined with some automated optimal parameter discovery ensures that the best possible storage placement is computed.

**Constraints Satisfaction Partition Placement**

Topicmappr honors Kafka's rack awareness configurations and enforces limits on how many replicas can be placed in the same zone (rack) while aiming to maximize leadership distribution, zone dispersion, and total replica distribution among brokers.

**Minimal Partition Movement**

Avoids reassigning partitions where movement isn't necessary, greatly reducing reassignment times and resource load for simple recoveries.

**Safer Operations**

Topicmappr minimizes unsafe replica placement, clearly informs users of what changes will be made or why a change isn't possible, and prevents storage placement decisions that would result in worse utilization.

**Leadership Optimization**

Leadership can be evenly distributed among brokers, optionally without even moving data.

**Deterministic Output**

A given input always produces the same output.

**Informative Operational Output**

An output of what's intended to change along with advisory notices (e.g. insufficient broker counts supplied to satisfy all constraints at the desired partition/replica count) that helps users make informed decisions decisions.

Additional statistical output is included where available. For instance, broker-to-broker relationships are represented as node degree counts (where an edge between nodes is defined as occupying the same replica set). These values can be used as a probabilistic indicator of replication bandwidth; replacing a broker with more edges will likely replicate from more source brokers than one with fewer edges, minimizing recovery time and replication source impact.

# Installation
- `go get github.com/DataDog/kafka-kit/cmd/topicmappr`

Binary will be found at `$GOPATH/bin/topicmappr`

**Compatibility**

Tested with Kafka 0.10, 2.2-2.7, ZooKeeper 3.4, 3.5

# Usage

## Commands

Most operations are performed through the `rebuild` command. Partial rebalances are performed through a dedicated `rebalance` command (beta).

```
Usage:
  topicmappr [command]

Available Commands:
  help        Help about any command
  rebalance   Rebalance partition allotments among a set of topics and brokers
  rebuild     Rebuild a partition map for one or more topics
  scale       Redistribute partitions to additional brokers
  version     Print the version

Flags:
  -h, --help               help for topicmappr
      --ignore-warns       Produce a map even if warnings are encountered [TOPICMAPPR_IGNORE_WARNS]
      --zk-addr string     ZooKeeper connect string [TOPICMAPPR_ZK_ADDR] (default "localhost:2181")
      --zk-prefix string   ZooKeeper prefix (if Kafka is configured with a chroot path prefix) [TOPICMAPPR_ZK_PREFIX]

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
      --brokers string                Broker list to scope all partition placements to ('-1' for all currently mapped brokers, '-2' for all brokers in cluster)
      --chunk-step-size int           Number of brokers to move data at a time for with a chunked operation. (default 0)
      --force-rebuild                 Forces a complete map rebuild
  -h, --help                          help for rebuild
      --leader-evac-brokers string    Broker list to remove leadership for topics in leader-evac-topics.
      --leader-evac-topics string     Topics list to remove leadership for the brokers given in leader-evac-brokers
      --map-string string             Rebuild a partition map provided as a string literal
      --metrics-age int               Kafka metrics age tolerance (in minutes) (when using storage placement) (default 60)
      --min-rack-ids int              Minimum number of required of unique rack IDs per replica set (0 requires that all are unique)
      --optimize string               Optimization priority for the storage placement strategy: [distribution, storage] (default "distribution")
      --optimize-leadership           Rebalance all broker leader/follower ratios
      --out-file string               If defined, write a combined map of all topics to a file
      --out-path string               Path to write output map files to
      --partition-size-factor float   Factor by which to multiply partition sizes when using storage placement (default 1)
      --phased-reassignment           Create two-phase output maps
      --placement string              Partition placement strategy: [count, storage] (default "count")
      --replication int               Normalize the topic replication factor across all replica sets (0 results in a no-op)
      --skip-no-ops                   Skip no-op partition assigments
      --sub-affinity                  Replacement broker substitution affinity
      --topics string                 Rebuild topics (comma delim. list) by lookup in ZooKeeper
      --topics-exclude string         Exclude topics
      --use-meta                      Use broker metadata in placement constraints (default true)

Global Flags:
      --ignore-warns               Produce a map even if warnings are encountered [TOPICMAPPR_IGNORE_WARNS]
      --zk-addr string             ZooKeeper connect string [TOPICMAPPR_ZK_ADDR] (default "localhost:2181")
      --zk-metrics-prefix string   ZooKeeper namespace prefix for Kafka metrics [TOPICMAPPR_ZK_METRICS_PREFIX] (default "topicmappr")
      --zk-prefix string           ZooKeeper prefix (if Kafka is configured with a chroot path prefix) [TOPICMAPPR_ZK_PREFIX]

```

## rebalance usage

```
Rebalance partition allotments among a set of topics and brokers

Usage:
  topicmappr rebalance [flags]

Flags:
      --brokers string                 Broker list to scope all partition placements to ('-1' for all currently mapped brokers, '-2' for all brokers in cluster)
  -h, --help                           help for rebalance
      --locality-scoped                Ensure that all partition movements are scoped by rack.id
      --metrics-age int                Kafka metrics age tolerance (in minutes) (default 60)
      --optimize-leadership            Rebalance all broker leader/follower ratios
      --out-file string                If defined, write a combined map of all topics to a file
      --out-path string                Path to write output map files to
      --partition-limit int            Limit the number of top partitions by size eligible for relocation per broker (default 30)
      --partition-size-threshold int   Size in megabytes where partitions below this value will not be moved in a rebalance (default 512)
      --storage-threshold float        Percent below the harmonic mean storage free to target for partition offload (0 targets a brokers) (default 0.2)
      --storage-threshold-gb float     Storage free in gigabytes to target for partition offload (those below the specified value); 0 [default] defers target selection to --storage-threshold
      --tolerance float                Percent distance from the mean storage free to limit storage scheduling (0 performs automatic tolerance selection)
      --topics string                  Rebuild topics (comma delim. list) by lookup in ZooKeeper
      --topics-exclude string          Exclude topics
      --verbose                        Verbose output
      --zk-metrics-prefix string       ZooKeeper namespace prefix for Kafka metrics (default "topicmappr")

Global Flags:
      --ignore-warns       Produce a map even if warnings are encountered [TOPICMAPPR_IGNORE_WARNS]
      --zk-addr string     ZooKeeper connect string [TOPICMAPPR_ZK_ADDR] (default "localhost:2181")
      --zk-prefix string   ZooKeeper prefix (if Kafka is configured with a chroot path prefix) [TOPICMAPPR_ZK_PREFIX]
```

## scale usage

```
Redistribute partitions to additional brokers

Usage:
  topicmappr scale [flags]

Flags:
      --brokers string                 Broker list to scope all partition placements to ('-1' for all currently mapped brokers, '-2' for all brokers in cluster)
  -h, --help                           help for scale
      --locality-scoped                Ensure that all partition movements are scoped by rack.id
      --metrics-age int                Kafka metrics age tolerance (in minutes) (default 60)
      --optimize-leadership            Scale all broker leader/follower ratios
      --out-file string                If defined, write a combined map of all topics to a file
      --out-path string                Path to write output map files to
      --partition-limit int            Limit the number of top partitions by size eligible for relocation per broker (default 30)
      --partition-size-threshold int   Size in megabytes where partitions below this value will not be moved in a scale (default 512)
      --tolerance float                Percent distance from the mean storage free to limit storage scheduling (0 performs automatic tolerance selection)
      --topics string                  Rebuild topics (comma delim. list) by lookup in ZooKeeper
      --topics-exclude string          Exclude topics
      --verbose                        Verbose output
      --zk-metrics-prefix string       ZooKeeper namespace prefix for Kafka metrics (default "topicmappr")

Global Flags:
      --ignore-warns       Produce a map even if warnings are encountered [TOPICMAPPR_IGNORE_WARNS]
      --zk-addr string     ZooKeeper connect string [TOPICMAPPR_ZK_ADDR] (default "localhost:2181")
      --zk-prefix string   ZooKeeper prefix (if Kafka is configured with a chroot path prefix) [TOPICMAPPR_ZK_PREFIX]
```

## Managing and Repairing Topics

See the wiki [Usage Guide](https://github.com/DataDog/kafka-kit/wiki/Topicmappr-Usage-Guide) section for examples of common topic management tasks.
