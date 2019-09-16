# Overview

Metricsfetcher is a simple tool that fetches Kafka broker and partition metrics from the Datadog API and stores it in ZooKeeper. This data is used for the topicmappr [storage placement](https://github.com/DataDog/kafka-kit/tree/master/cmd/topicmappr#placement-strategy) strategy.

# Installation
- `go get github.com/DataDog/kafka-kit/cmd/metricsfetcher`

Binary will be found at `$GOPATH/bin/metricsfetcher`

**Compatibility**

Tested with Go 1.10+ (required), Kafka 0.10.x, ZooKeeper 3.4.x.

# Usage

Once configured, metricsfetcher can be ran anywhere that has accessibility to the Datadog API and the destination ZooKeeper cluster. Metrics for both broker storage and partition sizes are fetched and written to ZooKeeper.

```
$ metricsfetcher
Submitting max:kafka.log.partition.size{role:test-cluster} by {topic,partition}.rollup(avg, 3600)
success
Submitting avg:system.disk.free{role:test-cluster,device:/data} by {broker_id}.rollup(avg, 3600)
success

Data written to ZooKeeper
```

## Flags

The variables in brackets are optional env var overrides.

```
Usage of metricsfetcher:
  -api-key string
    	Datadog API key [METRICSFETCHER_API_KEY]
  -app-key string
    	Datadog app key [METRICSFETCHER_APP_KEY]
  -broker-id-tag string
    	Datadog host tag for broker ID [METRICSFETCHER_BROKER_ID_TAG] (default "broker_id")
  -broker-storage-query string
    	Datadog metric query to get broker storage free [METRICSFETCHER_BROKER_STORAGE_QUERY] (default "avg:system.disk.free{service:kafka,device:/data}")
  -compression
    	Whether to compress metrics data written to ZooKeeper [METRICSFETCHER_COMPRESSION] (default true)
  -dry-run
    	Dry run mode (don't reach Zookeeper) [METRICSFETCHER_DRY_RUN]
  -partition-size-query string
    	Datadog metric query to get partition size by topic, partition [METRICSFETCHER_PARTITION_SIZE_QUERY] (default "max:kafka.log.partition.size{service:kafka} by {topic,partition}")
  -span int
    	Query range in seconds (now - span) [METRICSFETCHER_SPAN] (default 3600)
  -verbose
    	Verbose output [METRICSFETCHER_VERBOSE]
  -version
    	version [METRICSFETCHER_VERSION]
  -zk-addr string
    	ZooKeeper connect string [METRICSFETCHER_ZK_ADDR] (default "localhost:2181")
  -zk-prefix string
    	ZooKeeper namespace prefix [METRICSFETCHER_ZK_PREFIX] (default "topicmappr")
```

`-broker-storage-query` should be scoped to your target Kafka cluster and storage device that Kafka partition data is stored on. Brokers should be tagged in Datadog with their broker IDs using  `broker_id` tag. No aggregations should be specified.

`-partition-size-query` should be scoped to the same target Kafka cluster. No aggregations should be specified. If only a single topic is being used, the metric query can be simplified to reduce the amount of data to be fetched/stored. Example (note the addition of the `topic` query tag): `-partition-size-query="max:kafka.log.partition.size{service:kafka,topic:my_topic} by {topic,partition}"`.

Another detail to note regarding the partition size query is that `max` is being specified. This uses the largest observed size across all replicas for a given partition. This value is used as a safety precaution when placing partitions, even if a particular replica is actually smaller than this value. The assumption is that replicas with values well below the max may have been recently replicated and have not reached full retention. A peculiar drawback is that the storage change estimations in topicmappr may actually show a broker being decommissioned with an estimated target free space greater than its actual total capacity. This scenario can be encountered where a broker originally held a partition replica where the replica size was well below the observed maximum. When the storage change estimations are being calculated, the `max` value among all replicas for the each partition is used, thus resulting in a high free storage estimation (since more storage was added back than was actually consumed). It was decided that the query volume and internal complexity of actually mapping per-replica partition sizes to broker IDs to correct accounting in these edge cases was not worth it since the data would be purely used for the information output and not the placement logic.

`-span` specifies a duration in seconds that metric queries cover. All points in the series are rolled up as a single average value. This is automatically combined with the above flags to create complete rollup queries.

`-zk-prefix` specifies a namespace that the metrics data is stored. This should correspond with the topicmappr `-zk-metrics-prefix` parameter.

# Data Structures

The topicmappr rebalance sub-command or the rebuild sub-command with the storage placement strategy expects metrics in the following znodes under the parent `-zk-prefix` path (both metricsfetcher and topicmappr default to `topicmappr`), along with the described structure:

### /topicmappr/partitionmeta
`{"<topic name>": {"<partition number>": {"Size": <bytes>}}}`

Example:
```
[zk: localhost:2181(CONNECTED) 1] get /topicmappr/partitionmeta
{"topic1":{"0": {"Size":10},"1":{"Size": 12}},"topic2":{"0": {"Size":15},"1":{"Size": 13}}}
```

### /topicmappr/brokermetrics
`{"<broker ID>": {"StorageFree": <bytes>}}`

Example:
```
[zk: localhost:2181(CONNECTED) 0] get /topicmappr/brokermetrics
{"1002":{"StorageFree":1280803388090.7295},"1003":{"StorageFree":1104897156296.092},"1004":{"StorageFree":1161254545714.023},"1005":{"StorageFree":1196051803924.5977},"1006":{"StorageFree":1103418346402.9092},"1007":{"StorageFree":1299083586345.6743}}
```

The znode data can be optionally compressed with gzip (metricsfetcher will do this by default, configurable with the `--compression` flag) in the case of a high number of partitions where the znode data size may exceed the configured limit. Topicmappr transparently supports reading gzip compressed metrics data.
