# Overview
Autothrottle is a service that looks for reassigning partition events (as part of a recovery or routine data movement) and dynamically applies broker replication throttles. This is done to run replications as fast as possible without starving out bandwidth from Kafka consumer clients.

It does this by running in a loop that looks up all topics undergoing replication, maps source vs destination broker involved, fetches metrics for each broker from the Datadog API, and calculates a throttle based on a map of known bandwidth limits specific to each instance type being observed. An updated throttle rate is determined at each loop interval and continuously applies the throttle to adapt to changes in workloads. Rather than wrapping CLI tools, the throttle is applied directly in ZooKeeper by autothrottle, mirroring the internals of Kafka's tooling. When no replication is occurring, autothrottle will clear all throttles across the cluster.

Additionally, autothrottle writes Datadog events at each check interval that detail what topics are undergoing replication, a list of all brokers involved, and throttle rates applied.

Autothrottle is designed to work as a piggyback system that doesn't take ownership of your cluster, and can easily be overridden (through autothrottle via the admin API) or stopped safely at any time (allowing you to revert back to using off-the-shelf Kafka tools).

**Additional features**:
- Configurable portion of free headroom available for use by replication (`--max-rate`)
- Throttle rate change threshold to reduce propagating broker config updates (`--change-threshold`)
- User-supplied map of instance type and capacity values (`--cap-map`)
- Ability to dynamically set fixed replication rates (via the HTTP API)

# Installation
- `go get github.com/DataDog/kafka-kit/cmd/autothrottle`

Binary will be found at `$GOPATH/bin/autothrottle`

**Compatibility**

Tested with Go 1.10+ (required), Kafka 0.10.x, ZooKeeper 3.4.x.

# Usage

Autothrottle prerequisites include:

- Datadog API and app key
- A metric string that returns the `system.net.bytes_sent` metric per host, scoped to the cluster that's being managed
- That each Kafka host is tagged with `instance-type` (included via the AWS integration) and a broker ID tag (configurable via `-broker-id-tag`, defaults to `broker_id`)
- A map of instance types and available bandwidth (in MB/s), supplied as a json string via the `--cap-map` parameter (e.g. `--cap-map '{"d2.2xlarge":120,"d2.4xlarge":240}'`)

Once running, autothrottle should clearly log what it's doing:

```
2018/03/16 18:27:11 Autothrottle Running
2018/03/16 18:27:12 Admin API: localhost:8080
2018/03/16 18:27:13 No topics undergoing reassignment
<...>
2018/03/16 18:31:21 Topics with ongoing reassignments: [test_topic]
2018/03/16 18:31:21 Source brokers participating in replication: [1002 1003 1004 1005 1006 1007]
2018/03/16 18:31:21 Destination brokers participating in replication: [1002 1003 1004 1005 1006 1007]
2018/03/16 18:31:21 Most utilized source broker: [1005] net tx of 116.43MB/s (over 60s) with an existing throttle rate of 0.00MB/s
2018/03/16 18:31:21 Replication capacity (based on a 90% max free capacity utilization): 120.21MB/s
2018/03/16 18:31:21 Updated throttle to 120.21MB/s on broker 1005
2018/03/16 18:31:22 Updated throttle to 120.21MB/s on broker 1004
2018/03/16 18:31:22 Updated throttle to 120.21MB/s on broker 1003
2018/03/16 18:31:23 Updated throttle to 120.21MB/s on broker 1006
2018/03/16 18:31:23 Updated throttle to 120.21MB/s on broker 1002
2018/03/16 18:31:24 Updated throttle to 120.21MB/s on broker 1007
2018/03/16 18:32:24 Topics done reassigning: [test_topic]
2018/03/16 18:32:24 No topics undergoing reassignment
2018/03/16 18:32:29 Throttle removed on broker 1003
2018/03/16 18:32:30 Throttle removed on broker 1004
2018/03/16 18:32:30 Throttle removed on broker 1002
2018/03/16 18:32:31 Throttle removed on broker 1007
2018/03/16 18:32:31 Throttle removed on broker 1005
2018/03/16 18:32:32 Throttle removed on broker 1006
2018/03/16 18:33:32 No topics undergoing reassignment
```

Overlaying autothrottle Datadog events on a recovery dashboard:

![img](https://user-images.githubusercontent.com/4108044/37539923-c9c39e06-291a-11e8-97cf-cd3d06416929.png)

## Flags

The variables in brackets are optional env var overrides.

```
Usage of autothrottle:
  -api-key string
    	Datadog API key [AUTOTHROTTLE_API_KEY]
  -api-listen string
    	Admin API listen address:port [AUTOTHROTTLE_API_LISTEN] (default "localhost:8080")
  -app-key string
    	Datadog app key [AUTOTHROTTLE_APP_KEY]
  -broker-id-tag string
    	Datadog host tag for broker ID [AUTOTHROTTLE_BROKER_ID_TAG] (default "broker_id")
  -cap-map string
    	JSON map of instance types to network capacity in MB/s [AUTOTHROTTLE_CAP_MAP]
  -change-threshold float
    	Required change in replication throttle to trigger an update (percent) [AUTOTHROTTLE_CHANGE_THRESHOLD] (default 10)
  -cleanup-after int
    	Number of intervals after which to issue a global throttle unset if no replication is running [AUTOTHROTTLE_CLEANUP_AFTER] (default 60)
  -dd-event-tags string
    	Comma-delimited list of Datadog event tags [AUTOTHROTTLE_DD_EVENT_TAGS]
  -failure-threshold int
    	Number of iterations that throttle determinations can fail before reverting to the min-rate [AUTOTHROTTLE_FAILURE_THRESHOLD] (default 1)
  -interval int
    	Autothrottle check interval (seconds) [AUTOTHROTTLE_INTERVAL] (default 180)
  -max-rate float
    	Maximum replication throttle rate (as a percentage of available capacity) [AUTOTHROTTLE_MAX_RATE] (default 90)
  -metrics-window int
    	Time span of metrics required (seconds) [AUTOTHROTTLE_METRICS_WINDOW] (default 120)
  -min-rate float
    	Minimum replication throttle rate (MB/s) [AUTOTHROTTLE_MIN_RATE] (default 10)
  -net-tx-query string
    	Datadog query for broker outbound bandwidth by host [AUTOTHROTTLE_NET_TX_QUERY] (default "avg:system.net.bytes_sent{service:kafka} by {host}")
  -version
    	version [AUTOTHROTTLE_VERSION]
  -zk-addr string
    	ZooKeeper connect string (for broker metadata or rebuild-topic lookups) [AUTOTHROTTLE_ZK_ADDR] (default "localhost:2181")
  -zk-config-prefix string
    	ZooKeeper prefix to store autothrottle configuration [AUTOTHROTTLE_ZK_CONFIG_PREFIX] (default "autothrottle")
  -zk-prefix string
    	ZooKeeper namespace prefix [AUTOTHROTTLE_ZK_PREFIX]
```

## Rate Calculations, Applying Throttles

The throttle rate is calculated by building a map of destination (brokers where partitions are being replicated to) and source brokers (brokers where partitions are being replicated from) and determining a suitable rate based on outbound network utilization on source brokers. The most saturated source broker is used to determine the throttle rate for all replicating brokers (this is done for simplicity as a per-path rate is more complex than it sounds). Autothrottle references the provided `-cap-map` to lookup the network capacity. Autothrottle compares the amount of ongoing network throughput against the capacity (subtracting any amount already allocated for replication) to determine headroom. If more headroom is available, the throttle will be raised to consume the `-max-rate` (defaults to 90%) percent of what's available. If it's negative (throughput exceeds the configured capacity), the throttle will be lowered.

Autothrottle fetches metrics and performs this check every `-interval` seconds. In order to reduce propagating updated throttles to brokers too aggressively, a new throttle won't be applied unless it deviates more than `-change-threshold` (defaults to 10%) percent from the previous throttle. Any time a throttle change is applied, topics are done replicating, or throttle rates cleared, autothrottle will write Datadog events tagged with `name:autothrottle` along with any additionally defined tags (via the `-dd-event-tags` param).

Autothrottle is also designed to fail-safe and avoid any unspecified decision modes. If fetching metrics fails or returns partial data, autothrottle will log what's missing and revert brokers to a safety throttle rate of `-min-rate` (defaults to 10MB/s). In order to prevent flapping, a configurable number of sequential failures before reverting to the minimum rate can be set with the `-failure-threshold` param (defaults to 1).

Some considerations:
- This works best with clusters using a single instance type.
- A single throttle rate that applies to an entire group of replicating brokers tends to work quite well, but per-path rates is planned as an eventual feature.

## Operations Notes

- Autothrottle currently assumes that exactly one instance is running per cluster. Multi-node / HA support is planned.
- Autothrottle is safe to arbitrarily restart. If restarted, the first iteration may temporarily lower an existing throttle since it doesn't have a known rate to use as a compensation value in calculating headroom.
- Autothrottle is safe to stop using at any time. All operations mimic existing internals/functionality of Kafka. Autothrottle intends to be a layer of metrics driven decision autonomy.
- It's easy to accidentally leave throttles applied when performing manual reassignments. Autothrottle automatically clears previously applied throttles when no replications are running, and does a global throttle clearing every `-cleanup-after` iterations.

## Admin API

The administrative API allows overrides to be set. If an override is set, Datadog metrics will not be fetched.

```
$ curl localhost:8080/get_throttle
No throttle is set

$ curl -XPOST "localhost:8080/set_throttle?rate=100"
Throttle successfully set to 100MB/s

$ curl localhost:8080/get_throttle
A throttle override is configured at 100MB/s

$ curl -XPOST localhost:8080/remove_throttle
Throttle successfully removed
```

# Diagrams

![img_1623](https://user-images.githubusercontent.com/4108044/35110764-d2dd19b0-fc36-11e7-8086-9038a194a3ac.JPG)
