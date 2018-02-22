# Overview

Autothrottle is a service that looks for reassigning partition events (as part of a recovery or routine data movement) and dynamically applies broker replication throttles. This is done to run replications as fast as possible without starving out bandwidth from Kafka consumer clients.

It does this by looking up all topics undergoing replication, distinguishes source vs destination brokers, fetches metrics for each broker from the Datadog API, and calculates a throttle based on a map of known bandwidth limits specific to each instance type being observed. Rather than wrapping CLI tools, the throttle is applied directly in ZooKeeper by autothrottle, mirroring the internal mechanics of Kafka's tooling (applying broker configs and firing off sequential znode notices in a config path watched by all brokers). When no replication is occurring, autothrottle will clear all throttles across the cluster.

Additionally, autothrottle writes Datadog events at each check interval that detail what topics are undergoing replication, a list of all brokers involved, and throttle rates applied.

**Additional features**:
- Configurable portion of free headroom available for use by replication (`--max-rate`)
- Throttle rate change threshold to reduce propagating broker config updates (`--change-threshold`)
- User-supplied map of instance type and capacity values (`--cap-map`)
- Ability to dynamically set fixed replication rates (via the HTTP API)

# Usage

Besides basic configuration such as the cluster ZooKeeper address, Autothrottle requirements include:

- Datadog API and app key
- A metric string that returns the `system.net.bytes_sent` metric per host, scoped to the cluster that's being managed
- That each Kafka host is tagged with `instance-type` (included via the AWS integration) and a `broker_id` tag
- A map of instance types and available bandwidth, supplied as a json string via the `--cap-map` parameter

Once running, autothrottle should clearly log what it's doing:

```
15:13:08 Authrottle Running
15:13:09 Admin API: localhost:8080
15:13:09 Topics with ongoing reassignments: [test-topic]
15:13:09 Source brokers participating in replication: [1002 1003 1004 1005 1006 1007]
15:13:09 Destination brokers participating in replication: [1002 1003 1004]
15:13:10 Most utilized source broker: [1003] net tx of 60.25MB/s (over 60s) with an existing throttle rate of 0.00MB/s
15:13:10 Replication headroom (based on a 90% max free capacity utilization): 170.77MB/s
15:13:10 Updated throttle to 170.77MB/s on broker 1005
15:13:10 Updated throttle to 170.77MB/s on broker 1007
15:13:11 Updated throttle to 170.77MB/s on broker 1003
15:13:11 Updated throttle to 170.77MB/s on broker 1002
15:13:12 Updated throttle to 170.77MB/s on broker 1006
15:13:12 Updated throttle to 170.77MB/s on broker 1004
...
22:29:03 Topics done reassigning: [test_topic]
22:29:03 No topics undergoing reassignment
22:29:03 Removing throttle on broker 1004
22:29:03 Removing throttle on broker 1005
22:29:04 Removing throttle on broker 1003
```

Flags:

```
Usage of autothrottle:
  -api-key string
        Datadog API key [AUTOTHROTTLE_API_KEY]
  -api-listen string
        Admin API listen address:port [AUTOTHROTTLE_API_LISTEN] (default "localhost:8080")
  -app-key string
        Datadog app key [AUTOTHROTTLE_APP_KEY]
  -cap-map string
        JSON map of instance types to network capacity in MB/s [AUTOTHROTTLE_CAP_MAP]
  -change-threshold float
        Required change in percent of the replication throttle to apply an update [AUTOTHROTTLE_CHANGE_THRESHOLD] (default 10)
  -cleanup-after int
        Number of intervals after which to issue a global throttle unset if no replication is running [AUTOTHROTTLE_CLEANUP_AFTER] (default 180)
  -dd-event-tags string
        Comma-delimited list of Datadog event tags [AUTOTHROTTLE_DD_EVENT_TAGS]
  -interval int
        Autothrottle check interval in seconds [AUTOTHROTTLE_INTERVAL] (default 60)
  -max-rate float
        Maximum replication throttle rate as a percentage of available capacity [AUTOTHROTTLE_MAX_RATE] (default 90)
  -metrics-window int
        Time span of metrics to average [AUTOTHROTTLE_METRICS_WINDOW] (default 60)
  -min-rate float
        Minimum replication throttle rate in MB/s [AUTOTHROTTLE_MIN_RATE] (default 10)
  -net-tx-query string
        Datadog query for broker outbound bandwidth by host [AUTOTHROTTLE_NET_TX_QUERY] (default "avg:system.net.bytes_sent{service:kafka} by {host}")
  -zk-addr string
        ZooKeeper connect string (for broker metadata or rebuild-topic lookups) [AUTOTHROTTLE_ZK_ADDR] (default "localhost:2181")
  -zk-config-prefix string
        ZooKeeper prefix to store autothrottle configuration [AUTOTHROTTLE_ZK_CONFIG_PREFIX] (default "autothrottle")
  -zk-prefix string
        ZooKeeper namespace prefix [AUTOTHROTTLE_ZK_PREFIX]
```

Admin API:

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


# Design diagram

![img_1623](https://user-images.githubusercontent.com/4108044/35110764-d2dd19b0-fc36-11e7-8086-9038a194a3ac.JPG)

# Future improvements
- Multi-node / HA coordination through ZooKeeper
- Additional metrics (such as destination node flush latency)
- Tuning hints
