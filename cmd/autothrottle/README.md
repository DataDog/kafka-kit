# Overview

Autothrottle is a service that looks for reassigning partition events (as part of a recovery or routine data movement) and dynamically applies broker replication throttles. This is done to run replications as fast as possible without starving out bandwidth from Kafka consumer clients.

It does this by running in a loop that looks up all topics undergoing replication, maps source vs destination broker involved, fetches metrics for each broker from the Datadog API, and calculates a throttle based on a map of known bandwidth limits specific to each instance type being observed. An updated throttle rate is determined at each loop interval and continuously applies the throttle to adapt to changes in workloads. Rather than wrapping CLI tools, the throttle is applied directly in ZooKeeper by autothrottle, mirroring the internal mechanics of Kafka's tooling (applying broker configs and firing off sequential znode notices in a config path watched by all brokers). When no replication is occurring, autothrottle will clear all throttles across the cluster.

Additionally, autothrottle writes Datadog events at each check interval that detail what topics are undergoing replication, a list of all brokers involved, and throttle rates applied.

Autothrottle is designed to work as a piggyback system that doesn't take ownership of your cluster, and can easily be overridden (through autothrottle via the admin API) or stopped safely at any time (allowing you to revert back to using of off-the-shelf Kafka tools).

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
    	Required change in replication throttle to trigger an update (percent) [AUTOTHROTTLE_CHANGE_THRESHOLD] (default 10)
  -cleanup-after int
    	Number of intervals after which to issue a global throttle unset if no replication is running [AUTOTHROTTLE_CLEANUP_AFTER] (default 180)
  -dd-event-tags string
    	Comma-delimited list of Datadog event tags [AUTOTHROTTLE_DD_EVENT_TAGS]
  -interval int
    	Autothrottle check interval (seconds) [AUTOTHROTTLE_INTERVAL] (default 60)
  -max-rate float
    	Maximum replication throttle rate (as a percentage of available capacity) [AUTOTHROTTLE_MAX_RATE] (default 90)
  -metrics-window int
    	Time span of metrics required (seconds) [AUTOTHROTTLE_METRICS_WINDOW] (default 60)
  -min-rate float
    	Minimum replication throttle rate (MB/s) [AUTOTHROTTLE_MIN_RATE] (default 10)
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
