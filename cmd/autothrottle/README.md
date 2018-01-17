# Overview

Autothrottle is a service that looks for reassigning partition events (as part of a recovery or routine data movement) and dynamically applies broker replication throttles. This is done to run replications as fast as possible without starving out bandwidth from clients.

It does this by looking up all topics undergoing replication, distinguishes source vs destination brokers, fetches metrics for each broker from the Datadog API, and calculates a throttle based on a map of known bandwidth limits specific to each instance type being observed. Rather than wrapping CLI tools, the throttle is applied directly in ZooKeeper by autothrottle, mirroring the internal mechanics of Kafka's tooling (applying broker configs and firing off sequential znode notices in a config path watched by all brokers). Autothrottle additionally provides an HTTP admin API that allows manual throttle overrides to be set.

When no replication is occurring, autothrottle will clear all throttles across the cluster (Kafka does not do this on its own and the default tooling is somewhat clumsy at handling this).

Additionally, autothrottle writes Datadog events at each check interval that detail what topics are undergoing replication, a list of all brokers involved, and throttle rates applied.

# Usage

Besides basic configuration such as the cluster ZooKeeper address, Autothrottle requirements include:

- Datadog API and app key
- A metric string that returns the `system.net.bytes_sent` metric per host, scoped to the cluster that's being managed
- That each Kafka host is tagged with `instance-type` (included via the AWS integration) and a `broker_id` tag

Once running, autothrottle should clearly log what it's doing:

```
2018/01/17 22:28:13 Authrottle Running
2018/01/17 22:28:13 Admin API: localhost:8080
2018/01/17 22:28:13 No topics undergoing reassignment
2018/01/17 22:28:28 Topics with ongoing reassignments: [test_topic]
2018/01/17 22:28:29 Leaders participating in replication: [1010 1008 1009]
2018/01/17 22:28:29 Followers participating in replication: [1009 1010 1008]
2018/01/17 22:28:29 Broker 1010 has the highest outbound network throughput of 0.07MB/s
2018/01/17 22:28:29 Replication headroom: 249.93MB/s
2018/01/17 22:28:29 Setting throttle 249.93MB/s on broker 1010
2018/01/17 22:28:29 Setting throttle 249.93MB/s on broker 1008
2018/01/17 22:28:30 Setting throttle 249.93MB/s on broker 1009
...
2018/01/17 22:29:03 Topics done reassigning: [test_topic]
2018/01/17 22:29:03 No topics undergoing reassignment
2018/01/17 22:29:03 Removing throttle on broker 1009
2018/01/17 22:29:03 Removing throttle on broker 1010
2018/01/17 22:29:04 Removing throttle on broker 1008
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
  -dd-event-tags string
    	Comma-delimited list of Datadog event tags [AUTOTHROTTLE_DD_EVENT_TAGS]
  -interval int
    	Autothrottle check interval in seconds [AUTOTHROTTLE_INTERVAL] (default 60)
  -metrics-window int
    	Time span of metrics to average [AUTOTHROTTLE_METRICS_WINDOW] (default 300)
  -net-tx-query string
    	Network query for broker outbound bandwidth by host [AUTOTHROTTLE_NET_TX_QUERY] (default "avg:system.net.bytes_sent{service:kafka} by {host}")
  -zk-addr string
    	ZooKeeper connect string (for broker metadata or rebuild-topic lookups) [AUTOTHROTTLE_ZK_ADDR] (default "localhost:2181")
  -zk-config-prefix string
    	ZooKeeper prefix to store autothrottle configuration [AUTOTHROTTLE_ZK_CONFIG_PREFIX] (default "autothrottle")
  -zk-prefix string
    	ZooKeeper namespace prefix [AUTOTHROTTLE_ZK_PREFIX]
```

# Design diagram

![img_1620](https://user-images.githubusercontent.com/4108044/35069852-a8476a00-fb98-11e7-8e18-433fa78813a9.jpg)

# Future improvements
- Multi-node / HA coordination through ZooKeeper
- Additional metrics (such as destination node flush latency)
- Tuning hints
