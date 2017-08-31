### Overview

topicmappr was created as a replacement for Kafka's provided `kafka-reassign-partition.sh` tool, providing additional enhancements:

#### Deterministic output
Given the same input, topicmappr will always provide the same output map.

#### Minimal partition movement
Avoids reassigning partitions where movement isn't necessary, greatly reducing reassignment times and resource load for simple recoveries.

#### Balancing partition placement with constraints
For each broker pending replacement, topicmappr chooses the least-utilized candidate broker (based on a combination of topics held and leadership counts) that satisfies the following constraints:

- the broker isn't already in the replica set
- the broker isn't in any of the existing replica set localities (looks up the built in `rack-id` parameter)
- [todo] arbitrary, user-supplied constraint keys

Provided enough brokers, topicmapper determines the appropriate leadership, follower and failure domain balance.

#### Summary output
An output of what's changed along with advisory notices (e.g. insufficient broker counts supplied to satisfy all constraints at the desired partition/replica count).

### Usage

Flags:

```
topicmappr -h

Usage of topicmappr:
  -brokers string
        Broker list to rebuild topic partition map with
  -ignore-warns
        Whether a map should be produced if warnings are emitted
  -out-file string
        Output map to file
  -rebuild-map string
        Rebuild a topic map
  -rebuild-topics string
        Rebuild topics (comma delim list) by lookup in ZooKeeper
  -use-meta
        Use broker metadata as constraints (default true)
  -zk-addr string
        ZooKeeper connect string (for broker metadata or rebuild-topic lookups) (default "localhost:2181")
  -zk-prefix string
        ZooKeeper namespace prefix
```

#### rebuild-topics
Takes a comma delimited list of topic names and a list of target brokers. The broker map is fetched from ZooKeeper and rebuilt with the supplied broker list.

Regex is supported for topic names. For instance, providing `--rebuild-topics="myTopic[0-9]"` might return `myTopic1`, `myTopic2`, and `myTopic3`.

> % topicmappr -rebuild-topics myTopic -brokers "0,2" -zk-addr "localhost:2181"

```
Topics:
  myTopic

Broker change summary:
  Broker 1 marked for removal
  Replacing 1, added 1, total count changed by 0

Action:
  Rebuild topic with 1 broker(s) marked for removal

WARN:
  [none]

Partition map changes:
  myTopic p0: [0 1] -> [0 2] replaced broker
  myTopic p1: [1 0] -> [2 0] replaced broker
  myTopic p2: [0 1] -> [0 2] replaced broker
  myTopic p3: [1 0] -> [2 0] replaced broker
  myTopic p4: [0 1] -> [0 2] replaced broker
  myTopic p5: [1 0] -> [2 0] replaced broker
  myTopic p6: [0 1] -> [0 2] replaced broker
  myTopic p7: [1 0] -> [2 0] replaced broker

Partitions assigned:
  Broker 0 - leader: 4, follower: 4, total: 8
  Broker 2 - leader: 4, follower: 4, total: 8

New partition map:

{"version":1,"partitions":[{"topic":"myTopic","partition":0,"replicas":[0,2]},{"topic":"myTopic","partition":1,"replicas":[2,0]},{"topic":"myTopic","partition":2,"replicas":[0,2]},{"topic":"myTopic","partition":3,"replicas":[2,0]},{"topic":"myTopic","partition":4,"replicas":[0,2]},{"topic":"myTopic","partition":5,"replicas":[2,0]},{"topic":"myTopic","partition":6,"replicas":[0,2]},{"topic":"myTopic","partition":7,"replicas":[2,0]}]}
```

#### rebuild-map
Takes an existing topic map and a list of target brokers. A topic initially built with the brokers `[1001,1002,1003]` that lost broker `1003` could be rebuilt by supplying the new broker list `[1001,1002,1004]`.

Multiple topics can be included in the topic map.

Example:

> % topicmappr -rebuild-map '{"version":1,"partitions":[{"topic":"myTopic","partition":0,"replicas":[1005,1006]},{"topic":"myTopic","partition":2,"replicas":[1007,1001]},{"topic":"myTopic","partition":7,"replicas":[1007,1002]},{"topic":"myTopic","partition":6,"replicas":[1006,1001]},{"topic":"myTopic","partition":4,"replicas":[1002,1005]},{"topic":"myTopic","partition":5,"replicas":[1005,1007]},{"topic":"myTopic","partition":3,"replicas":[1001,1002]},{"topic":"myTopic","partition":1,"replicas":[1006,1007]}]}' -brokers="1001,1002,1003,1004,1005,1006,1008" -use-meta=false

```
Topics:
  myTopic

Broker change summary:
  Broker 1007 marked for removal
  Replacing 1, added 3, total count changed by 2

Action:
  Rebuild topic with 1 broker(s) marked for removal

WARN:
  [none]

Partition map changes:
  myTopic p0: [1005 1006] -> [1005 1006]
  myTopic p1: [1006 1007] -> [1006 1003] replaced broker
  myTopic p2: [1007 1001] -> [1003 1001] replaced broker
  myTopic p3: [1001 1002] -> [1001 1002]
  myTopic p4: [1002 1005] -> [1002 1005]
  myTopic p5: [1005 1007] -> [1005 1008] replaced broker
  myTopic p6: [1006 1001] -> [1006 1001]
  myTopic p7: [1007 1002] -> [1004 1002] replaced broker

Partitions assigned:
  Broker 1002 - leader: 1, follower: 2, total: 3
  Broker 1008 - leader: 0, follower: 1, total: 1
  Broker 1004 - leader: 1, follower: 0, total: 1
  Broker 1005 - leader: 2, follower: 1, total: 3
  Broker 1006 - leader: 2, follower: 1, total: 3
  Broker 1003 - leader: 1, follower: 1, total: 2
  Broker 1001 - leader: 1, follower: 2, total: 3

New partition map:
{"version":1,"partitions":[{"topic":"myTopic","partition":0,"replicas":[1005,1006]},{"topic":"myTopic","partition":1,"replicas":[1006,1003]},{"topic":"myTopic","partition":2,"replicas":[1003,1001]},{"topic":"myTopic","partition":3,"replicas":[1001,1002]},{"topic":"myTopic","partition":4,"replicas":[1002,1005]},{"topic":"myTopic","partition":5,"replicas":[1005,1008]},{"topic":"myTopic","partition":6,"replicas":[1006,1001]},{"topic":"myTopic","partition":7,"replicas":[1004,1002]}]}
```

### Safeties using ZooKeeper state

Topicmappr references cluster state from ZooKeeper, automatically enforcing the following:

- The `broker.rack` Kafka attribute is used as a placement constraint. No replica set shall have brokers that share this attribute.
- All provided brokers exist and are visible in ZooKeeper.
- The topic exists in ZooKeeper.

Example attempting to rebuild a topic with a non-existent broker:

> % topicmappr -rebuild-topic myTopic -brokers "0,3"

```
Broker change summary:
  Broker 1 marked for removal
  Broker 3 not found in ZooKeeper
  Replacing 1, added 0, total count changed by -1

Action:
  Shrinking topic by 1 broker(s)

WARN:
  myTopic p0: No additional brokers that meet constraints
  myTopic p1: No additional brokers that meet constraints
  myTopic p2: No additional brokers that meet constraints
  myTopic p3: No additional brokers that meet constraints
  myTopic p4: No additional brokers that meet constraints
  myTopic p5: No additional brokers that meet constraints
  myTopic p6: No additional brokers that meet constraints
  myTopic p7: No additional brokers that meet constraints

Partition map changes:
  myTopic p0: [0 1] -> [0] replaced broker
  myTopic p1: [1 0] -> [0] replaced broker
  myTopic p2: [0 1] -> [0] replaced broker
<...>
```
