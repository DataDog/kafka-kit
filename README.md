### Overview

topicmappr was created as a replacement for Kafka's provided `kafka-reassign-partition.sh` tool, providing additional enhancements:

#### Deterministic output
Given the same input, topicmappr will always provide the same output map.

#### Minimal partition movement
Avoids reassigning partitions where movement isn't necessary, greatly reducing reassignment times and resource load for simple recoveries.

#### Balancing placement algorithm with multi-dimensional constraints
For each broker pending replacement, topicmappr chooses the least-utilized candidate broker (based on a combination of topics held and leadership counts) that satisfies the following constraints:

- the broker isn't already in the replica set
- the broker isn't in any of the existing replica set localities (looks up the built in `rack-id` parameter)
- [todo] arbitrary, user-supplied constraint keys

Provided enough brokers, topicmapper determines the appropriate leadership, follower and failure domain balance.

#### Summary output
An output of what's changed along with advisory notices (e.g. insufficient broker counts supplied to satisfy all constraints at the desired partition/replica count).

### Usage

#### Build
Takes a topic name, partition/replica count, and broker list and generates a topic map.

#### rebuild-topic
Takes a topic name and list of target brokers. The broker map is fetched from ZooKeeper and rebuilt with the supplied broker list.

> % topicmappr -rebuild-topic='myTopic' -zk-addr='localhost:2181' -zk-prefix='kafka' -brokers='1001,1002,1005,1006'
```
broker 1007 marked for replacement

WARN:
  [none]

Changes:
  myTopic p0: [1005 1006] -> [1001 1002] replaced broker
  myTopic p1: [1006 1007] -> [1002 1005] replaced broker
  myTopic p2: [1007 1001] -> [1005 1001] replaced broker
  myTopic p3: [1001 1002] -> [1006 1001] replaced broker
  myTopic p4: [1002 1005] -> [1006 1001] replaced broker
  myTopic p5: [1005 1007] -> [1005 1006] replaced broker
  myTopic p6: [1006 1001] -> [1002 1001] replaced broker
  myTopic p7: [1007 1002] -> [1005 1002] replaced broker

New partition map:

{"version":1,"partitions":[{"topic":"myTopic","partition":3,"replicas":[1001,1002]},{"topic":"myTopic","partition":4,"replicas":[1002,1005]},{"topic":"myTopic","partition":5,"replicas":[1005,1001]},{"topic":"myTopic","partition":6,"replicas":[1006,1001]},{"topic":"myTopic","partition":1,"replicas":[1006,1001]},{"topic":"myTopic","partition":0,"replicas":[1005,1006]},{"topic":"myTopic","partition":2,"replicas":[1002,1001]},{"topic":"myTopic","partition":7,"replicas":[1005,1002]}]}
```

#### rebuild-map
Takes an existing topic map and a list of target brokers. A topic initially built with the brokers `[1001,1002,1003]` that lost broker `1003` could be rebuilt by supplying the new broker list `[1001,1002,1004]`.

Example:

>  % topicmappr -rebuild-map '{"version":1,"partitions":[{"topic":"myTopic","partition":0,"replicas":[1005,1006]},{"topic":"myTopic","partition":2,"replicas":[1007,1001]},{"topic":"myTopic","partition":7,"replicas":[1007,1002]},{"topic":"myTopic","partition":6,"replicas":[1006,1001]},{"topic":"myTopic","partition":4,"replicas":[1002,1005]},{"topic":"myTopic","partition":5,"replicas":[1005,1007]},{"topic":"myTopic","partition":3,"replicas":[1001,1002]},{"topic":"myTopic","partition":1,"replicas":[1006,1007]}]}' -brokers="1001,1002,1003,1004,1005,1006,1008"

```
1007 marked for replacement

WARN:
  [none]

Changes:
  myTopic p0: [1005 1006] -> [1005 1006]
  myTopic p1: [1006 1007] -> [1004 1001] replaced broker
  myTopic p2: [1007 1001] -> [1008 1002] replaced broker
  myTopic p3: [1001 1002] -> [1006 1001] replaced broker
  myTopic p4: [1002 1005] -> [1002 1005]
  myTopic p5: [1005 1007] -> [1005 1003] replaced broker
  myTopic p6: [1006 1001] -> [1001 1002] replaced broker
  myTopic p7: [1007 1002] -> [1006 1003] replaced broker

New partition map:

{"version":1,"partitions":[{"topic":"myTopic","partition":0,"replicas":[1005,1006]},{"topic":"myTopic","partition":2,"replicas":[1004,1001]},{"topic":"myTopic","partition":7,"replicas":[1008,1002]},{"topic":"myTopic","partition":6,"replicas":[1006,1001]},{"topic":"myTopic","partition":4,"replicas":[1002,1005]},{"topic":"myTopic","partition":5,"replicas":[1005,1003]},{"topic":"myTopic","partition":3,"replicas":[1001,1002]},{"topic":"myTopic","partition":1,"replicas":[1006,1003]}]}
```
