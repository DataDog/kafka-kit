# topicmappr
Kafka partition assignment tool

Input topic map + list of target brokers:
>  % ./topicmappr -rebuild '{"version":1,"partitions":[{"topic":"myTopic","partition":0,"replicas":[1005,1006]},{"topic":"myTopic","partition":2,"replicas":[1007,1001]},{"topic":"myTopic","partition":7,"replicas":[1007,1002]},{"topic":"myTopic","partition":6,"replicas":[1006,1001]},{"topic":"myTopic","partition":4,"replicas":[1002,1005]},{"topic":"myTopic","partition":5,"replicas":[1005,1007]},{"topic":"myTopic","partition":3,"replicas":[1001,1002]},{"topic":"myTopic","partition":1,"replicas":[1006,1007]}]}' -brokers="1001,1002,1003,1004,1005,1006,1008"

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

{"version":0,"partitions":[{"topic":"myTopic","partition":0,"replicas":[1005,1006]},{"topic":"myTopic","partition":2,"replicas":[1004,1001]},{"topic":"myTopic","partition":7,"replicas":[1008,1002]},{"topic":"myTopic","partition":6,"replicas":[1006,1001]},{"topic":"myTopic","partition":4,"replicas":[1002,1005]},{"topic":"myTopic","partition":5,"replicas":[1005,1003]},{"topic":"myTopic","partition":3,"replicas":[1001,1002]},{"topic":"myTopic","partition":1,"replicas":[1006,1003]}]}
```
