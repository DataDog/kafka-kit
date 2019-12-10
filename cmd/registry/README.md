# Overview

### BETA

The Registry service exposes Kafka topic and broker metadata via a gRPC & HTTP API. Lookups can be filtered by tag values (any field in topic and broker message response types) with eventual support for user-defined, custom tags.

# Installation
- `go get github.com/DataDog/kafka-kit/cmd/registry`

Binary will be found at `$GOPATH/bin/registry`

# Usage

## Flags

```
Usage of registry:
  -bootstrap-servers string
    	Kafka bootstrap servers [REGISTRY_BOOTSTRAP_SERVERS] (default "localhost")
  -grpc-listen string
    	Server gRPC listen address [REGISTRY_GRPC_LISTEN] (default "localhost:8090")
  -http-listen string
    	Server HTTP listen address [REGISTRY_HTTP_LISTEN] (default "localhost:8080")
  -kafka-version string
    	Kafka release (Semantic Versioning) [REGISTRY_KAFKA_VERSION] (default "v0.10.2")
  -read-rate-limit int
    	Read request rate limit (reqs/s) [REGISTRY_READ_RATE_LIMIT] (default 5)
  -version
    	version [REGISTRY_VERSION]
  -write-rate-limit int
    	Write request rate limit (reqs/s) [REGISTRY_WRITE_RATE_LIMIT] (default 1)
  -zk-addr string
    	ZooKeeper connect string [REGISTRY_ZK_ADDR] (default "localhost:2181")
  -zk-prefix string
    	ZooKeeper prefix (if Kafka is configured with a chroot path prefix) [REGISTRY_ZK_PREFIX]
  -zk-tags-prefix string
    	Tags storage ZooKeeper prefix [REGISTRY_ZK_TAGS_PREFIX] (default "registry")
```

## Setup

Run Registry, point it at your ZooKeeper cluster:

```
$ registry --zk-addr zk-test-0.service.consul:2181 --zk-prefix kafka --bootstrap-servers kafka1:9092
2019/12/10 21:48:42 Registry running
2019/12/10 21:48:42 Connected to ZooKeeper: zk-test-0.service.consul:2181
2019/12/10 21:48:42 KafkaAdmin connected to bootstrap servers: kafka1:9092
2019/12/10 21:48:42 gRPC up: 0.0.0.0:8090
2019/12/10 21:48:42 HTTP up: 0.0.0.0:8080
```

# API Examples

## List Topics
Lists topic names.

```
$ curl -s localhost:8080/v1/topics/list | jq
{
  "names": [
    "__consumer_offsets",
    "test0",
    "test1"
  ]
}
```

## Get Topic
Returns full metadata for topic.
```
$ curl -s "localhost:8080/v1/topics?name=test0" | jq
{
  "topics": {
    "test0": {
      "name": "test0",
      "partitions": 32,
      "replication": 2
    }
  }
}
```

## Get Topics
Returns full metadata for all topics.
```
$ curl -s localhost:8080/v1/topics
<...>
```

## List/Get Topics with Tag Filtering
Works with both list and get. Any number of tags can be specified (multiple tags currently become a logical AND).

```
$ curl -s "localhost:8080/v1/topics/list?tag=replication:3" | jq
{
  "names": [
    "__consumer_offsets",
    "test0",
    "test1"
  ]
}
```
```
$ curl -s "localhost:8080/v1/topics/list?tag=replication:2&tag=partitions:32" | jq
{
  "names": [
    "test0"
  ]
}
```

## List Brokers
Lists broker IDs.

```
$ curl -s localhost:8080/v1/brokers/list | jq
{
  "ids": [
    1001,
    1002,
    1003,
    1004,
    1005
  ]
}
```


## Get Broker
Returns full metadata for broker.
```
$ curl -s "localhost:8080/v1/brokers?id=1001" | jq
{
  "brokers": {
    "1001": {
      "id": 1001,
      "listenersecurityprotocolmap": {
        "PLAINTEXT": "PLAINTEXT"
      },
      "rack": "europe-west3-a",
      "jmxport": 9999,
      "host": "10.14.224.198",
      "timestamp": "1548442165222",
      "port": 9092,
      "version": 4
    }
  }
}
```

## Get Brokers
Returns full metadata for all brokers.
```
$ curl -s localhost:8080/v1/brokers | jq
<...>
```

## List/Get Brokers with Tag Filtering
Works with both list and get. Any number of tags can be specified, filtering is a "match all".

```
$ curl -s "localhost:8080/v1/brokers?tag=rack:a" | jq
{
  "brokers": {
    "1004": {
      "id": 1004,
      "listenersecurityprotocolmap": {
        "PLAINTEXT": "PLAINTEXT"
      },
      "rack": "a",
      "jmxport": 9999,
      "host": "10.0.1.103",
      "timestamp": "1545171891",
      "port": 9092,
      "version": 4
    }
  }
}
```
```
$ curl -s "localhost:8080/v1/brokers/list?tag=port:9092&tag=rack:a" | jq
{
  "ids": [
    1004
  ]
}
```

## Broker<->Topic Mappings
Returns brokers by topic or topics by brokers.

```
$ curl -s "localhost:8080/v1/mappings/topic/test0" | jq
{
  "ids": [
    1001,
    1002,
    1003
  ]
}
```
```
$ curl -s "localhost:8080/v1/mappings/broker/1001" | jq
{
  "names": [
    "__consumer_offsets",
    "test0",
    "test1"
  ]
}
```

## Set Custom Tags
Add user-defined custom tags. Multiple tags can be set at once. These show in `Get` requests and can be used seamlessly alongside default tags in lookup filtering. Since the tag filtering make no distinction between default and custom tags, setting custom tags names that conflict with default tags are rejected and indicated as such through the API.

```
$ curl -XPUT "localhost:8080/v1/topics/tag/test0?tag=team:eng&tag=use:testing"
{"message":"success"}
```

View the above tag:
```
$ curl -s "localhost:8080/v1/topics?name=test0" | jq
{
  "topics": {
    "test0": {
      "tags": {
        "team": "eng",
        "use": "testing"
      },
      "name": "test0",
      "partitions": 32,
      "replication": 2
    }
  }
}
```

## Delete Custom Tags
Custom tags can be deleted, optionally many at once.
```
$ curl -XDELETE "localhost:8080/v1/topics/tag/test0?tag=team&tag=use"
{"message":"success"}
```

## Create a Topic
Topics can be created through the Registry service. Additionally, all partitions can be scoped to specific brokers by tag. This call embeds [topicmappr](https://github.com/DataDog/kafka-kit/tree/master/cmd/topicmappr) placement constraints logic to ensure safe and optimal partition placement.

```
$ curl -XPOST localhost:8080/v1/topics/create -d '{
  "topic": {
    "name": "test2",
    "partitions": 6,
    "replication": 2,
    "tags": {
      "team": "eng"
    }
  },
  "target_broker_tags": [
    "pool:test"
  ]
}'
```
