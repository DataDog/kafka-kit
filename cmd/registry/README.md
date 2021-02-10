# Overview

The Registry service exposes Kafka topic and broker metadata via a gRPC & HTTP API. Resources can be queried and filtered using inherent attributes along with custom, user-defined tags.

Some example questions a user might ask by querying the Registry service:
- _Give me all broker IDs where the rack equals us-east-1a_
- _Give me the configurations for all topics tagged "environment:dev"_

Additionally, Registry is continuously receiving support for write operations. An example operations that's possible using tags and topic creation (see further into the README for the `target_broker_tags` feature):
- _Tag brokers 1001, 1002, 1003 with "pool:inbound"_
- _Create a topic named "inbound" with 32 partitions, a replication factor of 2, and place all partitions on brokers tagged "pool:inbound", and tag the topic "primary:true"_

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
  -kafka-sasl-mechanism string
    	SASL mechanism to use for authentication. Supported: SCRAM-SHA-512, PLAIN, SCRAM-SHA-256 [REGISTRY_KAFKA_SASL_MECHANISM]
  -kafka-sasl-password string
    	SASL password for use with the PLAIN and SASL-SCRAM-* mechanisms [REGISTRY_KAFKA_SASL_PASSWORD]
  -kafka-sasl-username string
    	SASL username for use with the PLAIN and SASL-SCRAM-* mechanisms [REGISTRY_KAFKA_SASL_USERNAME]
  -kafka-security-protocol string
    	Protocol used to communicate with brokers. Supported: SSL, SASL_PLAINTEXT, SASL_SSL, PLAINTEXT [REGISTRY_KAFKA_SECURITY_PROTOCOL]
  -kafka-ssl-ca-location string
    	CA certificate path (.pem/.crt) for verifying broker's identity. Needed for SSL and SASL_SSL protocols. [REGISTRY_KAFKA_SSL_CA_LOCATION]
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

See the Registry [proto](https://github.com/DataDog/kafka-kit/blob/master/registry/protos/registry.proto) definition for further details. The API is designed gRPC-first and provides HTTP using [grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway); the mappings are described in the proto file.

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

## Get unmapped brokers
Returns brokers that host no partitions. Optionally, `exclude` topic names can be specified where any partitions belonging to excluded topics are not counted as to whether a broker is considered mapped.

```
$ curl -s "localhost:8080/v1/brokers/unmapped?exclude=test1&exclude=test2&exclude=__consumer_offsets" | jq
{
  "ids": [
    1001,
    1002,
    1003
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
Add user-defined custom tags. Multiple tags can be set at once. These show in `Get` requests and can be used seamlessly alongside default tags in lookup filtering. Since the tag filtering make no distinction between inherent attribute and custom tags, setting custom tags names that conflict with default tags are rejected and indicated as such through the API.

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
