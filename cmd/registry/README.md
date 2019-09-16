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
  -grpc-listen string
    	Server gRPC listen address [REGISTRY_GRPC_LISTEN] (default "localhost:8090")
  -http-listen string
    	Server HTTP listen address [REGISTRY_HTTP_LISTEN] (default "localhost:8080")
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
$ registry --zk-addr zk-test-0.service.consul:2181 --zk-prefix kafka
2018/12/14 18:58:49 Registry running
2018/12/14 18:58:50 Connected to ZooKeeper: zk-test-0.service.consul:2181
2018/12/14 18:58:50 gRPC up: localhost:8090
2018/12/14 18:58:50 HTTP up: localhost:8080
```

## API

Full docs coming soon. Examples (via HTTP/curl):

```
$ curl -s localhost:8080/v1/topics/list | jq
{
  "names": [
    "connect-configs",
    "connect-offsets",
    "connect-status"
  ]
}

$ curl -s localhost:8080/v1/topics?name=connect-offsets | jq
{
  "topics": {
    "connect-offsets": {
      "name": "connect-offsets",
      "partitions": 16,
      "replication": 2
    }
  }
}

$ curl -s localhost:8080/v1/brokers/list?tag=rack:us-east-1a | jq
{
  "ids": [
    1001,
    1002,
    1003,
    1004,
    1018
  ]
}

$ curl -s localhost:8080/v1/brokers?id=1001 | jq
{
  "brokers": {
    "1001": {
      "id": 1001,
      "listenersecurityprotocolmap": {
        "PLAINTEXT": "PLAINTEXT"
      },
      "rack": "us-east-1a",
      "jmxport": 9999,
      "host": "172.21.21.224",
      "timestamp": "1544357419406",
      "port": 9092,
      "version": 4
    }
  }
}
```
