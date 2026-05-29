[![testing](https://github.com/DataDog/kafka-kit/actions/workflows/testing.yaml/badge.svg)](https://github.com/DataDog/kafka-kit/actions/workflows/testing.yaml)

# Kafka topic management tools

Read the blog post: [Introducing Kafka-Kit: Tools for Scaling Kafka](https://www.datadoghq.com/blog/engineering/introducing-kafka-kit-tools-for-scaling-kafka/)

This repository contains several services and tools for that help better operate Kafka. See the README for each tool for further details.

# topicmappr
Topicmappr replaces and extends the `kafka-reassign-partition` tool bundled with Kafka. It allows for minimal movement broker replacements, cluster storage rebalancing / partition bin-packing, leadership optimization, many-at-once topic management, and more—all with rack awareness support.

[README](cmd/topicmappr)

# registry
A gRPC+HTTP API service for Kafka that allows granular resource (topics, brokers) lookup and management with custom tagging support.

[README](cmd/registry)

# autothrottle
A service that automatically paces Kafka replication/recovery throttle rates, powered with metrics using the Datadog API.

[README](cmd/autothrottle)

# metricsfetcher
A utility that fetches metrics via the Datadog API for Kafka storage rebalancing and partition mapping with topicmappr.

[README](cmd/metricsfetcher)

# Building

All tools/services will likely build on recent versions of MacOS and Go: `go install ./cmd/...`.

A Docker based environment is available for repeatable linux builds and local testing. This will setup a Kafka environment with a running Registry instance from which all other Kafka-Kit tools will be available:

```
$ make run-compose
docker-compose build
zookeeper uses an image, skipping
kafka uses an image, skipping
Building ssl_setup
[+] Building 0.5s (15/15) FINISHED
[...build output...]

$ docker exec -ti kafka-kit_registry_1 bash
root@280ae6597b9b:/go/src/github.com/DataDog/kafka-kit# which topicmappr registry autothrottle metricsfetcher
/go/bin/topicmappr
/go/bin/registry
/go/bin/autothrottle
/go/bin/metricsfetcher

root@280ae6597b9b:/go/src/github.com/DataDog/kafka-kit# curl -s localhost:8080/v1/topics/list | jq
{
  "topics": {},
  "names": [
    "__consumer_offsets"
  ]
}
```

When finished, `make stop-compose` will tear down the environment.

# Development

See the [Development Guide](https://github.com/DataDog/kafka-kit/wiki/Development-Guide) for testing and contributing changes.
