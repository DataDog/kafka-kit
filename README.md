[![Build Status](https://travis-ci.org/DataDog/kafka-kit.svg?branch=master)](https://travis-ci.org/DataDog/kafka-kit)

# Kafka topic management tools

Read the blog post: [Introducing Kafka-Kit: Tools for Scaling Kafka](https://www.datadoghq.com/blog/engineering/introducing-kafka-kit-tools-for-scaling-kafka/)

This repository contains several tools for managing Kafka topic data. See the README for each tool for further details.

# topicmappr
A drop-in replacement for the `kafka-reassign-partition` tool bundled with Kafka. Topicmappr allows for minimal movement broker replacements, many-at-once topic management, replica placement constraints, storage-based rebalancing, and other features.

[README](cmd/topicmappr)

# registry
A gRPC+HTTP API for Kafka that allows granular resource lookup and custom user tagging.

[README](cmd/registry)

# autothrottle
A service that catches Kafka replication/recovery events and dynamically applies broker replication throttles, driven by metrics via the Datadog API.

[README](cmd/autothrottle)

# metricsfetcher
A utility that fetches metrics via the Datadog API for storage-based partition mapping.

[README](cmd/metricsfetcher)
