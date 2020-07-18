[![Build Status](https://travis-ci.org/DataDog/kafka-kit.svg?branch=master)](https://travis-ci.org/DataDog/kafka-kit)

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
