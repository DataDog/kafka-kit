# Kafka topic management tools

This repository contains several tools for managing Kafka topic data. See the README for each tool for further details.

# topicmappr
A drop-in replacement for the `kafka-reassign-partition` tool bundled with Kafka. Topicmappr allows for minimal movement broker replacements, many-at-once topic management, replica placement constraints, and other features.

[README](cmd/topicmappr)

# autothrottle
A service that catches Kafka replication/recovery events and dynamically applies broker replication throttles, driven by metrics via the Datadog API.

[README](cmd/autothrottle)
