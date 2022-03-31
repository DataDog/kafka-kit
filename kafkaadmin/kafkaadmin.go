// Package kafkaadmin provides Kafka administrative functionality.
package kafkaadmin

import (
	"context"
	"regexp"
)

// KafkaAdmin interface.
type KafkaAdmin interface {
	Close()
	// Topics.
	CreateTopic(context.Context, CreateTopicConfig) error
	DeleteTopic(context.Context, string) error
	DescribeTopics(context.Context, []*regexp.Regexp) (TopicStates, error)
	// Cluster.
	SetThrottle(context.Context, SetThrottleConfig) error
	RemoveThrottle(context.Context, RemoveThrottleConfig) error
	GetDynamicConfigs(context.Context, string, []string) (ResourceConfigs, error)
}
