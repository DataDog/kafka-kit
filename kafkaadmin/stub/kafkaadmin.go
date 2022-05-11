package stub

import (
	"context"

	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
)

// StubClient is a stubbed implementation of KafkaAdminClient.
type Client struct{}

func (s Client) Close() {
	return
}

func (s Client) CreateTopic(context.Context, kafkaadmin.CreateTopicConfig) error {
	return nil
}
func (s Client) DeleteTopic(context.Context, string) error {
	return nil
}
func (s Client) DescribeTopics(context.Context, []string) (kafkaadmin.TopicStates, error) {
	return kafkaadmin.TopicStatesFromMetadata(fakeKafkaMetadata())
}

func (s Client) SetThrottle(context.Context, kafkaadmin.SetThrottleConfig) error {
	return nil
}
func (s Client) RemoveThrottle(context.Context, kafkaadmin.RemoveThrottleConfig) error {
	return nil
}
func (s Client) GetDynamicConfigs(context.Context, string, []string) (kafkaadmin.ResourceConfigs, error) {
	return nil, nil
}
