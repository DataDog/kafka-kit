package stub

import (
	"context"
	"regexp"

	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
)

func (s Client) Close() {
	return
}

func (s Client) CreateTopic(context.Context, kafkaadmin.CreateTopicConfig) error {
	return nil
}
func (s Client) DeleteTopic(context.Context, string) error {
	return nil
}
func (s Client) DescribeTopics(_ context.Context, names []string) (kafkaadmin.TopicStates, error) {
	md := s.metadata

	re := []*regexp.Regexp{}
	for _, name := range names {
		re = append(re, regexp.MustCompile(name))
	}

	for topic := range md.Topics {
		var keep bool
		for _, r := range re {
			if r.MatchString(topic) {
				keep = true
			}
		}
		if !keep {
			delete(md.Topics, topic)
		}
	}

	return kafkaadmin.TopicStatesFromMetadata(&md)
}

func (s Client) SetThrottle(context.Context, kafkaadmin.SetThrottleConfig) error {
	return nil
}
func (s Client) RemoveThrottle(context.Context, kafkaadmin.RemoveThrottleConfig) error {
	return nil
}
func (s Client) ListBrokers(context.Context) ([]int, error) {
	return nil, nil
}

func (s Client) DescribeBrokers(context.Context, bool) (kafkaadmin.BrokerStates, error) {
	return s.brokerStates, nil
}

func (s Client) GetConfigs(context.Context, string, []string) (kafkaadmin.ResourceConfigs, error) {
	return nil, nil
}
func (s Client) GetDynamicConfigs(_ context.Context, _ string, names []string) (kafkaadmin.ResourceConfigs, error) {
	data := kafkaadmin.ResourceConfigs{
		"test1": {"retention.ms": "172800000"},
		"test2": {"retention.ms": "172800000"},
	}

	matched := kafkaadmin.ResourceConfigs{}

	for _, name := range names {
		if _, exist := data[name]; exist {
			matched[name] = data[name]
		}
	}
	return matched, nil
}
