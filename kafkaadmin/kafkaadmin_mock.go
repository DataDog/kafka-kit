package kafkaadmin

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/mock"
)

// MockedKafkaAdminClient is a mocked implementation of KafkaAdminClient
type MockedKafkaAdminClient struct {
	mock.Mock
}

// NewAdminClient creates a new AdminClient instance
func (m *MockedKafkaAdminClient) NewAdminClient(conf *kafka.ConfigMap) (*kafka.AdminClient, error) {
	args := m.Called(conf)
	return args.Get(0).(*kafka.AdminClient), args.Error(1)
}
