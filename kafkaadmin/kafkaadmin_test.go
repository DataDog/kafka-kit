package kafkaadmin

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	mkac := &MockedKafkaAdminClient{}
	mkac.On("NewAdminClient", &kafka.ConfigMap{"bootstrap.servers": "kafka:9092"}).Return(&kafka.AdminClient{}, nil)
	_, err := NewClientWithFactory(Config{BootstrapServers: "kafka:9092"}, mkac.NewAdminClient)
	assert.Nil(t, err)
	mkac.AssertExpectations(t)
}

func TestNewClientWithSSLEnabled(t *testing.T) {
	mkac := &MockedKafkaAdminClient{}
	mkac.On("NewAdminClient",
		&kafka.ConfigMap{
			"bootstrap.servers": "kafka:9092",
			"security.protocol": "SSL",
			"ssl.ca.location":   "/etc/kafka/config/ca.crt",
		},
	).Return(&kafka.AdminClient{}, nil)
	_, err := NewClientWithFactory(
		Config{BootstrapServers: "kafka:9092", SSLEnabled: true, SSLCALocation: "/etc/kafka/config/ca.crt"},
		mkac.NewAdminClient,
	)
	assert.Nil(t, err)
	mkac.AssertExpectations(t)
}
