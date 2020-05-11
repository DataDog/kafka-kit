package kafkaadmin

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	mkac := &MockedKafkaAdminClient{}
	mkac.On("NewAdminClient", &kafka.ConfigMap{"bootstrap.servers": "kafka:9092", "security.protocol": "PLAINTEXT"}).Return(&kafka.AdminClient{}, nil)
	_, err := NewClientWithFactory(Config{BootstrapServers: "kafka:9092", SecurityProtocol: "PLAINTEXT"}, mkac.NewAdminClient)
	assert.Nil(t, err)
	mkac.AssertExpectations(t)
}

func TestNewClientWithSSLEnabled(t *testing.T) {
	mkac := &MockedKafkaAdminClient{}
	mkac.On("NewAdminClient",
		&kafka.ConfigMap{
			"bootstrap.servers": "kafka:9092",
			"ssl.ca.location":   "/etc/kafka/config/ca.crt",
			"security.protocol": "SSL",
		},
	).Return(&kafka.AdminClient{}, nil)
	_, err := NewClientWithFactory(
		Config{BootstrapServers: "kafka:9092", SSLCALocation: "/etc/kafka/config/ca.crt", SecurityProtocol: "SSL"},
		mkac.NewAdminClient,
	)
	assert.Nil(t, err)
	mkac.AssertExpectations(t)
}

func TestNewClientWithSASLEnabled(t *testing.T) {
	mkac := &MockedKafkaAdminClient{}
	mkac.On("NewAdminClient",
		&kafka.ConfigMap{
			"bootstrap.servers": "kafka:9092",
			"ssl.ca.location":   "/etc/kafka/config/ca.crt",
			"security.protocol": "SASL_SSL",
			"sasl.mechanism":    "PLAIN",
			"sasl.username":     "registry",
			"sasl.password":     "secret",
		},
	).Return(&kafka.AdminClient{}, nil)
	_, err := NewClientWithFactory(
		Config{
			BootstrapServers: "kafka:9092",
			SSLCALocation:    "/etc/kafka/config/ca.crt",
			SecurityProtocol: "SASL_SSL",
			SASLMechanism:    "PLAIN",
			SASLUsername:     "registry",
			SASLPassword:     "secret",
		},
		mkac.NewAdminClient,
	)
	assert.Nil(t, err)
	mkac.AssertExpectations(t)
}
