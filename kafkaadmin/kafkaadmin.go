// Package kafkaadmin wraps Kafka admin API calls.
package kafkaadmin

import (
	"context"
	"fmt"
	"strings"

	"github.com/DataDog/kafka-kit/registry/admin"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	emtpy struct{}
	// SecurityProtocolSet is the set of protocols supported to communicate with brokers
	SecurityProtocolSet = map[string]struct{}{"PLAINTEXT": emtpy, "SSL": emtpy, "SASL_PLAINTEXT": emtpy, "SASL_SSL": emtpy}
	// SASLMechanismSet is the set of mechanisms supported for client to broker authentication
	SASLMechanismSet = map[string]struct{}{"PLAIN": emtpy, "SCRAM-SHA-256": emtpy, "SCRAM-SHA-512": emtpy}
)

type FactoryFunc func(conf *kafka.ConfigMap) (*kafka.AdminClient, error)

type Client struct {
	c *kafka.AdminClient
}

// Config holds Client configuration parameters.
type Config struct {
	BootstrapServers string
	SSLCALocation    string
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
}

// NewClient returns a new Client.
func NewClient(cfg Config) (*Client, error) {
	return newClient(cfg, kafka.NewAdminClient)
}

// NewClientWithFactory returns a new Client using a factory func for the kafkaAdminClient
func NewClientWithFactory(cfg Config, factory FactoryFunc) (*Client, error) {
	return newClient(cfg, factory)
}

func newClient(cfg Config, factory FactoryFunc) (*Client, error) {
	c := &Client{}

	kafkaCfg := &kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	}

	if cfg.SecurityProtocol != "" {
		kafkaCfg.SetKey("security.protocol", cfg.SecurityProtocol)
	}

	if cfg.SecurityProtocol == "SSL" || cfg.SecurityProtocol == "SASL_SSL" {
		if cfg.SSLCALocation == "" {
			return nil, fmt.Errorf("kafka %s is enabled but SSLCALocation was not provided", cfg.SecurityProtocol)
		}
		kafkaCfg.SetKey("ssl.ca.location", cfg.SSLCALocation)
	}

	if strings.HasPrefix(cfg.SecurityProtocol, "SASL_") {
		kafkaCfg.SetKey("sasl.mechanism", cfg.SASLMechanism)
		kafkaCfg.SetKey("sasl.username", cfg.SASLUsername)
		kafkaCfg.SetKey("sasl.password", cfg.SASLPassword)
	}

	k, err := factory(kafkaCfg)
	c.c = k

	if err != nil {
		err = fmt.Errorf("[librdkafka] %s", err)
	}
	return c, err
}

// Close closes the Client.
func (c Client) Close() {
	c.c.Close()
}

// CreateTopic creates a topic.
func (c Client) CreateTopic(ctx context.Context, cfg admin.CreateTopicConfig) error {
	spec := kafka.TopicSpecification{
		Topic:             cfg.Name,
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		ReplicaAssignment: cfg.ReplicaAssignment,
		Config:            cfg.Config,
	}

	// ReplicaAssignment and ReplicationFactor are
	// mutually exclusive.
	if cfg.ReplicaAssignment != nil {
		spec.ReplicationFactor = 0
	}

	topic := []kafka.TopicSpecification{spec}

	_, err := c.c.CreateTopics(ctx, topic)

	return err
}
