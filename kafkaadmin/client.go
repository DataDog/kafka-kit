package kafkaadmin

import (
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	empty struct{}
	// SecurityProtocolSet is the set of protocols supported to communicate with brokers
	SecurityProtocolSet = map[string]struct{}{"PLAINTEXT": empty, "SSL": empty, "SASL_PLAINTEXT": empty, "SASL_SSL": empty}
	// SASLMechanismSet is the set of mechanisms supported for client to broker authentication
	SASLMechanismSet = map[string]struct{}{"PLAIN": empty, "SCRAM-SHA-256": empty, "SCRAM-SHA-512": empty}
	// Default timeout for requests to Kafka if a context is passed in with no
	// deadline set.
	defaultTimeout = 5 * time.Second
)

type FactoryFunc func(conf *kafka.ConfigMap) (*kafka.AdminClient, error)

// Client implements a KafkaAdmin.
type Client struct {
	c                *kafka.AdminClient
	DefaultTimeoutMs int
}

// Config holds Client configuration parameters.
type Config struct {
	// Required.
	BootstrapServers string
	// Misc.
	DefaultTimeoutMs int
	GroupId          string
	SSLCALocation    string
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
}

// NewClient returns a KafkaAdmin.
func NewClient(cfg Config) (KafkaAdmin, error) {
	return newClient(cfg, kafka.NewAdminClient)
}

// Close closes the Client.
func (c Client) Close() {
	c.c.Close()
}

// NewClientWithFactory returns a new admin Client using a factory func for the kafkaAdminClient
func NewClientWithFactory(cfg Config, factory FactoryFunc) (*Client, error) {
	return newClient(cfg, factory)
}

func NewConsumer(cfg Config) (*kafka.Consumer, error) {
	kafkaCfg, err := cfgToConfigMap(cfg)
	if err != nil {
		return nil, fmt.Errorf("[config] %s", err)
	}
	c, err := kafka.NewConsumer(kafkaCfg)

	if err != nil {
		err = fmt.Errorf("[librdkafka] %s", err)
	}
	return c, err
}

func cfgToConfigMap(cfg Config) (*kafka.ConfigMap, error) {
	kafkaCfg := &kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	}

	if cfg.GroupId != "" {
		kafkaCfg.SetKey("group.id", cfg.GroupId)
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
	return kafkaCfg, nil
}

func newClient(cfg Config, factory FactoryFunc) (*Client, error) {
	c := &Client{
		DefaultTimeoutMs: cfg.DefaultTimeoutMs,
	}

	if c.DefaultTimeoutMs == 0 {
		c.DefaultTimeoutMs = 5000
	}

	kafkaCfg, err := cfgToConfigMap(cfg)
	if err != nil {
		return nil, fmt.Errorf("[config] %s", err)
	}

	k, err := factory(kafkaCfg)
	c.c = k

	if err != nil {
		err = fmt.Errorf("[librdkafka] %s", err)
	}
	return c, err
}
