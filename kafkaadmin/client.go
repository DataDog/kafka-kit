package kafkaadmin

import (
	"fmt"
	"strings"
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	empty struct{}
	// SecurityProtocolSet is the set of protocols supported to communicate with brokers
	SecurityProtocolSet = map[string]struct{}{"PLAINTEXT": empty, "SSL": empty, "SASL_PLAINTEXT": empty, "SASL_SSL": empty}
	// SASLMechanismSet is the set of mechanisms supported for client to broker authentication
	SASLMechanismSet = map[string]struct{}{"PLAIN": empty, "SCRAM-SHA-256": empty, "SCRAM-SHA-512": empty}
)

type FactoryFunc func(conf *kafka.ConfigMap) (*kafka.AdminClient, error)

// Client implements a KafkaAdmin.
type Client struct {
	c *kafka.AdminClient
}

// Config holds Client configuration parameters.
type Config struct {
	BootstrapServers string
	GroupId          string
	SSLCALocation    string
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
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
	c := &Client{}

	kafkaCfg, err := cfgToConfigMap(cfg)
	if err != nil {
		return nil, fmt.Errorf("[config] %s", err)
	}

	k, err := factory(kafkaCfg)
	c.c = k

	if err != nil {
		err = fmt.Errorf("[librdkafka] %s", err)
	}

	//md, err := k.GetMetadata(nil, true, 3000)
	md, err := k.DescribeConfigs(context.Background(),
	[]kafka.ConfigResource{kafka.ConfigResource{
		Type: kafka.ResourceBroker,
		Name: "1001",
	}})
	fmt.Println(err)

	for _, m := range md {
		fmt.Printf("XXX %s\n", m.Name)
		for k, v := range m.Config {
			fmt.Printf("\t%s: %+v\n", k, v)
		}
	}

	return c, err
}
