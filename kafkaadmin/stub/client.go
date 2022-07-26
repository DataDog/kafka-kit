package stub

import (
	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
)

// StubClient is a stubbed implementation of KafkaAdminClient.
type Client struct {
	brokerStates kafkaadmin.BrokerStates
}

func NewClient() Client {
	return Client{
		brokerStates: kafkaadmin.BrokerStates{
			1001: {
				Host: "1001",
				Port: 9092,
				Rack: "a",
			},
			1002: {
				Host: "1002",
				Port: 9092,
				Rack: "b",
			},
			1003: {
				Host: "1003",
				Port: 9092,
				Rack: "",
			},
			1004: {
				Host: "1004",
				Port: 9092,
				Rack: "a",
			},
			1005: {
				Host: "1005",
				Port: 9092,
				Rack: "b",
			},
			1007: {
				Host: "1007",
				Port: 9092,
				Rack: "",
			},
		},
	}
}

func (c Client) AddBrokers(bs kafkaadmin.BrokerStates) {
	for id, s := range bs {
		c.brokerStates[id] = s
	}
}
