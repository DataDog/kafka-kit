package kafkaadmin

import (
	"context"
	"sort"
	"time"
)

// BrokerMeta holds metadata that describes a broker.
type BrokerMetadata struct {
	// Internally provided/tracked metadata.
	StorageFree       float64 // Bytes.
	MetricsIncomplete bool
	// Metadata from the Kafka cluster state.
	ListenerSecurityProtocolMap map[string]string
	Endpoints                   []string
	Rack                        string
	Host                        string
	Port                        int
}

// BrokerMetaMap is a map of broker IDs to BrokerMetadata.
type BrokerMetadataMap map[int]BrokerMetadata

// GetBrokerMetadata returns a BrokerMetadataMap for all live brokers.
func (c Client) GetBrokerMetadata(ctx context.Context, ids []int) (BrokerMetadataMap, error) {
	var bmm = NewBrokerMetadataMap()
	return bmm, nil
}

// func (a *AdminClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error)
func (c Client) ListBrokers(ctx context.Context) ([]int, error) {
	// Configure the request timeout.
	var to time.Duration
	dl, ok := ctx.Deadline()

	// If the context does not have a deadline set, use the default value.
	if !ok {
		to = time.Millisecond * time.Duration(c.DefaultTimeoutMs)
	} else {
		to = time.Until(dl)
	}

	// confluent-kafka-go loads both topic and broker metadata in a single call.
	// This is a hack to avoid looking up topic metadata.
	ts := ""
	md, err := c.c.GetMetadata(&ts, false, int(to.Milliseconds()))
	if err != nil {
		return nil, ErrorFetchingMetadata{err.Error()}
	}

	var ids []int
	for _, bmd := range md.Brokers {
		ids = append(ids, int(bmd.ID))
	}

	sort.Ints(ids)

	return ids, nil
}

func NewBrokerMetadataMap() BrokerMetadataMap {
	return BrokerMetadataMap{}
}
