package kafkaadmin

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// BrokerMeta holds metadata that describes a broker.
type BrokerMetadata struct {
	// Internally provided/tracked metadata.
	StorageFree       float64 // Bytes.
	MetricsIncomplete bool
	// Key metadata from the Kafka cluster state.
	Host string
	Port int
	Rack string
	// All metadata.
	FullData map[string]string
}

// BrokerMetaMap is a map of broker IDs to BrokerMetadata.
type BrokerMetadataMap map[int]BrokerMetadata

// NewBrokerMetadataMap returns a BrokerMetadataMap.
func NewBrokerMetadataMap() BrokerMetadataMap {
	return BrokerMetadataMap{}
}

// GetBrokerMetadata returns a BrokerMetadataMap for all live brokers. By default,
// key metadata is populated for each broker's BrokerMetadata entry. If the
// fullData bool is set to True, complete metadata will be included in the
// BrokerMetadata.FullData field. This includes all broker configs found in
// the cluster state including dynamic configs.
func (c Client) GetBrokerMetadata(ctx context.Context, fullData bool) (BrokerMetadataMap, error) {
	var bmm = NewBrokerMetadataMap()

	// Fetch live brokers.
	brokers, err := c.fetchBrokers(ctx)
	if err != nil {
		return nil, err
	}

	var idStrings []string

	// Pre-populate the BrokerMetaMap.
	for _, b := range brokers {
		bmm[int(b.ID)] = BrokerMetadata{
			Host: b.Host,
			Port: b.Port,
		}

		// Build a []string of IDs for the config lookup.
		idStrings = append(idStrings, strconv.Itoa(int(b.ID)))
	}

	// Get full metadata for the brokers.
	md, err := c.GetConfigs(ctx, "broker", idStrings)
	if err != nil {
		return nil, err
	}

	// Populate the BrokerMetaMap.
	for strID, data := range md {
		id, _ := strconv.Atoi(strID)

		if _, exist := bmm[id]; !exist {
			// We shouldn't be here, but to avoid a nil access.
			continue
		}

		// Populate key data.
		b := bmm[id]
		b.Rack = md[strID]["rack.id"]

		// Populate full data is configured.
		if fullData {
			b.FullData = data
		}

		bmm[id] = b
	}

	return bmm, nil
}

// ListBrokers returns a []int of all live broker IDs.
func (c Client) ListBrokers(ctx context.Context) ([]int, error) {
	md, err := c.fetchBrokers(ctx)
	if err != nil {
		return nil, err
	}

	var ids []int
	for _, d := range md {
		ids = append(ids, int(d.ID))
	}

	sort.Ints(ids)

	return ids, nil
}

// fetchBrokers performs a ckg broker metadata lookup.
func (c Client) fetchBrokers(ctx context.Context) ([]kafka.BrokerMetadata, error) {
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

	return md.Brokers, nil
}
