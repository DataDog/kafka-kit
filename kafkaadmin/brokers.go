package kafkaadmin

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// BrokerMeta holds metadata that describes a broker.
type BrokerState struct {
	// Key metadata from the Kafka cluster state.
	Host                       string
	Port                       int
	Rack                       string // broker.rack
	LogMessageFormat           string // log.message.format.version
	InterBrokerProtocolVersion string // inter.broker.protocol.version
	// All metadata.
	FullData map[string]string
}

// BrokerStates is a map of broker IDs to BrokerState.
type BrokerStates map[int]BrokerState

// NewBrokerStates returns a BrokerStates.
func NewBrokerStates() BrokerStates {
	return BrokerStates{}
}

// DescribeBrokers returns a BrokerStates for all live brokers. By default,
// key metadata is populated for each broker's BrokerState entry. If the
// fullData bool is set to True, complete metadata will be included in the
// BrokerState.FullData field. This includes all broker configs found in
// the cluster state including dynamic configs.
func (c Client) DescribeBrokers(ctx context.Context, fullData bool) (BrokerStates, error) {
	var bmm = NewBrokerStates()

	// Fetch live brokers.
	brokers, err := c.fetchBrokers(ctx)
	if err != nil {
		return nil, err
	}

	var idStrings []string

	// Pre-populate the BrokerStates.
	for _, b := range brokers {
		bmm[int(b.ID)] = BrokerState{
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

	// Populate the BrokerStates.
	for strID, data := range md {
		id, _ := strconv.Atoi(strID)

		if _, exist := bmm[id]; !exist {
			// We shouldn't be here, but to avoid a nil access.
			continue
		}

		// Populate key data.
		b := bmm[id]
		b.Rack = md[strID]["broker.rack"]
		b.LogMessageFormat = md[strID]["log.message.format.version"]
		b.InterBrokerProtocolVersion = md[strID]["inter.broker.protocol.version"]

		// Populate full data if configured.
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

	md, err := c.c.GetMetadata(nil, false, int(to.Milliseconds()))
	if err != nil {
		return nil, ErrorFetchingMetadata{err.Error()}
	}

	return md.Brokers, nil
}
