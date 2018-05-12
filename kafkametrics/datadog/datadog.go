// Package datadog implements
// a kafkametrics Handler.
package datadog

import (
	"fmt"
	"time"

	"github.com/DataDog/topicmappr/kafkametrics"

	dd "github.com/zorkian/go-datadog-api"
)

// Config holds Handler
// configuration parameters.
type Config struct {
	// Datadog API key.
	APIKey string
	// Datadog app key.
	AppKey string
	// NetworkTXQuery is a query string that
	// should return the outbound network metrics
	// by host for the reference Kafka brokers.
	// For example (Datadog): "avg:system.net.bytes_sent{service:kafka} by {host}"
	NetworkTXQuery string
	// BrokerIDTag is the host tag name
	// for Kafka broker IDs.
	BrokerIDTag string
	// MetricsWindow specifies the window size of
	// timeseries data to evaluate in seconds.
	// All values for the window are averaged.
	MetricsWindow int
}

type ddHandler struct {
	c             *dd.Client
	netTXQuery    string
	brokerIDTag   string
	metricsWindow int
	tagCache      map[string][]string
}

// NewHandler takes a *Config and
// returns a Handler, along with
// any credential validation errors.
// Further backends can be supported with
// a type switch and some other changes.
func NewHandler(c *Config) (kafkametrics.Handler, error) {
	client := dd.NewClient(c.APIKey, c.AppKey)

	// Validate.
	ok, err := client.Validate()
	if err != nil {
		return nil, &APIError{
			request: "validate credentials",
			err:     err.Error(),
		}
	}

	if !ok {
		return nil, &APIError{
			request: "validate credentials",
			err:     "invalid API or app key",
		}
	}

	netQ := createNetTXQuery(c)

	k := &ddHandler{
		c:             client,
		netTXQuery:    netQ,
		metricsWindow: c.MetricsWindow,
		brokerIDTag:   c.BrokerIDTag,
		tagCache:      make(map[string][]string),
	}

	return k, nil
}

// PostEvent posts an event to the
// Datadog API.
func (h *ddHandler) PostEvent(e *kafkametrics.Event) error {
	m := &dd.Event{
		Title: &e.Title,
		Text:  &e.Text,
		Tags:  e.Tags,
	}

	_, err := h.c.PostEvent(m)
	return err
}

// GetMetrics requests broker metrics and metadata
// from the Datadog API and returns a BrokerMetrics.
func (h *ddHandler) GetMetrics() (kafkametrics.BrokerMetrics, error) {
	// Get series.
	start := time.Now().Add(-time.Duration(h.metricsWindow) * time.Second).Unix()
	o, err := h.c.QueryMetrics(start, time.Now().Unix(), h.netTXQuery)
	if err != nil {
		return nil, &APIError{
			request: "metrics query",
			err:     err.Error(),
		}
	}

	if len(o) == 0 {
		return nil, &PartialResults{
			err: fmt.Sprintf("No data returned with query %s", h.netTXQuery),
		}
	}

	// Get a []*kafkametrics.Broker from the series.
	blist, err := brokersFromSeries(o)
	if err != nil {
		return nil, err
	}
	// The []*kafkametrics.Broker only contains hostnames
	// and the network tx metric. Fetch the rest
	// of the required metadata and construct
	// a kafkametrics.BrokerMetrics.
	return h.brokerMetricsFromList(blist)
}
