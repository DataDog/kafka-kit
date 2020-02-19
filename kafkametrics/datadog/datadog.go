// Package datadog implements
// a kafkametrics Handler.
package datadog

import (
	"fmt"
	"regexp"
	"time"

	"github.com/DataDog/kafka-kit/kafkametrics"

	dd "github.com/zorkian/go-datadog-api"
)

// Config holds Handler configuration parameters.
type Config struct {
	// Datadog API key.
	APIKey string
	// Datadog app key.
	AppKey string
	// NetworkTXQuery is a query string that should return the outbound
	// network metrics by host for the reference Kafka brokers.
	// Example (Datadog): "avg:system.net.bytes_sent{service:kafka} by {host}"
	NetworkTXQuery string
	// NetworkRXQuery is a query string that should return the inbound
	// network metrics by house for the reference Kafka brokers.
	// Example (Datadog): "avg:system.net.bytes_rcvd{service:kafka} by {host}"
	NetworkRXQuery string
	// BrokerIDTag is the host tag name for Kafka broker IDs.
	BrokerIDTag string
	// MetricsWindow specifies the window size of timeseries data to evaluate
	// in seconds. All values for the window are averaged.
	MetricsWindow int
}

type ddHandler struct {
	c             *dd.Client
	netTXQuery    string
	netRXQuery    string
	brokerIDTag   string
	metricsWindow int
	tagCache      map[string][]string
	keysRegex     *regexp.Regexp
	redactionSub  []byte
}

// NewHandler takes a *Config and returns a Handler, along with any credential
// validation errors. Further backends can be supported with a type switch
// and some other changes.
func NewHandler(c *Config) (kafkametrics.Handler, error) {
	// The underlying client sometimes returns API errors with full dd URL,
	// including parameterized app/api keys. Until an upstream improvement
	// is done, we'll just brute force a redaction via string match/sub in all
	// wrapped errors from the client.
	keysRegex := regexp.MustCompile(fmt.Sprintf("%s|%s", c.APIKey, c.AppKey))

	h := &ddHandler{
		netTXQuery:    fmt.Sprintf("%s.rollup(avg, %d)", c.NetworkTXQuery, c.MetricsWindow),
		netRXQuery:    fmt.Sprintf("%s.rollup(avg, %d)", c.NetworkRXQuery, c.MetricsWindow),
		metricsWindow: c.MetricsWindow,
		brokerIDTag:   c.BrokerIDTag,
		tagCache:      make(map[string][]string),
		keysRegex:     keysRegex,
		redactionSub:  []byte("xxx"),
	}

	client := dd.NewClient(c.APIKey, c.AppKey)

	// Validate.
	ok, err := client.Validate()
	if err != nil {
		return nil, &kafkametrics.APIError{
			Request: "validate credentials",
			Message: h.scrubbedErrorText(err),
		}
	}

	if !ok {
		return nil, &kafkametrics.APIError{
			Request: "validate credentials",
			Message: "invalid API or app key",
		}
	}

	h.c = client

	return h, nil
}

// PostEvent posts an event to the Datadog API.
func (h *ddHandler) PostEvent(e *kafkametrics.Event) error {
	m := &dd.Event{
		Title: &e.Title,
		Text:  &e.Text,
		Tags:  e.Tags,
	}

	_, err := h.c.PostEvent(m)
	return err
}

// GetMetrics requests broker metrics and metadata from the Datadog API and
// returns a BrokerMetrics. If any errors are encountered (i.e. complete
// metadata for a given broker can't be retrieved), the broker will not
// be included in the BrokerMetrics.
// TODO(jamie): retries.
func (h *ddHandler) GetMetrics() (kafkametrics.BrokerMetrics, []error) {
	var errors []error
	var mergedBrokerList []*kafkametrics.Broker

	start := time.Now().Add(-time.Duration(h.metricsWindow) * time.Second).Unix()

	// Get network metrics for tx and rx.
	var lastLen int
	for i, query := range []string{h.netTXQuery, h.netRXQuery} {
		series, err := h.c.QueryMetrics(start, time.Now().Unix(), query)
		if err != nil {
			return nil, []error{&kafkametrics.APIError{
				Request: "metrics query",
				Message: h.scrubbedErrorText(err),
			}}
		}

		if len(series) == 0 {
			return nil, []error{&kafkametrics.NoResults{
				Message: fmt.Sprintf("No data returned with query %s", query),
			}}
		}

		// Get a []*kafkametrics.Broker from the series. Brokers with missing
		// points are excluded from blist.
		blist, errs := brokersFromSeries(series, i)
		if errs != nil {
			errors = append(errors, errs...)
		}

		if i > 0 && len(blist) != lastLen {
			return nil, []error{&kafkametrics.NoResults{
				Message: "Failed to fetch complete metrics for brokers",
			}}
		}

		lastLen = len(blist)

		// Merge the results into the mergedBrokerList.
		mergedBrokerList = mergeBrokerLists(mergedBrokerList, blist)
	}

	// The []*kafkametrics.Broker only contains hostnames and the network tx
	// metric. Fetch the rest of the required metadata and construct a
	// kafkametrics.BrokerMetrics.
	bm, errs := h.brokerMetricsFromList(mergedBrokerList)
	if errs != nil {
		errors = append(errors, errs...)
	}

	return bm, errors
}

// scrubbedErrorText takes an error and returns the message
// string, scrubbed of API and app keys.
func (h *ddHandler) scrubbedErrorText(e error) string {
	return string(h.keysRegex.ReplaceAll([]byte(e.Error()), h.redactionSub))
}
