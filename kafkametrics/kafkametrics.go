// package kafkametrics yields reference
// data structures of kafka broker IDs to
// metrics data, fetched via the Datadog API.
package kafkametrics

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	dd "github.com/zorkian/go-datadog-api"
)

// Config holds KafkaMetrics
// configuration parameters.
type Config struct {
	// Datadog API key.
	APIKey string
	// Datadog app key.
	AppKey string
	// NetworkTXQuery is a Datadog query that should
	// return a series per broker of outbound network
	// metrics. For example: avg:system.net.bytes_sent{service:kafka} by {host}".
	NetworkTXQuery string
	// MetricsWindow specifies the window size of
	// timeseries data to evaluate in seconds.
	// All values for the window are averaged.
	MetricsWindow int
}

// KafkaMetrics makes requests to the
// Datadog API and returns broker metrics.
type KafkaMetrics struct {
	c             *dd.Client
	netTXQuery    string
	metricsWindow int
}

// BrokerMetrics is a map of broker IDs
// to *Broker structs.
type BrokerMetrics map[int]*Broker

// Broker holds metrics and metadata
// for a Kafka broker.
type Broker struct {
	ID           int
	Host         string
	InstanceType string
	NetTX        float64
}

// APIError types are returned
// with Datadog API errors.
type APIError struct {
	request string
	err     string
}

// Error implements the error
// interface for APIError.
func (e *APIError) Error() string {
	return fmt.Sprintf("API error [%s]: %s", e.request, e.err)
}

// PartialResults types are returned
// when incomplete broker metrics or
// metadata is returned.
type PartialResults struct {
	err string
}

// Error implements the error
// interface for PartialResults.
func (e *PartialResults) Error() string {
	return e.err
}

// NewKafkaMetrics takes a *Config and
// returns a *KafkaMetrics, along with
// any credential validation errors.
func NewKafkaMetrics(c *Config) (*KafkaMetrics, error) {
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

	// Construct metric queries used
	// in metric requests.

	var b bytes.Buffer
	b.WriteString(c.NetworkTXQuery)
	b.WriteString(fmt.Sprintf(".rollup(avg, %d)", c.MetricsWindow))
	netQ := b.String()

	k := &KafkaMetrics{
		c:             client,
		netTXQuery:    netQ,
		metricsWindow: c.MetricsWindow,
	}

	return k, nil
}

// GetMetrics requests broker metrics and metadata
// from the Datadog API and returns a BrokerMetrics.
func (k *KafkaMetrics) GetMetrics() (BrokerMetrics, error) {
	brokers := BrokerMetrics{}

	// Get series.
	start := time.Now().Add(-time.Duration(k.metricsWindow) * time.Second).Unix()
	o, err := k.c.QueryMetrics(start, time.Now().Unix(), k.netTXQuery)
	if err != nil {
		return brokers, &APIError{
			request: "metrics query",
			err:     err.Error(),
		}
	}

	if len(o) == 0 {
		return brokers, &PartialResults{
			err: fmt.Sprintf("No data returned with query %s", k.netTXQuery),
		}
	}

	// Series to []*Broker.
	brokerMeta := []*Broker{}
	for _, ts := range o {
		b := &Broker{
			Host:  tagValFromScope(ts.GetScope(), "host"),
			NetTX: ts.Points[0][1] / 1024 / 1024,
		}

		brokerMeta = append(brokerMeta, b)
	}

	// Get broker IDs for each host,
	// populate into a BrokerMetrics.
	var missingTags [][2]string
	for _, b := range brokerMeta {
		ht, err := k.c.GetHostTags(b.Host, "")
		if err != nil {
			return brokers, &APIError{
				request: "host tags",
				err:     fmt.Sprintf("Error requesting host tags for %s", b.Host),
			}
		}

		// Fetch host metadata.
		ids := valFromTags(ht, "broker_id")
		if ids != "" {
			id, _ := strconv.Atoi(ids)
			b.ID = id
			brokers[id] = b
		} else {
			missingTags = append(missingTags, [2]string{"broker_id", b.Host})
		}

		it := valFromTags(ht, "instance-type")
		if it != "" {
			b.InstanceType = it
		} else {
			missingTags = append(missingTags, [2]string{"instance_type", b.Host})
		}
	}

	if len(missingTags) > 0 {
		return brokers, &PartialResults{
			err: fmt.Sprintf("%s tag missing for: %s", missingTags[0], missingTags[1]),
		}
	}

	return brokers, nil
}

// tagValFromScope takes a metric scope string
// and a tag and returns that tag's value.
func tagValFromScope(scope, tag string) string {
	ts := strings.Split(scope, ",")

	return valFromTags(ts, tag)
}

// valFromTags takes a []string of tags and
// a key, returning the value for the key.
func valFromTags(tags []string, key string) string {
	var v []string

	for _, tag := range tags {
		if strings.HasPrefix(tag, key+":") {
			v = strings.Split(tag, ":")
			break
		}
	}

	if len(v) > 1 {
		return v[1]
	}

	return ""
}
