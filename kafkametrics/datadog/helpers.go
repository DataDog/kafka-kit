package datadog

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/mrmuggymuggy/kafka-kit/kafkametrics"

	dd "github.com/zorkian/go-datadog-api"
)

// createNetTXQuery takes a metric query
// with no aggs plus a window in seconds. A full
// metric query is returned with an avg rollup
// for the provided window.
func createNetTXQuery(c *Config) string {
	var b bytes.Buffer
	b.WriteString(c.NetworkTXQuery)
	b.WriteString(fmt.Sprintf(".rollup(avg, %d)", c.MetricsWindow))
	return b.String()
}

// brokersFromSeries takes metrics series as a
// []dd.Series and returns a []*kafkametrics.Broker.
// If for some reason points were not returned for a
// broker, it's excluded from the []*kafkametrics.Broker
// and an error is populated in the return []error.
func brokersFromSeries(s []dd.Series) ([]*kafkametrics.Broker, []error) {
	bs := []*kafkametrics.Broker{}
	var errors []error

	for _, ts := range s {
		host := tagValFromScope(ts.GetScope(), "host")

		if len(ts.Points) == 0 {
			errors = append(errors, &kafkametrics.PartialResults{
				Message: fmt.Sprintf("No points for host %s", host),
			})
			continue
		}

		b := &kafkametrics.Broker{
			Host:  host,
			NetTX: *ts.Points[0][1] / 1024 / 1024,
		}

		bs = append(bs, b)
	}

	return bs, errors
}

// brokerMetricsFromList takes a *[]kafkametrics.Broker and fetches
// relevant host tags for all brokers in the list, returning
// a BrokerMetrics.
func (h *ddHandler) brokerMetricsFromList(l []*kafkametrics.Broker) (kafkametrics.BrokerMetrics, []error) {
	var errors []error
	// Get host tags for brokers
	// in the list.
	tags, errs := h.getHostTagMap(l)
	if errs != nil {
		errors = append(errors, errs...)
	}

	brokers := kafkametrics.BrokerMetrics{}
	errs = populateFromTagMap(brokers, h.tagCache, tags, h.brokerIDTag, h.InstanceTypeTag)
	if errs != nil {
		errs = append(errors, errs...)
	}
	return brokers, errors
}

// getHostTagMap takes a []*kafkametrics.Broker and fetches
// host tags for each. If no errors are encountered,
// a map[*kafkametrics.Broker][]string holding the received tags
// is returned.
func (h *ddHandler) getHostTagMap(l []*kafkametrics.Broker) (map[*kafkametrics.Broker][]string, []error) {
	var errors []error

	brokers := map[*kafkametrics.Broker][]string{}
	// Get broker IDs for each host,
	// populate into a BrokerMetrics.
	for _, b := range l {
		// Check if we already have this broker's metadata.
		ht, cached := h.tagCache[b.Host]

		if cached {
			brokers[b] = ht
		} else {
			// Else fetch it.
			ht, err := h.c.GetHostTags(b.Host, "")
			if err != nil {
				errors = append(errors, &kafkametrics.APIError{
					Request: "host tags",
					Message: fmt.Sprintf("Error requesting host tags for %s", b.Host),
				})
				continue
			}

			brokers[b] = ht
		}
	}
	return brokers, errors
}

// populateFromTagMap takes a kafkametrics.BrokerMetrics, map of broker
// IDs to []string host tags that functions as a cache, a map of brokers
// to []string unparsed host tag key:value pairs, and a broker ID tag key
// populates the kafkametrics.BrokerMetrics with tags of interest.
// An error describing any missing tags is returned.
func populateFromTagMap(bm kafkametrics.BrokerMetrics, c map[string][]string, t map[*kafkametrics.Broker][]string, btag string, itag string) []error {
	var missingTags bytes.Buffer

	for b, ht := range t {
		// We need to get both the ID and instance type
		// tag values. Both must exist for the broker to be
		// populated in the BrokerMetrics.
		var id int
		var it string

		// Get ID.
		ids := valFromTags(ht, btag)
		if ids != "" {
			id, _ = strconv.Atoi(ids)
		} else {
			s := fmt.Sprintf(" %s:%s", btag, b.Host)
			missingTags.WriteString(s)
			continue
		}

		// Get instance type.
		it = valFromTags(ht, itag)
		if it != "" {
			// Cache this broker's tags. In case additional tags are populated
			// in the future, we should only cache brokers that have
			// successfully had all of their tags populated. Leaving it
			// uncached gives it another chance for complete metadata in the
			// preceding API lookups.
			c[b.Host] = t[b]
		} else {
			s := fmt.Sprintf(" instance_type:%s", b.Host)
			missingTags.WriteString(s)
			continue
		}

		// If we are here, we have
		// both the ID and Instance
		// type tag values. Populate.
		b.ID = id
		b.InstanceType = it
		bm[id] = b

	}

	if missingTags.String() != "" {
		return []error{&kafkametrics.PartialResults{
			Message: fmt.Sprintf("Missing host tags:%s", missingTags.String()),
		}}
	}

	return nil
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
