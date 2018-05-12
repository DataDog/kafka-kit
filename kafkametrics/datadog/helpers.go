package datadog

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/DataDog/topicmappr/kafkametrics"

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
// []dd.Series and returns a []*kafkametrics.Broker. An error
// is returned if for some reason no points were
// returned with the series.
func brokersFromSeries(s []dd.Series) ([]*kafkametrics.Broker, error) {
	bs := []*kafkametrics.Broker{}
	for _, ts := range s {
		host := tagValFromScope(ts.GetScope(), "host")

		if len(ts.Points) == 0 {
			return nil, &PartialResults{
				err: fmt.Sprintf("no points for host %s", host),
			}
		}

		b := &kafkametrics.Broker{
			Host:  host,
			NetTX: *ts.Points[0][1] / 1024 / 1024,
		}

		bs = append(bs, b)
	}

	return bs, nil
}

// brokerMetricsFromList takes a *[]kafkametrics.Broker and fetches
// relevant host tags for all brokers in the list, returning
// a BrokerMetrics.
func (h *ddHandler) brokerMetricsFromList(l []*kafkametrics.Broker) (kafkametrics.BrokerMetrics, error) {
	// Get host tags for brokers
	// in the list.
	tags, err := h.getHostTagMap(l)
	if err != nil {
		return nil, err
	}

	brokers := kafkametrics.BrokerMetrics{}
	err = populateFromTagMap(brokers, h.tagCache, tags, h.brokerIDTag)
	if err != nil {
		return nil, err
	}

	return brokers, nil
}

// getHostTagMap takes a []*kafkametrics.Broker and fetches
// host tags for each. If no errors are encountered,
// a map[*kafkametrics.Broker][]string holding the received tags
// is returned.
func (h *ddHandler) getHostTagMap(l []*kafkametrics.Broker) (map[*kafkametrics.Broker][]string, error) {
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
				return nil, &APIError{
					request: "host tags",
					err:     fmt.Sprintf("Error requesting host tags for %s", b.Host),
				}
			}
			brokers[b] = ht
		}
	}

	return brokers, nil
}

// populateFromTagMap takes a kafkametrics.BrokerMetrics, map of broker
// IDs to []string host tags that functions as a cache, a map of brokers
// to []string unparsed host tag key:value pairs, and a broker ID tag key
// populates the kafkametrics.BrokerMetrics with tags of interest.
// An error describing any missing tags is returned.
func populateFromTagMap(bm kafkametrics.BrokerMetrics, c map[string][]string, t map[*kafkametrics.Broker][]string, btag string) error {
	var missingTags bytes.Buffer

	for b, ht := range t {
		ids := valFromTags(ht, btag)

		if ids != "" {
			id, _ := strconv.Atoi(ids)
			b.ID = id
			bm[id] = b
		} else {
			s := fmt.Sprintf(" %s:%s", btag, b.Host)
			missingTags.WriteString(s)
			// Early break if this tag is missing.
			// We need it in the next step.
			continue
		}

		it := valFromTags(ht, "instance-type")
		if it != "" {
			bm[b.ID].InstanceType = it
			// Cache this broker's tags. In case additional tags are populated
			// in the future, we should only cache brokers that have
			// successfully had all of their tags populated. Leaving it
			// uncached gives it another chance for complete metadata in the
			// preceding API lookups.
			c[b.Host] = t[b]

		} else {
			s := fmt.Sprintf(" instance_type:%s", b.Host)
			missingTags.WriteString(s)
		}
	}

	if missingTags.String() != "" {
		return &PartialResults{
			err: fmt.Sprintf("Missing host tags:%s", missingTags.String()),
		}
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
