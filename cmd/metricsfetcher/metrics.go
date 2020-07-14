package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	dd "github.com/zorkian/go-datadog-api"
)

// Both fuctions here fetch 2x the span in duration to handle lagging
// metrics from the DD API. This should result in a max of two rollup values
// per timeseries. We then choose the latest non-nil value.

// It's also OK to ignore errors beyond the metrics query. Sometimes one or more
// timeseries may not have been returned, but if we're not using them, it doesn't
// matter. Error handling for required but missing metrics is handled elsewhere
// in metricsfetcher.

func partitionMetrics(c *Config) (map[string]map[string]map[string]float64, error) {
	start := time.Now().Add(-time.Duration(c.Span*2) * time.Second).Unix()
	o, err := c.Client.QueryMetrics(start, time.Now().Unix(), c.PartnQuery)
	if err != nil {
		return nil, err
	}

	d := map[string]map[string]map[string]float64{}

	for _, ts := range o {
		topic := tagValFromScope(ts.GetScope(), "topic")
		// Cope with the double underscore dedupe in the __consumer_offsets topic.
		if topic == "_consumer_offsets" {
			topic = "__consumer_offsets"
		}

		partition := tagValFromScope(ts.GetScope(), "partition")

		// Get the latest value.
		val, err := latestValue(ts.Points)
		if err != nil {
			continue
		}

		if _, exists := d[topic]; !exists {
			d[topic] = map[string]map[string]float64{}
		}

		d[topic][partition] = map[string]float64{}
		d[topic][partition]["Size"] = val
	}

	return d, nil
}

func brokerMetrics(c *Config) (map[string]map[string]float64, error) {
	start := time.Now().Add(-time.Duration(c.Span*2) * time.Second).Unix()
	o, err := c.Client.QueryMetrics(start, time.Now().Unix(), c.BrokerQuery)
	if err != nil {
		return nil, err
	}

	// Populate.
	d := map[string]map[string]float64{}

	for _, ts := range o {
		broker := tagValFromScope(ts.GetScope(), c.BrokerIDTag)

		// Check that the tag value is actually a broker ID. An improperly tagged or
		// untagged broker may have "N/A" or some other invalid value.
		if _, err := strconv.Atoi(broker); err != nil {
			continue
		}

		// Get the latest value.
		val, err := latestValue(ts.Points)
		if err != nil {
			continue
		}

		if _, exists := d[broker]; !exists {
			d[broker] = map[string]float64{}
		}

		d[broker]["StorageFree"] = val
	}

	return d, nil
}

// latestValue takes a []dd.DataPoint and returns the most recent (in time)
// non-nil datapoint.
func latestValue(points []dd.DataPoint) (float64, error) {
	for i := len(points) - 1; i >= 0; i-- {
		val := points[i][1]
		if val != nil {
			return *val, nil
		}
	}

	return 0, fmt.Errorf("no value found")
}

// tagValFromScope takes a metric scope string and a tag and returns that tag's value.
func tagValFromScope(scope, tag string) string {
	ts := strings.Split(scope, ",")

	return valFromTags(ts, tag)
}

// valFromTags takes a []string of tags and a key, returning the value for the key.
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
