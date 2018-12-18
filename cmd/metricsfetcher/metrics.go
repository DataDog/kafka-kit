package main

import (
	"strconv"
	"strings"
	"time"
)

func partitionMetrics(c *Config) (map[string]map[string]map[string]float64, error) {
	start := time.Now().Add(-time.Duration(c.Span) * time.Second).Unix()
	o, err := c.Client.QueryMetrics(start, time.Now().Unix(), c.PartnQuery)
	if err != nil {
		return nil, err
	}

	d := map[string]map[string]map[string]float64{}

	for _, ts := range o {
		topic := tagValFromScope(ts.GetScope(), "topic")
		// Cope with the double underscore
		// dedupe in the __consumer_offsets topic.
		if topic == "_consumer_offsets" {
			topic = "__consumer_offsets"
		}

		partition := tagValFromScope(ts.GetScope(), "partition")

		if _, exists := d[topic]; !exists {
			d[topic] = map[string]map[string]float64{}
		}

		d[topic][partition] = map[string]float64{}
		d[topic][partition]["Size"] = *ts.Points[0][1]
	}

	return d, nil
}

func brokerMetrics(c *Config) (map[string]map[string]float64, error) {
	start := time.Now().Add(-time.Duration(c.Span) * time.Second).Unix()
	o, err := c.Client.QueryMetrics(start, time.Now().Unix(), c.BrokerQuery)
	if err != nil {
		return nil, err
	}

	// Populate.
	d := map[string]map[string]float64{}

	for _, ts := range o {
		broker := tagValFromScope(ts.GetScope(), c.BrokerIDTag)

		// Check that the tag value
		// is actually a broker ID.
		// An improperly tagged or
		// untagged broker may have "N/A"
		// or some other invalid value.
		if _, err := strconv.Atoi(broker); err != nil {
			continue
		}

		if _, exists := d[broker]; !exists {
			d[broker] = map[string]float64{}
		}

		d[broker]["StorageFree"] = *ts.Points[0][1]
	}

	return d, nil
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
