// Package kafkametrics fetches Kafka
// broker metrics and posts events to
// supported metrics backends.
package kafkametrics

// Handler requests broker metrics
// and posts events.
type Handler interface {
	GetMetrics() (BrokerMetrics, []error)
	PostEvent(*Event) error
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

// Event is used to post autothrottle
// events to the backend metrics system.
type Event struct {
	Title string
	Text  string
	Tags  []string
}
