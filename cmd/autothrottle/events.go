package main

import (
	"fmt"
	"log"

	"github.com/DataDog/kafka-kit/kafkametrics"
)

// Events configs.
var eventTitlePrefix = "kafka-autothrottle"

// DDEventWriter wraps a channel where *kafkametrics.Event are written
// to along with any defaults configs, such as tags to apply to each event.
type DDEventWriter struct {
	c           chan *kafkametrics.Event
	tags        []string
	titlePrefix string
}

// Write takes an event title and message string and writes a
// *kafkametrics.Event to the event channel, formatted with
// the configured title and tags.
func (e *DDEventWriter) Write(t string, m string) {
	e.c <- &kafkametrics.Event{
		Title: fmt.Sprintf("[%s] %s", e.titlePrefix, t),
		Text:  m,
		Tags:  e.tags,
	}
}

// eventWriter reads from a channel of *kafkametrics.Event and writes
// them to the Datadog API.
func eventWriter(k kafkametrics.Handler, c chan *kafkametrics.Event) {
	for e := range c {
		err := k.PostEvent(e)
		if err != nil {
			log.Printf("Error writing event: %s\n", err)
		}
	}
}
