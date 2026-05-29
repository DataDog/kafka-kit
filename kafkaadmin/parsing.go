package kafkaadmin

import (
	"fmt"
	"regexp"
)

/*
Enqueue some borrowed helpers from topicmappr that aren't worth exporting (plus,
they're modified a bit for this library).
*/

var (
	// Accepted characters in Kafka topic names.
	topicNormalChar = regexp.MustCompile(`[a-zA-Z0-9_\\-]`)
)

// stringsToRegex takes a []string of topic names and returns a []*regexp.Regexp.
// The values are either a string literal and become ^value$ or are regex and
// compiled then added.
func stringsToRegex(names []string) ([]*regexp.Regexp, error) {
	var out []*regexp.Regexp

	// Update string literals to ^value$ regex.
	for n, t := range names {
		if !containsRegex(t) {
			names[n] = fmt.Sprintf(`^%s$`, t)
		}
	}

	// Compile regex patterns.
	for _, t := range names {
		r, err := regexp.Compile(t)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern: %s\n", t)
		}

		out = append(out, r)
	}

	return out, nil
}

// containsRegex takes a topic name string and returns whether or not
// it should be interpreted as regex.
func containsRegex(t string) bool {
	// Check each character of the topic name. If it doesn't contain a legal Kafka
	// topic name character, we're going to assume it's regex.
	for _, c := range t {
		if !topicNormalChar.MatchString(string(c)) {
			return true
		}
	}

	return false
}
