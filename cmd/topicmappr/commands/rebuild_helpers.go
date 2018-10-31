package commands

import (
	"flag"
	"fmt"
	"os"
)

const (
	indent = "  "
)

// containsRegex takes a topic name
// reference and returns whether or not
// it should be interpreted as regex.
func containsRegex(t string) bool {
	// Check each character of the
	// topic name. If it doesn't contain
	// a legal Kafka topic name character, we're
	// going to assume it's regex.
	for _, c := range t {
		if !topicNormalChar.MatchString(string(c)) {
			return true
		}
	}

	return false
}

func defaultsAndExit() {
	fmt.Println()
	flag.PrintDefaults()
	os.Exit(1)
}
