package main

import (
	"bytes"
	"flag"
	"math"
	"os"
	"sort"
)

type constraints struct {
	locality map[string]bool
	id       map[int]bool
}

func newConstraints() *constraints {
	return &constraints{
		locality: make(map[string]bool),
		id:       make(map[int]bool),
	}
}

// whatChanged takes a before and after broker
// replica set and returns a string describing
// what changed.
func whatChanged(s1 []int, s2 []int) string {
	var changes []string

	a, b := make([]int, len(s1)), make([]int, len(s2))
	copy(a, s1)
	copy(b, s2)

	var lchanged bool
	var echanged bool

	// Check if the len is different.
	switch {
	case len(a) > len(b):
		lchanged = true
		changes = append(changes, "decreased replication")
	case len(a) < len(b):
		lchanged = true
		changes = append(changes, "increased replication")
	}

	// If the len is the same,
	// check elements.
	if !lchanged {
		for i := range a {
			if a[i] != b[i] {
				echanged = true
			}
		}
	}

	// Nothing changed.
	if !lchanged && !echanged {
		return "no-op"
	}

	// Determine what else changed.

	// Get smaller replica set len between
	// old vs new, then cap both to this len for
	// comparison.
	slen := int(math.Min(float64(len(a)), float64(len(b))))

	a = a[:slen]
	b = b[:slen]

	echanged = false
	for i := range a {
		if a[i] != b[i] {
			echanged = true
		}
	}

	sort.Ints(a)
	sort.Ints(b)

	samePostSort := true
	for i := range a {
		if a[i] != b[i] {
			samePostSort = false
		}
	}

	// If the broker lists changed but
	// are the same after sorting,
	// we've just changed the preferred
	// leader.
	if echanged && samePostSort {
		changes = append(changes, "preferred leader")
	}

	// If the broker lists changed and
	// aren't the same after sorting, we've
	// replaced a broker.
	if echanged && !samePostSort {
		changes = append(changes, "replaced broker")
	}

	// Construct change string.
	var buf bytes.Buffer
	for i, c := range changes {
		buf.WriteString(c)
		if i < len(changes)-1 {
			buf.WriteString(", ")
		}
	}

	return buf.String()
}

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
	flag.PrintDefaults()
	os.Exit(1)
}
