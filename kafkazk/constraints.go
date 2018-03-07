package kafkazk

import (
	"sort"
)

type constraints struct {
	locality map[string]bool
	id       map[int]bool
}

func NewConstraints() *constraints {
	return &constraints{
		locality: make(map[string]bool),
		id:       make(map[int]bool),
	}
}

// bestCandidate takes a *constraints
// and returns the *broker with the lowest used
// count that satisfies all constraints.
func (b brokerList) bestCandidate(c *constraints) (*broker, error) {
	sort.Sort(b)

	var candidate *broker

	// Iterate over candidates.
	for _, candidate = range b {
		// Candidate passes, return.
		if c.passes(candidate) {
			c.add(candidate)
			candidate.used++

			return candidate, nil
		}
	}

	// List exhausted, no brokers passed.
	return nil, errNoBrokers
}

// add takes a *broker and adds its
// attributes to the *constraints.
func (c *constraints) add(b *broker) {
	if b.locality != "" {
		c.locality[b.locality] = true
	}

	c.id[b.id] = true
}

// passes takes a *broker and returns
// whether or not it passes constraints.
func (c *constraints) passes(b *broker) bool {
	switch {
	// Fail if the candidate is one of the
	// IDs already in the replica set.
	case c.id[b.id]:
		return false
	// Fail if the candidate is in any of
	// the existing replica set localities.
	case c.locality[b.locality]:
		return false
	}
	return true
}

// mergeConstraints takes a brokerlist and
// builds a *constraints by merging the
// attributes of all brokers from the supplied list.
func mergeConstraints(bl brokerList) *constraints {
	c := NewConstraints()

	for _, b := range bl {
		// Don't merge in attributes
		// from nodes that will be removed.
		if b.replace {
			continue
		}

		if b.locality != "" {
			c.locality[b.locality] = true
		}

		c.id[b.id] = true
	}

	return c
}
