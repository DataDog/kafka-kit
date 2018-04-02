package kafkazk

import (
	"errors"
	"sort"
)

var (
	errNoBrokers              = errors.New("No additional brokers that meet constraints")
	errInvalidSelectionMethod = errors.New("Invalid selection method")
)

// Constraints holds a map of
// IDs and locality key-values.
type constraints struct {
	requestSize float64
	locality    map[string]bool
	id          map[int]bool
}

// newConstraints returns an empty *constraints.
func newConstraints() *constraints {
	return &constraints{
		locality: make(map[string]bool),
		id:       make(map[int]bool),
	}
}

// bestCandidate takes a *constraints and selection
// method and returns the most suitable broker.
func (b brokerList) bestCandidate(c *constraints, by string) (*broker, error) {
	// Sort type based on the
	// desired placement criteria.
	switch by {
	case "count":
		sort.Sort(brokersByCount(b))
	case "storage":
		sort.Sort(brokersByStorage(b))
	default:
		return nil, errInvalidSelectionMethod
	}

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
// The requestSize is also subtracted
// from the *broker.StorageFree.
func (c *constraints) add(b *broker) {
	b.StorageFree -= c.requestSize

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
	// Fail if the candidate would run
	// out of storage.
	// TODO this needs thresholds and
	// more intelligent controls.
	case b.StorageFree-c.requestSize < 0:
		return false
	}

	return true
}

// mergeConstraints takes a brokerlist and
// builds a *constraints by merging the
// attributes of all brokers from the supplied list.
func mergeConstraints(bl brokerList) *constraints {
	c := newConstraints()

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
