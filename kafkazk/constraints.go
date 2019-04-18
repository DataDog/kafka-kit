package kafkazk

import (
	"errors"
)

var (
	// ErrNoBrokers error.
	ErrNoBrokers = errors.New("No additional brokers that meet Constraints")
	// ErrInvalidSelectionMethod error.
	ErrInvalidSelectionMethod = errors.New("Invalid selection method")
)

// Constraints holds a map of
// IDs and locality key-values.
type Constraints struct {
	requestSize float64
	locality    map[string]bool
	id          map[int]bool
}

// NewConstraints returns an empty *Constraints.
func NewConstraints() *Constraints {
	return &Constraints{
		locality: make(map[string]bool),
		id:       make(map[int]bool),
	}
}

// ConstraintsParams holds parameters for
// the SelectBroker method.
type ConstraintsParams struct {
	SelectorMethod   string
	MinUniqueRackIDs int
	RequestSize      float64
	SeedVal          int64
}

// SelectBroker takes a BrokerList and a ConstraintsParams and
// selects the most suitable broker that passes all specified
// constraints.
func (c *Constraints) SelectBroker(b BrokerList, p ConstraintsParams) (*Broker, error) {
	// Sort type based on the
	// desired placement criteria.
	switch p.SelectorMethod {
	case "count":
		// XXX Should instantiate
		// a dedicated Rand for this.
		b.SortPseudoShuffle(p.SeedVal)
	case "storage":
		b.SortByStorage()
	default:
		return nil, ErrInvalidSelectionMethod
	}

	var candidate *Broker

	// Iterate over candidates.
	for _, candidate = range b.Filter(AllBrokersFn) {
		// Candidate passes, return.
		if c.passesWithParams(candidate, p) {
			c.requestSize = p.RequestSize
			c.Add(candidate)
			candidate.Used++

			return candidate, nil
		}
	}

	// List exhausted, no brokers passed.
	return nil, ErrNoBrokers
}

// TODO deprecate.
// BestCandidate takes a *Constraints, selection method and
// pass / iteration number (for use as a seed value for
// pseudo-random number generation) and returns the
// most suitable broker.
func (b BrokerList) BestCandidate(c *Constraints, by string, p int64) (*Broker, error) {
	// Sort type based on the
	// desired placement criteria.
	switch by {
	case "count":
		// XXX Should instantiate
		// a dedicated Rand for this.
		b.SortPseudoShuffle(p)
	case "storage":
		b.SortByStorage()
	default:
		return nil, ErrInvalidSelectionMethod
	}

	var candidate *Broker

	// Iterate over candidates.
	for _, candidate = range b.Filter(AllBrokersFn) {
		// Candidate passes, return.
		if c.passes(candidate) {
			c.Add(candidate)
			candidate.Used++

			return candidate, nil
		}
	}

	// List exhausted, no brokers passed.
	return nil, ErrNoBrokers
}

// Add takes a *Broker and adds its attributes to the *Constraints.
// The requestSize is also subtracted from the *Broker.StorageFree.
func (c *Constraints) Add(b *Broker) {
	b.StorageFree -= c.requestSize

	if b.Locality != "" {
		c.locality[b.Locality] = true
	}

	c.id[b.ID] = true
}

// MergeConstraints takes a brokerlist and updates the
// *Constraints by merging the attributes of all brokers
// from the supplied list.
func (c *Constraints) MergeConstraints(bl BrokerList) {
	// Don't merge in attributes
	// from nodes that will be removed.
	var f BrokerFilterFn = func(b *Broker) bool {
		if b.Replace {
			return false
		}
		return true
	}

	for _, b := range bl.Filter(f) {
		if b.Locality != "" {
			c.locality[b.Locality] = true
		}

		c.id[b.ID] = true
	}
}

// TODO deprecate.
// passes takes a *Broker and returns whether
// or not it passes Constraints.
func (c *Constraints) passes(b *Broker) bool {
	switch {
	// Fail if the candidate is one of the
	// IDs already in the replica set.
	case c.id[b.ID]:
		return false
	// Fail if the candidate is in any of
	// the existing replica set localities.
	case c.locality[b.Locality]:
		return false
	// Fail if the candidate would run
	// out of storage.
	case b.StorageFree-c.requestSize < 0:
		return false
	}

	return true
}

func (c *Constraints) passesWithParams(b *Broker, p ConstraintsParams) bool {
	var uniqueRackIDsSatisfied bool
	if len(c.locality) >= p.MinUniqueRackIDs {
		uniqueRackIDsSatisfied = true
	}

	switch {
	// Check the candidate against already used IDs.
	case c.id[b.ID]:
		return false
	// Check the candidate against rack ID constraints
	// where all rack IDs must be unique.
	case c.locality[b.Locality] && p.MinUniqueRackIDs == 0:
		return false
	// Check the candidate against rack ID constraints
	// where a non-zero MinUniqueRackIDs is set.
	case c.locality[b.Locality] && p.MinUniqueRackIDs > 0:
		if !uniqueRackIDsSatisfied {
			return false
		}
	// Check the candidate against storage capacity.
	case b.StorageFree-p.RequestSize < 0:
		return false
	}

	return true
}

// TODO deprecate.
// MergeConstraints takes a brokerlist and builds a
// *Constraints by merging the attributes of all brokers
// from the supplied list.
func MergeConstraints(bl BrokerList) *Constraints {
	c := NewConstraints()

	for _, b := range bl {
		// Don't merge in attributes
		// from nodes that will be removed.
		if b.Replace {
			continue
		}

		if b.Locality != "" {
			c.locality[b.Locality] = true
		}

		c.id[b.ID] = true
	}

	return c
}
