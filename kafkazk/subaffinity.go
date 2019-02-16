package kafkazk

import (
	"errors"
	"fmt"
	"sort"
)

// SubstitutionAffinities is a mapping of an ID belonging
// to a *Broker marked for replacement and a replacement
// *Broker that will fill all previously filled replica
// slots held by the *Broker being replaced.
type SubstitutionAffinities map[int]*Broker

// Get takes a broker ID and returns a *Broker
// if one was set as a substitution affinity.
func (sa SubstitutionAffinities) Get(id int) *Broker {
	if b, exists := sa[id]; exists {
		b.Used++
		return b
	}

	return nil
}

// SubstitutionAffinities finds all brokers marked for replacement and for
// each broker, it creates an exclusive association with a newly provided broker.
// In the rebuild stage, each to-be-replaced broker will be only replaced with
// the affinity it's associated with. A given new broker can only be an affinity
// for a single outgoing broker. An error is returned if a complete
// mapping of affinities cannot be constructed (e.g. two brokers are
// marked for replacement but only one new replacement was provided
// and substitution affinities is enabled).
func (b BrokerMap) SubstitutionAffinities(pm *PartitionMap) (SubstitutionAffinities, error) {
	replace := map[*Broker]struct{}{}
	missing := map[*Broker]struct{}{}
	new := map[*Broker]struct{}{}
	affinities := SubstitutionAffinities{}

	// Map brokers according to their status.
	for _, broker := range b {
		switch {
		case broker.ID == StubBrokerID:
			continue
		case broker.Missing:
			missing[broker] = struct{}{}
		case broker.Replace:
			replace[broker] = struct{}{}
		case broker.New:
			new[broker] = struct{}{}
		}
	}

	// Check if we have enough new nodes
	// to cover replacements.
	if len(new) < len(replace)+len(missing) {
		return nil, errors.New("Insufficient number of new brokers")
	}

	// Missing brokers are no longer registered in
	// ZooKeeper, thus have no rack ID metadata to
	// reference. Therefore, it must be inferred.

	// For each missing broker, get a list of all replica
	// sets it was in. From this, build a list of rack ID
	// values occupied by remaining brokers in the replicas
	// list plus a list of all rack ID values seen. We will
	// assume that a suitable substitution is any broker that has
	// a rack ID value that has't been used by any of the
	// remaining brokers in the ISRs that the missing
	// broker dropped out from.
	// This assertion is ultimately tested in the rebuild stage
	// where constraints checking is done.
	// TODO we should guarantee this will pass in the rebuild stage
	// by accounting for other brokers that are being replaced and
	// will coexist in a replica set with an affinity determined here.
	for broker := range missing {
		// Get localities that a substitution
		// could reside in.
		localities := pm.LocalitiesAvailable(b, broker)
		// Find the first broker available
		// that resides in one of the
		// available localities.
		var match *Broker
		for _, locality := range localities {
			var err error
			mockBroker := &Broker{
				Locality: locality,
			}
			match, err = constraintsMatch(mockBroker, new)
			if err == nil {
				break
			}
		}

		if match != nil {
			affinities[broker.ID] = match
		} else {
			return nil, fmt.Errorf("Could not infer a replacement for %d", broker.ID)
		}
	}

	// For each broker being replaced, find
	// replacement with the same Rack ID.
	for broker := range replace {
		match, err := constraintsMatch(broker, new)
		if err != nil {
			return affinities, err
		}

		affinities[broker.ID] = match
	}

	return affinities, nil
}

// constraintsMatch takes a *Broker and a map[*Broker]struct{}.
// The map is traversed for a broker that matches the constraints
// of the provided broker. If one is available, it's removed from
// the map and returned. Otherwise, an error is returned.
func constraintsMatch(b *Broker, bm map[*Broker]struct{}) (*Broker, error) {
	// Need a predictable selection.
	brokers := BrokerList{}
	for broker := range bm {
		brokers = append(brokers, broker)
	}

	sort.Sort(brokersByID(brokers))

	// Get the first constraints match.
	for _, broker := range brokers {
		if broker.Locality == b.Locality {
			delete(bm, broker)
			return broker, nil
		}
	}

	// No match was found.
	return nil, fmt.Errorf("Insufficient free brokers for locality %s", b.Locality)
}
