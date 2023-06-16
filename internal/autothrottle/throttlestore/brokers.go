package throttlestore

// BrokerOverrides is a map of broker ID to BrokerThrottleOverride.
type BrokerOverrides map[int]BrokerThrottleOverride

// BrokerThrottleOverride holds broker-specific overrides.
type BrokerThrottleOverride struct {
	// Broker ID.
	ID int
	// Whether this override is for a broker that's part of a reassignment.
	ReassignmentParticipant bool
	// The ThrottleOverrideConfig.
	Config ThrottleOverrideConfig
}

// BrokerOverridesFilterFn specifies a filter function.
type BrokerOverridesFilterFn func(BrokerThrottleOverride) bool

// Copy returns a copy of a BrokerThrottleOverride.
func (b BrokerThrottleOverride) Copy() BrokerThrottleOverride {
	return BrokerThrottleOverride{
		ID:                      b.ID,
		ReassignmentParticipant: b.ReassignmentParticipant,
		Config: ThrottleOverrideConfig{
			Rate:       b.Config.Rate,
			AutoRemove: b.Config.AutoRemove,
		},
	}
}

// IDs returns a []int of broker IDs held by the BrokerOverrides.
func (b BrokerOverrides) IDs() []int {
	var ids []int
	for id := range b {
		ids = append(ids, id)
	}

	return ids
}

// Filter takes a BrokerOverridesFilterFn and returns a BrokerOverrides where
// all elements return true as an input to the filter func.
func (b BrokerOverrides) Filter(fn BrokerOverridesFilterFn) BrokerOverrides {
	var bo = make(BrokerOverrides)
	for _, bto := range b {
		if fn(bto) {
			bo[bto.ID] = bto.Copy()
		}
	}

	return bo
}
