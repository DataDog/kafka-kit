// go:build integration

package kafkaadmin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetBrokerMetadata(t *testing.T) {
	ctx, ka := testKafkaAdminClient(t)

	md, err := ka.GetBrokerMetadata(ctx, false)
	assert.Nil(t, err)

	// It's expected that the metadata will evolve over time; rather than managing
	// complete data fixtures, we'll just spot check that we're approximately
	// fetching the data that we're looking for and for the correct number of brokers.

	assert.Len(t, md, 3, "unexpected number of brokers in the BrokerMetadataMap")
	assert.Equal(t, 9094, md[1001].Port, "unexpected value")
	assert.Equal(t, "1a", md[1002].Rack, "unexpected value")
	assert.Nil(t, md[1001].FullData)

	// Fetch with full metadata.
	md, err = ka.GetBrokerMetadata(ctx, true)
	assert.Nil(t, err)

	// Check a random value.
	assert.Equal(t, "true", md[1002].FullData["auto.leader.rebalance.enable"], "unexpected value")
}

func TestListBrokers(t *testing.T) {
	ctx, ka := testKafkaAdminClient(t)

	ids, err := ka.ListBrokers(ctx)
	assert.Nil(t, err)

	assert.Equal(t, []int{1001, 1002, 1003}, ids, "unexpected IDs list")
}
