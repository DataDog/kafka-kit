// go:build integration

package kafkaadmin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	testKafkaBootstrapServers = "kafka:9092"
	//testKafkaBootstrapServers = "localhost:60536"
	testKafkaAdminTimeoutMS = time.Duration(5000)
)

func TestKafkaAdminClient(t *testing.T) (context.Context, KafkaAdmin) {
	ctx, _ := context.WithTimeout(context.Background(), testKafkaAdminTimeoutMS*time.Millisecond)
	ka, err := NewClient(Config{
		BootstrapServers: testKafkaBootstrapServers,
	})
	if err != nil {
		t.Logf("failed to initialize client: %s", err)
		t.FailNow()
	}

	return ctx, ka
}

func TestListBrokers(t *testing.T) {
	ctx, ka := testKafkaAdminClient(t)

	ids, err := ka.ListBrokers(ctx)
	assert.Nil(t, err)

	assert.Equal(t, []int{1001, 1002, 1003}, ids, "unexpected IDs list")
}
