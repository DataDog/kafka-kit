//go:build integration

package kafkaadmin

import (
	"context"
	"testing"
	"time"
)

var (
	testKafkaBootstrapServers = "kafka:9094"
	testKafkaAdminTimeoutMS   = time.Duration(5000)
)

func testKafkaAdminClient(t *testing.T) (context.Context, KafkaAdmin) {
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
