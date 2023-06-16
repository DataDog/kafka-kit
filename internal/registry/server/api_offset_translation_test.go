package server

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeValue(t *testing.T) {
	expectedCheckpoint := Checkpoint{
		UpstreamOffset: 507,
		Offset:         101,
	}

	record := make([]byte, 21)
	binary.BigEndian.PutUint16(record[0:], 0) // header version
	binary.BigEndian.PutUint64(record[2:10], expectedCheckpoint.UpstreamOffset)
	binary.BigEndian.PutUint64(record[10:18], expectedCheckpoint.Offset)
	binary.BigEndian.PutUint16(record[18:20], 0) // metadata string length

	checkpoint := Checkpoint{}
	decodeValue(record, &checkpoint)
	assert.Equal(t, expectedCheckpoint, checkpoint)
}

func TestDecodeKey(t *testing.T) {
	expectedCheckpoint := Checkpoint{
		ConsumerGroupID: "test-consumer",
		Topic:           "test",
		Partition:       7,
	}
	consumerGroupIDLen := uint16(len(expectedCheckpoint.ConsumerGroupID))
	topicLen := uint16(len(expectedCheckpoint.Topic))

	// 8 => 4 bytes on strLength + 4 bytes for Partition
	record := make([]byte, 8+consumerGroupIDLen+topicLen)
	binary.BigEndian.PutUint16(record[0:2], consumerGroupIDLen)
	copy(record[2:2+consumerGroupIDLen], []byte(expectedCheckpoint.ConsumerGroupID))

	pos := 2 + consumerGroupIDLen
	binary.BigEndian.PutUint16(record[pos:pos+2], topicLen)
	copy(record[pos+2:pos+2+topicLen], []byte(expectedCheckpoint.Topic))
	binary.BigEndian.PutUint32(record[pos+2+topicLen:], expectedCheckpoint.Partition)

	checkpoint := Checkpoint{}
	decodeKey(record, &checkpoint)
	assert.Equal(t, expectedCheckpoint, checkpoint)
}
