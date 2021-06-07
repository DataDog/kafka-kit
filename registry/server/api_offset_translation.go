package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"time"

	pb "github.com/DataDog/kafka-kit/v3/registry/registry"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	// ErrGroupIDEmpty error.
	ErrGroupIDEmpty = errors.New("GroupId field must be specified")
)

const (
	defaultRemoteAliasCluster      = "source"
	checkpointsTopicSuffix         = ".checkpoints.internal"
	unsetOffset                    = -1001
	seekTimeoutMs                  = 1000
	queryWatermarkOffsetsTimeoutMs = 1000
	consumerFetchTimeout           = 1 * time.Second
)

// Checkpoint holds a record emmitted from the MirrorCheckpointConnector in MirrorMaker2.
type Checkpoint struct {
	Topic           string
	Partition       uint32
	ConsumerGroupID string
	UpstreamOffset  uint64
	Offset          uint64
	Metadata        string
}

// TranslateOffsets translates the last committed remote consumer group's offset into the corresponding local offsets.
func (s *Server) TranslateOffsets(ctx context.Context, req *pb.TranslateOffsetRequest) (*pb.TranslateOffsetResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	if req.RemoteClusterAlias == "" {
		log.Printf("Using the default remote alias cluster: %s", defaultRemoteAliasCluster)
		req.RemoteClusterAlias = defaultRemoteAliasCluster
	}

	if req.GroupId == "" {
		return nil, ErrGroupIDEmpty
	}

	offsets, err := s.translateOffsets(req)
	if err != nil {
		return nil, err
	}
	resp := &pb.TranslateOffsetResponse{Offsets: offsets}
	return resp, nil
}

func (s *Server) translateOffsets(req *pb.TranslateOffsetRequest) (map[string]*pb.OffsetMapping, error) {
	checkpointTopic := req.RemoteClusterAlias + checkpointsTopicSuffix
	checkpointPartition := int32(0)
	checkpointTopicPartition := kafka.TopicPartition{
		Topic:     &checkpointTopic,
		Partition: checkpointPartition,
		Offset:    kafka.OffsetBeginning,
	}
	checkpointAssignment := []kafka.TopicPartition{checkpointTopicPartition}

	s.kafkaconsumer.Assign(checkpointAssignment)
	s.kafkaconsumer.Seek(checkpointTopicPartition, seekTimeoutMs)
	_, endOffset, err := s.kafkaconsumer.QueryWatermarkOffsets(
		checkpointTopic, checkpointPartition, queryWatermarkOffsetsTimeoutMs,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the log end offset for %s:%d", checkpointTopic, checkpointPartition)
	}

	endOfStream := func() bool {
		offsets, _ := s.kafkaconsumer.Position(checkpointAssignment)
		if offsets[0].Offset == unsetOffset {
			return false
		}
		if int64(offsets[0].Offset) < endOffset {
			return false
		}
		return true
	}

	offsets := make(map[string]*pb.OffsetMapping)
	for !endOfStream() {
		msg, err := s.kafkaconsumer.ReadMessage(consumerFetchTimeout)
		if err != nil {
			return nil, fmt.Errorf("consumer error while fetching a message: %v", err)
		}
		checkpoint, err := getCheckpoint(msg)
		if err != nil {
			return nil, err
		}
		if checkpoint.ConsumerGroupID == req.GroupId {
			topicPartition := fmt.Sprintf("%s:%d", checkpoint.Topic, checkpoint.Partition)
			offsets[topicPartition] = &pb.OffsetMapping{
				UpstreamOffset: checkpoint.UpstreamOffset,
				LocalOffset:    checkpoint.Offset,
			}
		}
	}
	s.kafkaconsumer.Unassign()
	return offsets, nil
}

func getCheckpoint(msg *kafka.Message) (*Checkpoint, error) {
	checkpoint := Checkpoint{}
	err := decodeValue(msg.Value, &checkpoint)
	if err != nil {
		return nil, err
	}
	err = decodeKey(msg.Key, &checkpoint)
	if err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func decodeValue(record []byte, checkpoint *Checkpoint) error {
	headerVersion := binary.BigEndian.Uint16(record[0:2])
	if headerVersion != 0 {
		return fmt.Errorf("unknown header version on checkpoint record")
	}

	checkpoint.UpstreamOffset = binary.BigEndian.Uint64(record[2:10])
	checkpoint.Offset = binary.BigEndian.Uint64(record[10:18])
	strLength := binary.BigEndian.Uint16(record[18:20])
	if strLength == 0 {
		checkpoint.Metadata = ""
	} else {
		checkpoint.Metadata = string(record[20 : 20+strLength])
	}

	return nil
}

func decodeKey(record []byte, checkpoint *Checkpoint) error {
	strLength := binary.BigEndian.Uint16(record[0:2])
	if strLength == 0 {
		return fmt.Errorf("unexpected consumer group with empty name")
	}
	checkpoint.ConsumerGroupID = string(record[2 : 2+strLength])

	pos := 2 + strLength
	strLength = binary.BigEndian.Uint16(record[pos : pos+2])
	if strLength == 0 {
		return fmt.Errorf("unexpected topic with empty name")
	}
	checkpoint.Topic = string(record[pos+2 : pos+2+strLength])
	checkpoint.Partition = binary.BigEndian.Uint32(record[pos+2+strLength:])

	return nil
}
