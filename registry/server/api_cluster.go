package server

import (
	"context"

	pb "github.com/DataDog/kafka-kit/v3/registry/api"
)

// Reassignment returns a *pb.ReassignmentResponse containing the current
// reassignment and its starting date.
func (s *Server) Reassignment(ctx context.Context, _ *pb.Empty) (*pb.ReassignmentResponse, error) {
	reassignments := s.ZK.GetReassignments()
	time := reassignments.CreatedAt
	var topicReassignments []*pb.TopicReassignment
	for t, partitionToReplica := range reassignments.Topics {
		var partitionReassignments []*pb.PartitionReassignment
		for pIndex, replicasAsInt := range partitionToReplica {
			var replicas []uint32
			for r := range replicasAsInt {
				replicas = append(replicas, uint32(r))
			}

			partitionReassignments = append(partitionReassignments, &pb.PartitionReassignment{
				PartitionIndex: uint32(pIndex),
				Replicas:       replicas,
			})
		}
		topicReassignments = append(topicReassignments, &pb.TopicReassignment{
			Name:       t,
			Partitions: partitionReassignments,
		})
	}

	return &pb.ReassignmentResponse{
		CreatedAt: time,
		Topics:    topicReassignments,
	}, nil
}
