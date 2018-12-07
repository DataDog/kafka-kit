package server

import (
	"context"
	//"fmt"
	//"log"
	"errors"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

var (
	ErrFetchingBrokers = errors.New("Error fetching brokers")
)

// GetBrokers gets brokers.
func (s *Server) GetBrokers(ctx context.Context, req *pb.BrokerRequest) (*pb.BrokerResponse, error) {
	// Fetch brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	matchedBrokers := map[uint32]*pb.Broker{}
	resp := &pb.BrokerResponse{Brokers: matchedBrokers}

	// If the Broker field is non-nil, we're fetching
	// a specific broker by ID.
	if req.Broker != nil {
		// Lookup the broker.
		if b, ok := brokers[int(req.Broker.Id)]; ok {
			matchedBrokers[req.Broker.Id] = &pb.Broker{
				Id:   req.Broker.Id,
				Rack: b.Rack,
			}
		}

		return resp, nil
	}

	// Populate all brokers.
	for b, m := range brokers {
		matchedBrokers[uint32(b)] = &pb.Broker{
			Id:   uint32(b),
			Rack: m.Rack,
		}
	}

	return resp, nil
}

// ListBrokers gets broker IDs.
func (s *Server) ListBrokers(ctx context.Context, req *pb.BrokerRequest) (*pb.BrokerResponse, error) {
	// Fetch brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	ids := []uint32{}
	resp := &pb.BrokerResponse{Ids: ids}

	// Populate all brokers.
	for b := range brokers {
		ids = append(ids, uint32(b))
	}

	resp.Ids = ids

	return resp, nil
}
