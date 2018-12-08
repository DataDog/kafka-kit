package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/DataDog/kafka-kit/kafkazk"
	pb "github.com/DataDog/kafka-kit/registry/protos"
)

var (
	ErrFetchingBrokers = errors.New("Error fetching brokers")
)

// GetBrokers gets brokers.
func (s *Server) GetBrokers(ctx context.Context, req *pb.BrokerRequest) (*pb.BrokerResponse, error) {
	s.LogRequest(ctx, fmt.Sprintf("%v", req))

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
			matchedBrokers[req.Broker.Id] = pbBrokerFromMeta(req.Broker.Id, b)
		}

		return resp, nil
	}

	// Populate all brokers.
	for b, m := range brokers {
		matchedBrokers[uint32(b)] = pbBrokerFromMeta(uint32(b), m)
	}

	return resp, nil
}

// ListBrokers gets broker IDs.
func (s *Server) ListBrokers(ctx context.Context, req *pb.BrokerRequest) (*pb.BrokerResponse, error) {
	s.LogRequest(ctx, fmt.Sprintf("%v", req))

	// Fetch brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	ids := []uint32{}
	resp := &pb.BrokerResponse{Ids: ids}

	// Populate all brokers.
	for b := range brokers {
		resp.Ids = append(resp.Ids, uint32(b))
	}

	return resp, nil
}

func pbBrokerFromMeta(id uint32, b *kafkazk.BrokerMeta) *pb.Broker {
	ts, _ := strconv.ParseInt(b.Timestamp, 10, 64)

	return &pb.Broker{
		Id:                          id,
		ListenerSecurityProtocolMap: b.ListenerSecurityProtocolMap,
		Rack:                        b.Rack,
		JmxPort:                     uint32(b.JMXPort),
		Host:                        b.Host,
		Timestamp:                   ts,
		Port:                        uint32(b.Port),
		Version:                     uint32(b.Version),
	}
}
