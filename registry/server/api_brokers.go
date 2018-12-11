package server

import (
	"context"
	"errors"
	"strconv"

	"github.com/DataDog/kafka-kit/kafkazk"
	pb "github.com/DataDog/kafka-kit/registry/protos"
)

var (
	ErrFetchingBrokers = errors.New("Error fetching brokers")
)

// GetBrokers gets brokers.
func (s *Server) GetBrokers(ctx context.Context, req *pb.BrokerRequest) (*pb.BrokerResponse, error) {
	if err := s.ValidateRequest(ctx, req, ReadRequest); err != nil {
		return nil, err
	}

	// Fetch brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	matchedBrokers := map[uint32]*pb.Broker{}

	// Check if a specific broker is being fetched.
	if req.Id != 0 {
		// Lookup the broker.
		if b, ok := brokers[int(req.Id)]; ok {
			matchedBrokers[req.Id] = pbBrokerFromMeta(req.Id, b)
		}
	} else {
		// Otherwise, populate all brokers.
		for b, m := range brokers {
			matchedBrokers[uint32(b)] = pbBrokerFromMeta(uint32(b), m)
		}
	}

	filteredBrokers, err := s.Tags.FilterBrokers(matchedBrokers, req.Tag)
	if err != nil {
		return nil, err
	}

	resp := &pb.BrokerResponse{Brokers: filteredBrokers}

	return resp, nil
}

// ListBrokers gets broker IDs.
func (s *Server) ListBrokers(ctx context.Context, req *pb.BrokerRequest) (*pb.BrokerResponse, error) {
	if err := s.ValidateRequest(ctx, req, ReadRequest); err != nil {
		return nil, err
	}

	// Fetch brokers from ZK.
	// TODO replace this with a GetChildren call.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	matchedBrokers := map[uint32]*pb.Broker{}

	// Check if a specific broker is being fetched.
	if req.Id != 0 {
		// Lookup the broker.
		if b, ok := brokers[int(req.Id)]; ok {
			matchedBrokers[req.Id] = pbBrokerFromMeta(req.Id, b)
		}
	} else {
		// Otherwise, populate all brokers.
		for b, m := range brokers {
			matchedBrokers[uint32(b)] = pbBrokerFromMeta(uint32(b), m)
		}
	}

	filteredBrokers, err := s.Tags.FilterBrokers(matchedBrokers, req.Tag)
	if err != nil {
		return nil, err
	}

	var ids = []uint32{}
	for id := range filteredBrokers {
		ids = append(ids, id)
	}

	resp := &pb.BrokerResponse{Ids: ids}

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
