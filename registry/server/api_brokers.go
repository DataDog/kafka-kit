package server

import (
	"context"
	"errors"
	"sort"
	"strconv"

	"github.com/DataDog/kafka-kit/kafkazk"
	pb "github.com/DataDog/kafka-kit/registry/protos"
)

var (
	// ErrFetchingBrokers error.
	ErrFetchingBrokers = errors.New("Error fetching brokers")
)

// BrokerSet is a mapping of broker IDs to *pb.Broker.
type BrokerSet map[uint32]*pb.Broker

// GetBrokers gets brokers. If the input *pb.BrokerRequest Id field is
// non-zero, the specified broker is matched if it exists. Otherwise, all
// brokers found in ZooKeeper are matched. Matched brokers are then filtered
// by all tags specified, if specified, in the *pb.BrokerRequest tag field.
func (s *Server) GetBrokers(ctx context.Context, req *pb.BrokerRequest) (*pb.BrokerResponse, error) {
	if err := s.ValidateRequest(ctx, req, readRequest); err != nil {
		return nil, err
	}

	// Get brokers.
	brokers, err := s.fetchBrokerSet(req)
	if err != nil {
		return nil, err
	}

	// Populate response Brokers field.
	resp := &pb.BrokerResponse{Brokers: brokers}

	return resp, nil
}

// ListBrokers gets broker IDs. If the input *pb.BrokerRequest Id field is
// non-zero, the specified broker is matched if it exists. Otherwise, all
// brokers found in ZooKeeper are matched. Matched brokers are then filtered
// by all tags specified, if specified, in the *pb.BrokerRequest tag field.
func (s *Server) ListBrokers(ctx context.Context, req *pb.BrokerRequest) (*pb.BrokerResponse, error) {
	if err := s.ValidateRequest(ctx, req, readRequest); err != nil {
		return nil, err
	}

	// Get brokers.
	brokers, err := s.fetchBrokerSet(req)
	if err != nil {
		return nil, err
	}

	// Populate response Ids field.
	resp := &pb.BrokerResponse{Ids: brokers.IDs()}

	return resp, nil
}

// fetchBrokerSet fetches metadata for all brokers.
func (s *Server) fetchBrokerSet(req *pb.BrokerRequest) (BrokerSet, error) {
	// Get brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	matched := BrokerSet{}

	// Check if a specific broker is being fetched.
	if req.Id != 0 {
		// Lookup the broker.
		if b, ok := brokers[int(req.Id)]; ok {
			matched[req.Id] = pbBrokerFromMeta(req.Id, b)
		}
	} else {
		// Otherwise, populate all brokers.
		for b, m := range brokers {
			matched[uint32(b)] = pbBrokerFromMeta(uint32(b), m)
		}
	}

	// Filter results by any supplied tags.
	filtered, err := s.Tags.FilterBrokers(matched, req.Tag)
	if err != nil {
		return nil, err
	}

	return filtered, nil
}

// IDs returns a []uint32 of IDs from a BrokerSet.
func (b BrokerSet) IDs() []uint32 {
	var ids = []uint32{}
	for id := range b {
		ids = append(ids, id)
	}

	sort.Sort(idList(ids))

	return ids
}

type idList []uint32

func (s idList) Len() int           { return len(s) }
func (s idList) Less(i, j int) bool { return s[i] < s[j] }
func (s idList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func pbBrokerFromMeta(id uint32, b *kafkazk.BrokerMeta) *pb.Broker {
	ts, _ := strconv.ParseInt(b.Timestamp, 10, 64)

	return &pb.Broker{
		Id:                          id,
		Listenersecurityprotocolmap: b.ListenerSecurityProtocolMap,
		Rack:                        b.Rack,
		Jmxport:                     uint32(b.JMXPort),
		Host:                        b.Host,
		Timestamp:                   ts,
		Port:                        uint32(b.Port),
		Version:                     uint32(b.Version),
	}
}
