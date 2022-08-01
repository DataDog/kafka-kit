package server

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"

	"github.com/DataDog/kafka-kit/v4/mapper"
	pb "github.com/DataDog/kafka-kit/v4/registry/registry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrFetchingBrokers error.
	ErrFetchingBrokers = status.Error(codes.Internal, "error fetching brokers")
	// ErrBrokerNotExist error.
	ErrBrokerNotExist = status.Error(codes.FailedPrecondition, "broker does not exist")
	// ErrBrokerIDEmpty error.
	ErrBrokerIDEmpty = status.Error(codes.InvalidArgument, "broker Id field must be specified")
	// ErrBrokerIDsEmpty error.
	ErrBrokerIDsEmpty = status.Error(codes.InvalidArgument, "broker Ids field must be specified")
)

// BrokerSet is a mapping of broker IDs to *pb.Broker.
type BrokerSet map[uint32]*pb.Broker

// GetBrokers gets brokers. If the input *pb.BrokerRequest Id field is
// non-zero, the specified broker is matched if it exists. Otherwise, all
// brokers found in ZooKeeper are matched. Matched brokers are then filtered
// by all tags specified, if specified, in the *pb.BrokerRequest tag field.
func (s *Server) GetBrokers(ctx context.Context, req *pb.BrokerRequest) (*pb.BrokerResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	// Get brokers.
	brokers, err := s.fetchBrokerSet(ctx, req)
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
	ctx, cancel, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	// Get brokers.
	brokers, err := s.fetchBrokerSet(ctx, req)
	if err != nil {
		return nil, err
	}

	// Populate response Ids field.
	resp := &pb.BrokerResponse{Ids: brokers.IDs()}

	return resp, nil
}

// UnmappedBrokers returns a list of broker IDs that hold no partitions. An
// optional list of topic names can be specified in the UnmappedBrokersRequest
// exclude field where partitions for those topics are not considered. For
// example, broker 1000 holds no partitions other than one belonging to
// the 'test0' topic. If UnmappedBrokers is called with 'test0' specified as
// an exclude name, broker 1000 will be returned in the BrokerResponse as
// an unmapped broker.
func (s *Server) UnmappedBrokers(ctx context.Context, req *pb.UnmappedBrokersRequest) (*pb.BrokerResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	// Build a map of the exclude names.
	var exclude = map[string]struct{}{}
	for _, e := range req.Exclude {
		exclude[e] = struct{}{}
	}

	// Get a mapper.BrokerMetaMap.
	bm, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	// Get all topic names.
	ts, err := s.ZK.GetTopics([]*regexp.Regexp{regexp.MustCompile(".*")})
	if err != nil {
		return nil, ErrFetchingTopics
	}

	// Strip any topic names that are listed as exclusions.
	var topics []string
	for _, name := range ts {
		if _, excluded := exclude[name]; !excluded {
			topics = append(topics, name)
		}
	}

	// Get a mapper.PartitionMap for each topic.
	var pms []*mapper.PartitionMap
	for _, name := range topics {
		pm, err := s.ZK.GetPartitionMap(name)
		if err != nil {
			return nil, err
		}
		pms = append(pms, pm)
	}

	// Build a set of all broker IDs seen in all partition maps.
	var mappedBrokers = map[int]struct{}{}
	// For each partition map...
	for _, pm := range pms {
		// For each partition...
		for _, partn := range pm.Partitions {
			// Populate each replica (broker) in the partition replica list into
			// the mapped broker set.
			for _, r := range partn.Replicas {
				mappedBrokers[r] = struct{}{}
			}
		}
	}

	// Diff. {all brokers} and {mapped brokers}.
	var unmappedBrokers = map[int]struct{}{}
	for id := range bm {
		if _, mapped := mappedBrokers[id]; !mapped {
			unmappedBrokers[id] = struct{}{}
		}
	}

	// Populate response Ids field.
	resp := &pb.BrokerResponse{}
	for id := range unmappedBrokers {
		resp.Ids = append(resp.Ids, uint32(id))
	}

	sort.Sort(idList(resp.Ids))

	return resp, nil
}

// BrokerMappings returns all topic names that have at least one partition
// held by the requested broker. The broker is specified in the BrokerRequest.ID
// field.
func (s *Server) BrokerMappings(ctx context.Context, req *pb.BrokerRequest) (*pb.TopicResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	if req.Id == 0 {
		return nil, ErrBrokerIDEmpty
	}

	// Get a mapper.BrokerMetaMap.
	bm, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	// Check if the broker exists.
	if _, ok := bm[int(req.Id)]; !ok {
		return nil, ErrBrokerNotExist
	}

	// Get all topic names.
	ts, err := s.ZK.GetTopics([]*regexp.Regexp{regexp.MustCompile(".*")})
	if err != nil {
		return nil, ErrFetchingTopics
	}

	// Get a mapper.PartitionMap for each topic.
	var pms []*mapper.PartitionMap
	for _, p := range ts {
		pm, err := s.ZK.GetPartitionMap(p)
		if err != nil {
			return nil, err
		}
		pms = append(pms, pm)
	}

	// Build a mapping of brokers to topics. This is structured
	// as a map[<broker ID>]map[<topic name>]struct{}.
	var bmapping = make(map[int]map[string]struct{})

	for _, pm := range pms {
		// For each PartitionMap, get a mapper.BrokerMap.
		bm := mapper.BrokerMapFromPartitionMap(pm, nil, false)

		// Add the topic name to each broker's topic set.
		name := pm.Partitions[0].Topic

		for id := range bm {
			if bmapping[id] == nil {
				bmapping[id] = map[string]struct{}{}
			}
			bmapping[id][name] = struct{}{}
		}
	}

	// Get a []string of topic names where at least one
	// partition is held by the requested broker.
	names := []string{}
	for n := range bmapping[int(req.Id)] {
		names = append(names, n)
	}

	sort.Strings(names)

	return &pb.TopicResponse{Names: names}, nil
}

// fetchBrokerSet fetches metadata for all brokers.
func (s *Server) fetchBrokerSet(ctx context.Context, req *pb.BrokerRequest) (BrokerSet, error) {
	// Get broker states.
	brokerStates, errs := s.kafkaadmin.DescribeBrokers(ctx, false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	brokers, _ := mapper.BrokerMetaMapFromStates(brokerStates)
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

// TagBroker sets custom tags for the specified broker. Any previously existing
// tags that were not specified in the request remain unmodified.
func (s *Server) TagBroker(ctx context.Context, req *pb.BrokerRequest) (*pb.TagResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, req, writeRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	if err := s.Locking.Lock(ctx); err != nil {
		return nil, err
	}
	defer s.Locking.UnlockLogError(ctx)

	if req.Id == 0 {
		return nil, ErrBrokerIDEmpty
	}

	if len(req.Tag) == 0 {
		return nil, ErrNilTags
	}

	// Get a TagSet from the supplied tags.
	ts, err := Tags(req.Tag).TagSet()
	if err != nil {
		return nil, err
	}

	// Set the tags.
	id := fmt.Sprintf("%d", req.Id)
	err = s.Tags.Store.SetTags(KafkaObject{Type: "broker", ID: id}, ts)
	if err != nil {
		return nil, err
	}

	return &pb.TagResponse{Message: "success"}, nil
}

// TagBrokers sets custom tags for the specified brokers. Any previously existing
// tags that were not specified in the request remain unmodified.
func (s *Server) TagBrokers(ctx context.Context, req *pb.TagBrokersRequest) (*pb.TagBrokersResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, req, writeRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	if err := s.Locking.Lock(ctx); err != nil {
		return nil, err
	}
	defer s.Locking.UnlockLogError(ctx)

	if len(req.Ids) == 0 {
		return nil, ErrBrokerIDsEmpty
	}

	if len(req.Tags) == 0 {
		return nil, ErrNilTags
	}

	// Get a TagSet from the supplied tags.
	ts, err := Tags(req.Tags).TagSet()
	if err != nil {
		return nil, err
	}

	ids := req.Ids
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	// Set the tags.
	for _, id := range ids {
		sid := fmt.Sprintf("%d", id)
		err = s.Tags.Store.SetTags(KafkaObject{Type: "broker", ID: sid}, ts)
	}
	if err != nil {
		return nil, err
	}

	return &pb.TagBrokersResponse{}, nil
}

//DeleteBrokerTags deletes custom tags for the specified broker.
func (s *Server) DeleteBrokerTags(ctx context.Context, req *pb.BrokerRequest) (*pb.TagResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, req, writeRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	if err := s.Locking.Lock(ctx); err != nil {
		return nil, err
	}
	defer s.Locking.UnlockLogError(ctx)

	if req.Id == 0 {
		return nil, ErrBrokerIDEmpty
	}

	if len(req.Tag) == 0 {
		return nil, ErrNilTags
	}

	// Ensure the broker exists.

	// Get brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return nil, ErrFetchingBrokers
	}

	if _, exist := brokers[int(req.Id)]; !exist {
		return nil, ErrBrokerNotExist
	}

	// Delete the tags.
	id := fmt.Sprintf("%d", req.Id)
	err = s.Tags.Store.DeleteTags(KafkaObject{Type: "broker", ID: id}, Tags(req.Tag).Keys())
	if err != nil {
		return nil, err
	}

	return &pb.TagResponse{Message: "success"}, nil
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

func pbBrokerFromMeta(id uint32, b *mapper.BrokerMeta) *pb.Broker {
	ts, _ := strconv.ParseInt(b.Timestamp, 10, 64)

	return &pb.Broker{
		Id:                          id,
		Listenersecurityprotocolmap: b.ListenerSecurityProtocolMap,
		Rack:                        b.Rack,
		Jmxport:                     uint32(b.JMXPort),
		Endpoints:                   b.Endpoints,
		Host:                        b.Host,
		Timestamp:                   ts,
		Port:                        uint32(b.Port),
		Version:                     uint32(b.Version),
	}
}
