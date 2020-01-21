package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/DataDog/kafka-kit/kafkazk"
	"github.com/DataDog/kafka-kit/registry/admin"
	pb "github.com/DataDog/kafka-kit/registry/protos"
)

var (
	// ErrFetchingTopics error.
	ErrFetchingTopics = errors.New("error fetching topics")
	// ErrTopicNotExist error.
	ErrTopicNotExist = errors.New("topic does not exist")
	// ErrTopicNameEmpty error.
	ErrTopicNameEmpty = errors.New("topic Name field must be specified")
	// ErrTopicFieldMissing error.
	ErrTopicFieldMissing = errors.New("topic field missing in request body")
	// ErrTopicAlreadyExists error.
	ErrTopicAlreadyExists = errors.New("topic already exists")
	// ErrInsufficientBrokers error.
	ErrInsufficientBrokers = errors.New("insufficient number of brokers")
	// Misc.
	tregex = regexp.MustCompile(".*")
)

// TopicSet is a mapping of topic name to *pb.Topic.
type TopicSet map[string]*pb.Topic

// GetTopics gets topics. If the input *pb.TopicRequest Name field is
// non-nil, the specified topic is matched if it exists. Otherwise, all
// topics found in ZooKeeper are matched. Matched topics are then filtered
// by all tags specified, if specified, in the *pb.TopicRequest tag field.
func (s *Server) GetTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	ctx, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	// Get topics.
	topics, err := s.fetchTopicSet(req)
	if err != nil {
		return nil, err
	}

	// Populate the response Topics field.
	resp := &pb.TopicResponse{Topics: topics}

	return resp, nil
}

// ListTopics gets topic names. If the input *pb.TopicRequest Name field is
// non-nil, the specified topic is matched if it exists. Otherwise, all
// topics found in ZooKeeper are matched. Matched topics are then filtered
// by all tags specified, if specified, in the *pb.TopicRequest tag field.
func (s *Server) ListTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	ctx, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	// Get topics.
	topics, err := s.fetchTopicSet(req)
	if err != nil {
		return nil, err
	}

	// Populate the response Names field.
	resp := &pb.TopicResponse{Names: topics.Names()}

	return resp, nil
}

// CreateTopic creates a topic if it doesn't exist. Topic tags can optionally
// be set at topic creation time. Additionally, topics can be created on
// a target set of brokers by specifying the broker tag(s) in the request.
func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Empty, error) {
	empty := &pb.Empty{}

	ctx, err := s.ValidateRequest(ctx, req, writeRequest)
	if err != nil {
		return empty, err
	}

	if req.Topic == nil {
		return nil, ErrTopicFieldMissing
	}

	if req.Topic.Name == "" {
		return nil, ErrTopicNameEmpty
	}

	reqParams := &pb.TopicRequest{Name: req.Topic.Name}

	// Check if the topic already exists.
	resp, err := s.ListTopics(ctx, reqParams)
	if err != nil {
		return empty, err
	}

	if len(resp.Names) > 0 {
		return empty, ErrTopicAlreadyExists
	}

	// If we're targeting a specific set of brokers by tag, build
	// a replica assignment.
	var assignment admin.ReplicaAssignment
	if req.TargetBrokerTags != nil {
		// Create a stub map with the provided request dimensions.
		opts := kafkazk.Populate(
			req.Topic.Name,
			int(req.Topic.Partitions),
			int(req.Topic.Replication),
		)
		pMap := kafkazk.NewPartitionMap(opts)

		// Fetch brokers by tag.
		reqParams := &pb.BrokerRequest{Tag: req.TargetBrokerTags}
		resp, err := s.ListBrokers(ctx, reqParams)
		if err != nil {
			return empty, err
		}

		targetBrokerIDs := make([]int, len(resp.Ids))
		for i := range resp.Ids {
			targetBrokerIDs[i] = int(resp.Ids[i])
		}

		if len(targetBrokerIDs) < int(req.Topic.Replication) {
			return empty, ErrInsufficientBrokers
		}

		// Create a stub BrokerMap.
		bMap := kafkazk.NewBrokerMap()

		// Get the live broker metadata.
		brokerState, errs := s.ZK.GetAllBrokerMeta(false)
		if errs != nil {
			return empty, ErrFetchingBrokers
		}

		// Update the BrokerMap with the target broker list.
		// XXX we don't catch any errors here, such as provided
		// brokers being marked as missing. This is because we're
		// only ever using brokers we just fetched from the cluster
		// state as opposed to user provided lists. Other scenarios
		// may need to be covered, however.
		bMap.Update(targetBrokerIDs, brokerState)

		// Rebuild the stub map with the discovered target broker list.
		rebuildParams := kafkazk.RebuildParams{
			BM:       bMap,
			Strategy: "count",
		}

		partitionMap, errs := pMap.Rebuild(rebuildParams)
		if errs != nil {
			return empty, fmt.Errorf("%s", errs)
		}

		// Convert the assignment map to a ReplicaAssignment.
		assignment = PartitionMapToReplicaAssignment(partitionMap)
	}

	// Init the create request.
	// XXX if a topic can't be created due to insufficient brokers,
	// the command will fail at Kafka but not return an error here.
	cfg := admin.CreateTopicConfig{
		Name:              req.Topic.Name,
		Partitions:        int(req.Topic.Partitions),
		ReplicationFactor: int(req.Topic.Replication),
		Config:            req.Topic.Configs,
		ReplicaAssignment: assignment,
	}

	if err = s.kafkaadmin.CreateTopic(ctx, cfg); err != nil {
		return empty, err
	}

	// Tag the topic.
	reqParams.Tag = TagSet(req.Topic.Tags).Tags()
	_, err = s.TagTopic(ctx, reqParams)

	return empty, err
}

// TopicMappings returns all broker IDs that hold at least one partition for
// the requested topic. The topic is specified in the TopicRequest.Name
// field.
func (s *Server) TopicMappings(ctx context.Context, req *pb.TopicRequest) (*pb.BrokerResponse, error) {
	ctx, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, ErrTopicNameEmpty
	}

	// Get a kafkazk.PartitionMap for the topic.
	pm, err := s.ZK.GetPartitionMap(req.Name)
	if err != nil {
		switch err.(type) {
		case kafkazk.ErrNoNode:
			return nil, ErrTopicNotExist
		default:
			return nil, err
		}
	}

	// Get a kafkazk.BrokerMap from the PartitionMap.
	bm := kafkazk.BrokerMapFromPartitionMap(pm, nil, false)

	// Get all brokers as a []int of IDs.
	allf := func(*kafkazk.Broker) bool { return true }
	bl := bm.Filter(allf).List()
	bl.SortByID()

	var ids []uint32

	// Populate broker IDs.
	for _, b := range bl {
		ids = append(ids, uint32(b.ID))
	}

	resp := &pb.BrokerResponse{Ids: ids}

	return resp, nil
}

// TagTopic sets custom tags for the specified topic. Any previously existing
// tags that were not specified in the request remain unmodified.
func (s *Server) TagTopic(ctx context.Context, req *pb.TopicRequest) (*pb.TagResponse, error) {
	ctx, err := s.ValidateRequest(ctx, req, writeRequest)
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, ErrTopicNameEmpty
	}

	if len(req.Tag) == 0 {
		return nil, ErrNilTags
	}

	// Get a TagSet from the supplied tags.
	ts, err := Tags(req.Tag).TagSet()
	if err != nil {
		return nil, err
	}

	// Ensure the topic exists.

	// Get topics from ZK.
	r := regexp.MustCompile(fmt.Sprintf("^%s$", req.Name))
	tr := []*regexp.Regexp{r}

	topics, errs := s.ZK.GetTopics(tr)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	if len(topics) == 0 {
		return nil, ErrTopicNotExist
	}

	// Set the tags.
	err = s.Tags.Store.SetTags(KafkaObject{Type: "topic", ID: req.Name}, ts)
	if err != nil {
		return nil, err
	}

	return &pb.TagResponse{Message: "success"}, nil
}

// DeleteTopicTag deletes custom tags for the specified topic.
func (s *Server) DeleteTopicTags(ctx context.Context, req *pb.TopicRequest) (*pb.TagResponse, error) {
	ctx, err := s.ValidateRequest(ctx, req, writeRequest)
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, ErrTopicNameEmpty
	}

	if len(req.Tag) == 0 {
		return nil, ErrNilTags
	}

	// Ensure the topic exists.

	// Get topics from ZK.
	r := regexp.MustCompile(fmt.Sprintf("^%s$", req.Name))
	tr := []*regexp.Regexp{r}

	topics, errs := s.ZK.GetTopics(tr)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	if len(topics) == 0 {
		return nil, ErrTopicNotExist
	}

	// Delete the tags.
	err = s.Tags.Store.DeleteTags(KafkaObject{Type: "topic", ID: req.Name}, req.Tag)
	if err != nil {
		return nil, err
	}

	return &pb.TagResponse{Message: "success"}, nil
}

// fetchTopicSet fetches metadata for all topics.
func (s *Server) fetchTopicSet(req *pb.TopicRequest) (TopicSet, error) {
	topicRegex := []*regexp.Regexp{}

	// Check if a specific topic is being fetched.
	if req.Name != "" {
		r := regexp.MustCompile(fmt.Sprintf("^%s$", req.Name))
		topicRegex = append(topicRegex, r)
	} else {
		topicRegex = append(topicRegex, tregex)
	}

	// Fetch topics from ZK.
	topics, errs := s.ZK.GetTopics(topicRegex)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	matched := TopicSet{}

	// Populate all topics.
	for _, t := range topics {
		s, _ := s.ZK.GetTopicState(t)
		matched[t] = &pb.Topic{
			Name:       t,
			Partitions: uint32(len(s.Partitions)),
			// TODO more sophisticated check than the
			// first partition len.
			Replication: uint32(len(s.Partitions["0"])),
		}
	}

	filtered, err := s.Tags.FilterTopics(matched, req.Tag)
	if err != nil {
		return nil, err
	}

	return filtered, nil
}

// PartitionMapToReplicaAssignment takes a *kafkazk.PartitionMap and
// transforms it into an admin.ReplicaAssignment.
func PartitionMapToReplicaAssignment(pm *kafkazk.PartitionMap) admin.ReplicaAssignment {
	ra := make(admin.ReplicaAssignment, len(pm.Partitions))

	// Map partition replica sets from the PartitionMap
	// to the ReplicaAssignment.
	for _, p := range pm.Partitions {
		// Type conversion; create the replicas slice.
		ra[p.Partition] = make([]int32, len(p.Replicas))
		// Loop the replica set, convert, and assign
		// to the appropriate index in the ReplicaAssignment.
		for i := range ra[p.Partition] {
			ra[p.Partition][i] = int32(p.Replicas[i])
		}
	}

	return ra
}

// Names returns a []string of topic names from a TopicSet.
func (t TopicSet) Names() []string {
	var names = []string{}

	for n := range t {
		names = append(names, n)
	}

	sort.Strings(names)

	return names
}
