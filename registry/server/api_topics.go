package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"

	zklocking "github.com/DataDog/kafka-kit/v3/cluster/zookeeper"
	"github.com/DataDog/kafka-kit/v3/kafkaadmin"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
	pb "github.com/DataDog/kafka-kit/v3/registry/registry"
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
	allTopicsRegex = regexp.MustCompile(".*")
)

// TopicSet is a mapping of topic name to *pb.Topic.
type TopicSet map[string]*pb.Topic

// GetTopics gets topics. If the input *pb.TopicRequest Name field is
// non-nil, the specified topic is matched if it exists. Otherwise, all
// topics found in ZooKeeper are matched. Matched topics are then filtered
// by all tags specified, if specified, in the *pb.TopicRequest tag field.
func (s *Server) GetTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	// Get topics.
	fetchParams := fetchTopicSetParams{
		name:         req.Name,
		tags:         req.Tag,
		spanning:     req.Spanning,
		withReplicas: req.WithReplicas,
	}

	topics, err := s.fetchTopicSet(fetchParams)
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
	ctx, cancel, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	// Get topics.
	fetchParams := fetchTopicSetParams{
		name:         req.Name,
		tags:         req.Tag,
		spanning:     req.Spanning,
		withReplicas: req.WithReplicas,
	}

	topics, err := s.fetchTopicSet(fetchParams)
	if err != nil {
		return nil, err
	}

	// Populate the response Names field.
	resp := &pb.TopicResponse{Names: topics.Names()}

	return resp, nil
}

// ReassigningTopics returns a *pb.TopicResponse holding the names of all
// topics currently undergoing reassignment.
func (s *Server) ReassigningTopics(ctx context.Context, _ *pb.Empty) (*pb.TopicResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, nil, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	reassigning := s.ZK.GetReassignments()
	var names []string

	for t := range reassigning {
		names = append(names, t)
	}

	return &pb.TopicResponse{Names: names}, nil
}

// UnderReplicatedTopics returns a *pb.TopicResponse holding the names of all
// under replicated topics.
func (s *Server) UnderReplicatedTopics(ctx context.Context, _ *pb.Empty) (*pb.TopicResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, nil, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	reassigning, err := s.ZK.GetUnderReplicated()
	if err != nil {
		return nil, err
	}

	return &pb.TopicResponse{Names: reassigning}, nil
}

// CreateTopic creates a topic if it doesn't exist. Topic tags can optionally
// be set at topic creation time. Additionally, topics can be created on
// a target set of brokers by specifying the broker tag(s) in the request.
func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Empty, error) {
	empty := &pb.Empty{}

	ctx, cancel, err := s.ValidateRequest(ctx, req, writeRequest)
	if err != nil {
		return empty, err
	}

	if cancel != nil {
		defer cancel()
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

	if err := s.Locking.Lock(ctx); err != nil {
		return nil, err
	}
	defer s.Locking.UnlockLogError(ctx)

	// If we're targeting a specific set of brokers by tag, build
	// a replica assignment.
	var assignment kafkaadmin.ReplicaAssignment
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
	cfg := kafkaadmin.CreateTopicConfig{
		Name:              req.Topic.Name,
		Partitions:        int(req.Topic.Partitions),
		ReplicationFactor: int(req.Topic.Replication),
		Config:            req.Topic.Configs,
		ReplicaAssignment: kafkaadmin.ReplicaAssignment(assignment),
	}

	if err = s.kafkaadmin.CreateTopic(ctx, cfg); err != nil {
		// XXX: sometimes topics fail to create but no error is returned. One example
		// is if a replication higher than the number of available brokers is attempted.
		return empty, err
	}

	// Tag the topic. It's possible that we get a non-nil
	// but empty Tags parameter. In this case, we simply return.
	tags := TagSet(req.Topic.Tags).Tags()
	if len(tags) > 0 {
		reqParams.Tag = tags
		_, err = s.TagTopic(ctx, reqParams)
	}

	return empty, err
}

// DeleteTopic deletes the topic specified in the req.Topic.Name field.
func (s *Server) DeleteTopic(ctx context.Context, req *pb.TopicRequest) (*pb.Empty, error) {
	empty := &pb.Empty{}

	ctx, cancel, err := s.ValidateRequest(ctx, req, writeRequest)
	if err != nil {
		return empty, err
	}

	if cancel != nil {
		defer cancel()
	}

	if err := s.Locking.Lock(ctx); err != nil {
		return nil, err
	}
	defer s.Locking.UnlockLogError(ctx)

	if req.Name == "" {
		return nil, ErrTopicNameEmpty
	}

	// Ensure that the topic exists.
	reqParams := &pb.TopicRequest{Name: req.Name}
	resp, err := s.ListTopics(ctx, reqParams)
	if err != nil {
		return empty, err
	}

	if len(resp.Names) == 0 {
		return empty, ErrTopicNotExist
	}

	// Make the delete request.
	return empty, s.kafkaadmin.DeleteTopic(ctx, req.Name)
}

// TopicMappings returns all broker IDs that hold at least one partition for
// the requested topic. The topic is specified in the TopicRequest.Name
// field.
func (s *Server) TopicMappings(ctx context.Context, req *pb.TopicRequest) (*pb.BrokerResponse, error) {
	ctx, cancel, err := s.ValidateRequest(ctx, req, readRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
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
	ctx, cancel, err := s.ValidateRequest(ctx, req, writeRequest)
	if err != nil {
		return nil, err
	}

	if cancel != nil {
		defer cancel()
	}

	if req.Name == "" {
		return nil, ErrTopicNameEmpty
	}

	if len(req.Tag) == 0 {
		return nil, ErrNilTags
	}

	err = s.Locking.Lock(ctx)
	switch err {
	case nil:
		defer s.Locking.UnlockLogError(ctx)
	case zklocking.ErrAlreadyOwnLock:
		// Don't call unlock. We should be here because CreateTopic was called with
		// optional tags. We'll let the parent CreateTopic call finally issue unlock.
	default:
		return nil, err
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
	err = s.Tags.Store.DeleteTags(KafkaObject{Type: "topic", ID: req.Name}, Tags(req.Tag).Keys())
	if err != nil {
		return nil, err
	}

	return &pb.TagResponse{Message: "success"}, nil
}

// fetchTopicSet fetches metadata for all topics.
func (s *Server) fetchTopicSet(params fetchTopicSetParams) (TopicSet, error) {
	var topicRegex = []*regexp.Regexp{}

	// Check if a specific topic is being fetched.
	if params.name != "" {
		r := regexp.MustCompile(fmt.Sprintf("^%s$", params.name))
		topicRegex = append(topicRegex, r)
	} else {
		topicRegex = []*regexp.Regexp{allTopicsRegex}
	}

	// Fetch all topics from ZK.
	topics, errs := s.ZK.GetTopics(topicRegex)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	matched := TopicSet{}

	// Certain state-based topic requests will need broker info.
	var liveBrokers []uint32
	if params.spanning {
		brokers, err := s.fetchBrokerSet(&pb.BrokerRequest{})
		if err != nil {
			return nil, ErrFetchingTopics
		}
		liveBrokers = brokers.IDs()
	}

	// Populate all topics with state/config data.
	for _, t := range topics {
		// Get the topic state.
		st, _ := s.ZK.GetTopicState(t)
		// Get the topic configurations.
		c, err := s.ZK.GetTopicConfig(t)
		if err != nil {
			switch err.(type) {
			// ErrNoNode cases for a topic that exists just means that there hasn't been
			// non-default config specified for the topic.
			case kafkazk.ErrNoNode:
				c = &kafkazk.TopicConfig{}
				c.Config = map[string]string{}
			default:
				return nil, err
			}
		}

		// If we're filtering for "spanning" topics, now is a good time to check
		// since the full topic state is visible here.
		if params.spanning {
			// A simple check as to whether a topic satisfies the spanning property
			// is to request the set of brokers hosting its partitions; if the len
			// is not equal to the number of brokers in the cluster, it cannot be
			// considered spanning.
			if len(st.Brokers()) < len(liveBrokers) {
				continue
			}
		}

		replicaMap := make(map[uint32]*pb.Replicas)
		if params.withReplicas {
			for id, iReplicas := range st.Partitions {
				replicas := []uint32{}
				for _, r := range iReplicas {
					replicas = append(replicas, uint32(r))
				}
				// assuming that the data in zookeeper will be well-formated, ignoring error
				parsedId, _ := strconv.ParseUint(id, 10, 32)
				replicaMap[uint32(parsedId)] = &pb.Replicas{
					Ids: replicas,
				}
			}
		}

		// Add the topic to the TopicSet.
		matched[t] = &pb.Topic{
			Name:       t,
			Partitions: uint32(len(st.Partitions)),
			// TODO more sophisticated check than the
			// first partition len.
			Replication: uint32(len(st.Partitions["0"])),
			Configs:     c.Config,

			Replicas: replicaMap,
		}
	}

	// Returned filtered results by tag.
	filtered, err := s.Tags.FilterTopics(matched, params.tags)
	if err != nil {
		return nil, err
	}

	return filtered, nil
}

type fetchTopicSetParams struct {
	// If name is specified, only the metadata for this topic is returned.
	name string
	// Tags are used to filter the topics returned in the TopicSet. Matched topics
	// are those that have *all* key/value pairs specified.
	tags []string
	// Spanning is used to filter topics that satisfy the "spanning" property. A
	// topic is considered spanning if it has at least one partition on every broker
	// in the cluster.
	spanning bool
	// If withReplicas is true, the list of partitions with replicas is populated.
	withReplicas bool
}

// PartitionMapToReplicaAssignment takes a *kafkazk.PartitionMap and
// transforms it into an admin.ReplicaAssignment.
func PartitionMapToReplicaAssignment(pm *kafkazk.PartitionMap) kafkaadmin.ReplicaAssignment {
	ra := make(kafkaadmin.ReplicaAssignment, len(pm.Partitions))

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
