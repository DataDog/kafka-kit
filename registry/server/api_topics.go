package server

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	zklocking "github.com/DataDog/kafka-kit/v4/cluster/zookeeper"
	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
	"github.com/DataDog/kafka-kit/v4/mapper"
	pb "github.com/DataDog/kafka-kit/v4/registry/registry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrFetchingTopics error.
	ErrFetchingTopics = status.Error(codes.Internal, "error fetching topics")
	// ErrTopicNotExist error.
	ErrTopicNotExist = status.Error(codes.NotFound, "topic does not exist")
	// ErrTopicNameEmpty error.
	ErrTopicNameEmpty = status.Error(codes.InvalidArgument, "topic Name field must be specified")
	// ErrTopicFieldMissing error.
	ErrTopicFieldMissing = status.Error(codes.InvalidArgument, "topic field missing in request body")
	// ErrTopicAlreadyExists error.
	ErrTopicAlreadyExists = status.Error(codes.AlreadyExists, "topic already exists")
	// ErrInsufficientBrokers error.
	ErrInsufficientBrokers = status.Error(codes.FailedPrecondition, "insufficient number of brokers")
	// ErrInvalidBrokerId error.
	ErrInvalidBrokerId = status.Error(codes.FailedPrecondition, "invalid broker id")
	// ErrTaggingTopicTimedOut
	ErrTaggingTopicTimedOut = status.Error(codes.DeadlineExceeded, "tagging topic timed out")
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

	topics, err := s.fetchTopicSet(ctx, fetchParams)
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

	topics, err := s.fetchTopicSet(ctx, fetchParams)
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

	// XXX(jamie): this is still read from ZooKeeper because the underlying
	// confluent-kafka-go client cannot differentiate reassigning and under-replicated
	// topics. See the kafkaadmin package UnderReplicatedTopics method comments.
	reassigning, err := s.ZK.ListReassignments()
	if err != nil {
		return nil, ErrFetchingTopics
	}

	return &pb.TopicResponse{Names: reassigning.List()}, nil
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

	underReplicated, err := s.kafkaadmin.UnderReplicatedTopics(ctx)
	if err != nil {
		return nil, err
	}

	return &pb.TopicResponse{Names: underReplicated.List()}, nil
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
	if req.TargetBrokerTags != nil || req.TargetBrokerIds != nil {
		// Create a stub map with the provided request dimensions.
		opts := mapper.Populate(
			req.Topic.Name,
			int(req.Topic.Partitions),
			int(req.Topic.Replication),
		)
		pMap := mapper.NewPartitionMap(opts)
		// Fetch brokers by tag. If no tag was specified, this will return all the brokers:
		reqParams := &pb.BrokerRequest{Tag: req.TargetBrokerTags}
		resp, err := s.ListBrokers(ctx, reqParams)
		if err != nil {
			return empty, err
		}
		type Empty struct{}
		selectedBrokerIds := make(map[uint32]Empty)
		existingBrokers := make(map[uint32]Empty)

		// Populate a map of existing brokers returned by the ListBrokers call:
		for _, id := range resp.Ids {
			existingBrokers[id] = Empty{}
		}

		for _, id := range req.TargetBrokerIds {
			selectedBrokerIds[id] = Empty{}
		}
		// Validate that the passed broker ids actually exists or were part of the returned brokers.
		// If that's not the case that's either because the specified broker id does not exist or does not
		// contain passed broker tags. In this case return ErrInvalidBrokerId error.
		if len(selectedBrokerIds) > 0 {
			for id := range selectedBrokerIds {
				if _, ok := existingBrokers[id]; !ok {
					return nil, ErrInvalidBrokerId
				}
			}
		}
		var targetBrokerIDs []int
		for brokerId := range existingBrokers {
			// If we have specified a list of selected brokers, only consider that broker if it's in that list:
			if len(selectedBrokerIds) > 0 {
				if _, ok := selectedBrokerIds[brokerId]; ok {
					targetBrokerIDs = append(targetBrokerIDs, int(brokerId))
				}
			} else {
				// We haven't specified explicit broker ids. Select returned broker in that case:
				targetBrokerIDs = append(targetBrokerIDs, int(brokerId))
			}
		}

		if len(targetBrokerIDs) < int(req.Topic.Replication) {
			return empty, ErrInsufficientBrokers
		}

		// Create a stub BrokerMap.
		bMap := mapper.NewBrokerMap()

		// Get the live broker metadata.
		brokerStates, err := s.kafkaadmin.DescribeBrokers(ctx, false)
		if err != nil {
			return empty, ErrFetchingBrokers
		}

		bmm, err := mapper.BrokerMetaMapFromStates(brokerStates)
		if err != nil {
			return empty, ErrFetchingBrokers
		}

		// Update the BrokerMap with the target broker list.
		// XXX we don't catch any errors here, such as provided
		// brokers being marked as missing. This is because we're
		// only ever using brokers we just fetched from the cluster
		// state as opposed to user provided lists. Other scenarios
		// may need to be covered, however.
		bMap.Update(targetBrokerIDs, bmm)

		// Rebuild the stub map with the discovered target broker list.
		rebuildParams := mapper.RebuildParams{
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

	// Tag the topic. It's possible that we get a non-nil but empty Tags parameter.
	// In this case, we simply return.
	tags := TagSet(req.Topic.Tags).Tags()
	if len(tags) > 0 {
		reqParams.Tag = tags
		if err := s.tagTopicWithRetries(ctx, reqParams); err != nil {
			return empty, err
		}
	}

	return empty, nil
}

// tagTopicWithRetries is an exponential backoff/retry wrapper for tagging topics.
func (s *Server) tagTopicWithRetries(ctx context.Context, req *pb.TopicRequest) error {
	// There's occasionally a lag period between creating the topic and the topic
	// being visible; start a backoff retry loop that maxes at:
	// - 5s wait intervals
	// - 10 retries
	// - returns at the context timeout
	delay := 250 * time.Millisecond
	maxDelay := 5 * time.Second
	maxRetries := 100

	var err error

	// Start the loop.
	for i := 0; i < maxRetries; i++ {
		// Stop running if the context has already expired.
		select {
		case <-ctx.Done():
			return ErrTaggingTopicTimedOut
		default:
		}

		// Attempt the topic tagging.
		_, err = s.TagTopic(ctx, req)
		if err != nil {
			// Approach the max wait.
			delay = delay * time.Duration(i)
			if delay > maxDelay {
				delay = maxDelay
			}
			// Sleep until the next interval.
			time.Sleep(delay)
			continue
		}
		// Success.
		return nil
	}

	return err
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

	// Get the topic state.
	tState, err := s.kafkaadmin.DescribeTopics(ctx, []string{req.Name})
	switch err {
	case nil:
	case kafkaadmin.ErrNoData:
		return nil, ErrTopicNotExist
	default:
		return nil, err
	}

	// Translate it to a mapper object.
	pm, _ := mapper.PartitionMapFromTopicStates(tState)

	// Get a mapper.BrokerMap from the PartitionMap.
	bm := mapper.BrokerMapFromPartitionMap(pm, nil, false)

	// Get all brokers as a []int of IDs.
	allf := func(*mapper.Broker) bool { return true }
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
	_, err = s.kafkaadmin.DescribeTopics(ctx, []string{req.Name})
	switch err {
	case nil:
	case kafkaadmin.ErrNoData:
		return nil, ErrTopicNotExist
	default:
		return nil, err
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
	_, err = s.kafkaadmin.DescribeTopics(ctx, []string{req.Name})
	switch err {
	case nil:
	case kafkaadmin.ErrNoData:
		return nil, ErrTopicNotExist
	default:
		return nil, err
	}

	// Delete the tags.
	err = s.Tags.Store.DeleteTags(KafkaObject{Type: "topic", ID: req.Name}, Tags(req.Tag).Keys())
	if err != nil {
		return nil, err
	}

	return &pb.TagResponse{Message: "success"}, nil
}

// fetchTopicSet fetches metadata for all topics.
func (s *Server) fetchTopicSet(ctx context.Context, params fetchTopicSetParams) (TopicSet, error) {
	var topicRegexStrings = []string{}

	// Check if a specific topic is being fetched.
	if params.name != "" {
		r := fmt.Sprintf("^%s$", params.name)
		topicRegexStrings = append(topicRegexStrings, r)
	} else {
		topicRegexStrings = []string{".*"}
	}

	// Fetch topic(s) from cluster.
	topics, err := s.kafkaadmin.DescribeTopics(ctx, topicRegexStrings)
	switch err {
	case nil:
	case kafkaadmin.ErrNoData:
		return TopicSet{}, nil
	default:
		return nil, err
	}

	results := TopicSet{}

	// Certain state-based topic requests will need broker info.
	var liveBrokers []uint32
	if params.spanning {
		brokers, err := s.fetchBrokerSet(ctx, &pb.BrokerRequest{})
		if err != nil {
			log.Printf("fetchTopicSet: %s\n", err)
			return nil, ErrFetchingTopics
		}
		liveBrokers = brokers.IDs()
	}

	// Get topic dynamic configs.
	configs, err := s.kafkaadmin.GetDynamicConfigs(ctx, "topic", topics.List())
	if err != nil {
		log.Printf("fetchTopicSet: %s\n", err)
		return nil, ErrFetchingTopics
	}

	// Populate all topics with state/config data.
	for topic, topicState := range topics {
		// Check if we're filtering for spanning topics.
		if params.spanning {
			// A simple check as to whether a topic satisfies the spanning property
			// is to request the set of brokers hosting its partitions; if the len
			// is not equal to the number of brokers in the cluster, it cannot be
			// considered spanning.
			if len(topicState.Brokers()) < len(liveBrokers) {
				continue
			}
		}

		// If requested, fetch assigned partition replicas.
		replicaMap := make(map[uint32]*pb.Replicas)
		if params.withReplicas {
			for _, partnState := range topicState.PartitionStates {
				var replicas []uint32
				for _, r := range partnState.Replicas {
					replicas = append(replicas, uint32(r))
				}
				replicaMap[uint32(partnState.ID)] = &pb.Replicas{
					Ids: replicas,
				}
			}
		}

		var topicConfigs = map[string]string{}
		if cfg, exist := configs[topic]; exist {
			topicConfigs = cfg
		}

		// Add the topic to the TopicSet.
		results[topic] = &pb.Topic{
			Name:        topic,
			Partitions:  uint32(topicState.Partitions),
			Replication: uint32(topicState.ReplicationFactor),
			Configs:     topicConfigs,
			Replicas:    replicaMap,
		}
	}

	// Returned filtered results by tag. Note that tag population is also
	// handled here.
	filtered, err := s.Tags.FilterTopics(results, params.tags)
	if err != nil {
		log.Printf("fetchTopicSet: %s\n", err)
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

// PartitionMapToReplicaAssignment takes a *mapper.PartitionMap and
// transforms it into an admin.ReplicaAssignment.
func PartitionMapToReplicaAssignment(pm *mapper.PartitionMap) kafkaadmin.ReplicaAssignment {
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
