package server

import (
	"context"
	regexp2 "regexp"
	"strconv"
	"time"
)

var tagMarkTimeKey = "tagMarkedForDeletionTSMin"
var allowedTagStalenessMinutes = 60 // todo: pass this through via config

// todo: should I handle errors, or just log?
// todo: what calls this function? A timer? Should anything happen if it fails?
// MarkForDeletion marks stored tags that have been stranded without an associated kafka resource.
func(s *Server) MarkForDeletion(ctx context.Context) error {
	markTimeMinutes := string(time.Now().Minute())

	// Get all brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return ErrFetchingBrokers
	}

	// Get all topics from ZK
	rgex, _ := regexp2.Compile(".*")
	regexes := []*regexp2.Regexp{rgex}
	topics, err := s.ZK.GetTopics(regexes)
	if err != nil {
		// todo
	}
	topicSet := TopicSetFromSlice(topics)

	allTags, _ := s.Tags.Store.GetAllTags()

	// Add a marker tag with timestamp to any dangling tagset whose associated kafka resource no longer exists.
	for kafkaObject, tagSet := range allTags {
		switch kafkaObject.Type {
		case "broker":
			brokerId, err := strconv.Atoi(kafkaObject.ID)
			if err != nil {
				// todo
			}
			if _, exists := brokers[brokerId]; exists {
				tagSet[tagMarkTimeKey] = markTimeMinutes
			}
		case "topic":
			if _, exists := topicSet[kafkaObject.ID]; exists {
				tagSet[tagMarkTimeKey] = markTimeMinutes
			}
		}
	}

	return nil
}

// DeleteStaleTags deletes any tags that have not had a kafka resource associated with them.
func(s *Server) DeleteStaleTags(ctx context.Context) {
	sweepTimeMinutes := time.Now().Minute()
	allTags, _ := s.Tags.Store.GetAllTags()

	for kafkaObject, tags := range allTags {
		markTag := tags[tagMarkTimeKey]
		markTime, err := strconv.Atoi(markTag)
		if err != nil {
			// todo
		}

		if sweepTimeMinutes - markTime < allowedTagStalenessMinutes {
			s.Tags.Store.DeleteTags(kafkaObject, tags.Tags())
		}
	}
}

// TopicSetFromSlice converts a slice into a TopicSet for convenience
func TopicSetFromSlice(s []string) TopicSet {
	var ts = TopicSet{}
	for _, t := range s {
		ts[t] = nil // TODO: I hope this works assigning nil...
	}
	return ts
}