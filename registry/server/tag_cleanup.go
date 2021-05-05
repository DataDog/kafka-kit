package server

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"
)

var tagMarkTimeKey = "tagMarkedForDeletionTSMin"
var topicRegex = regexp.MustCompile(".*")

type TagCleaner struct {
	running bool
}

// RunTagCleanup is regularly checks for tags that are stale and need clean up.
func (tc *TagCleaner) RunTagCleanup(s *Server, ctx context.Context, c Config) {
	tc.running = true

	// Interval timer.
	t := time.NewTicker(time.Duration(c.TagCleanupFrequencyMinutes) * time.Second)
	defer t.Stop()

	for tc.running {
		<-t.C
		s.MarkForDeletion()
		s.DeleteStaleTags(ctx, c)
	}
}

// MarkForDeletion marks stored tags that have been stranded without an associated kafka resource.
func(s *Server) MarkForDeletion() error {
	markTimeMinutes := fmt.Sprint(time.Now().Minute())

	// Get all brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return ErrFetchingBrokers
	}

	// Get all topics from ZK
	topics, err := s.ZK.GetTopics([]*regexp.Regexp{topicRegex})
	topicSet := TopicSetFromSlice(topics)
	if err != nil {
		return ErrFetchingTopics
	}

	allTags, err := s.Tags.Store.GetAllTags()
	if err != nil {
		return err
	}

	// Add a marker tag with timestamp to any dangling tagset whose associated kafka resource no longer exists.
	for kafkaObject, tagSet := range allTags {
		switch kafkaObject.Type {
		case "broker":
			brokerId, err := strconv.Atoi(kafkaObject.ID)
			if err != nil {
				log.Println(fmt.Printf("Found non int broker ID %s in tag cleanup", kafkaObject.ID))
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
func(s *Server) DeleteStaleTags(ctx context.Context, c Config) {
	sweepTimeMinutes := time.Now().Minute()
	allTags, _ := s.Tags.Store.GetAllTags()

	for kafkaObject, tags := range allTags {
		markTag := tags[tagMarkTimeKey]
		markTime, err := strconv.Atoi(markTag)
		if err != nil {
			log.Printf("Found non timestamp tag %s in stale tag marker\n", markTag)
		}

		if sweepTimeMinutes - markTime < c.TagAllowedStalenessMinutes {
			s.Tags.Store.DeleteTags(kafkaObject, tags.Tags())
		}
	}
}

// TopicSetFromSlice converts a slice into a TopicSet for convenience
func TopicSetFromSlice(s []string) TopicSet {
	var ts = TopicSet{}
	for _, t := range s {
		ts[t] = nil
	}
	return ts
}