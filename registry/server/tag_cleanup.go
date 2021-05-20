package server

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"
)

var TagMarkTimeKey = "tagMarkedForDeletionTime"
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
		err := s.MarkForDeletion(time.Now)
		if err != nil {
			log.Println(err)
			continue
		}

		s.DeleteStaleTags(time.Now, c)
	}
}

// MarkForDeletion marks stored tags that have been stranded without an associated kafka resource.
func (s *Server) MarkForDeletion(now func() time.Time) error {
	markTimeMinutes := fmt.Sprint(now().Unix())

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
			if _, exists := brokers[brokerId]; !exists {
				tagSet[TagMarkTimeKey] = markTimeMinutes
			} else {
				delete(tagSet, TagMarkTimeKey)
			}
		case "topic":
			if _, exists := topicSet[kafkaObject.ID]; !exists {
				tagSet[TagMarkTimeKey] = markTimeMinutes
			} else {
				delete(tagSet, TagMarkTimeKey)
			}
		}
		err := s.Tags.Store.SetTags(kafkaObject, tagSet) // Persist any changes
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteStaleTags deletes any tags that have not had a kafka resource associated with them.
func (s *Server) DeleteStaleTags(now func() time.Time, c Config) {
	sweepTime := now().Unix()
	allTags, _ := s.Tags.Store.GetAllTags()

	for kafkaObject, tags := range allTags {
		markTag, exists := tags[TagMarkTimeKey]
		if !exists {
			continue
		}

		markTime, err := strconv.Atoi(markTag)
		if err != nil {
			log.Printf("Found non timestamp tag %s in stale tag marker\n", markTag)
		}

		if sweepTime-int64(markTime) > int64(c.TagAllowedStalenessMinutes*60) {
			keys := make([]string, len(tags))
			i := 0
			for k := range tags {
				keys[i] = k
				i++
			}
			s.Tags.Store.DeleteTags(kafkaObject, keys)
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
