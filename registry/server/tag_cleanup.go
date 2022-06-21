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
	tickerPeriod := time.Duration(c.TagCleanupFrequencyMinutes) * time.Minute
	log.Printf("Setting up tag clean up to run every %d seconds\n", tickerPeriod)
	t := time.NewTicker(tickerPeriod)
	defer t.Stop()

	for tc.running {
		<-t.C

		log.Println("running tag cleanup")
		if err := s.MarkForDeletion(ctx, time.Now); err != nil {
			log.Println("error marking tags for deletion: ", err)
			continue
		}

		if err := s.DeleteStaleTags(ctx, time.Now, c); err != nil {
			log.Println("error deleting stale tags: ", err)
		}
	}
}

// MarkForDeletion marks stored tags that have been stranded without an associated
// kafka resource.
func (s *Server) MarkForDeletion(ctx context.Context, now func() time.Time) error {
	markTimeSeconds := fmt.Sprint(now().Unix())

	// Get all brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return ErrFetchingBrokers
	}

	// Lock.
	if err := s.Locking.Lock(ctx); err != nil {
		return err
	}
	defer s.Locking.UnlockLogError(ctx)

	// Get all topics from ZK
	topics, err := s.ZK.GetTopics([]*regexp.Regexp{topicRegex})
	topicSet := TopicSetFromSlice(topics)
	if err != nil {
		return ErrFetchingTopics
	}

	// Get all tag sets.
	allTags, err := s.Tags.Store.GetAllTags()
	if err != nil {
		return err
	}

	// Add a marker tag with timestamp to any dangling tagset whose associated kafka
	// resource no longer exists.
	for kafkaObject, tagSet := range allTags {
		var objectExists bool

		// Check whether the object currently exists.
		switch kafkaObject.Type {
		case "broker":
			brokerId, err := strconv.Atoi(kafkaObject.ID)
			if err != nil {
				log.Printf("found non int broker ID %s in tag cleanup\n", kafkaObject.ID)
				continue
			}
			_, objectExists = brokers[brokerId]
		case "topic":
			_, objectExists = topicSet[kafkaObject.ID]
		}

		// Check if the object has already been marked.
		_, marked := tagSet[TagMarkTimeKey]

		// If the object doesn't exist and hasn't already been marked, do so.
		if !objectExists && !marked {
			log.Printf("marking %s:%s for cleanup\n", kafkaObject.Type, kafkaObject.ID)
			tagSet[TagMarkTimeKey] = markTimeSeconds
			if err := s.Tags.Store.SetTags(kafkaObject, tagSet); err != nil {
				log.Printf("failed to update TagSet for %s %s: %s\n", kafkaObject.Type, kafkaObject.ID, err)
			}
			continue
		}

		// Otherwise, the object exists and we should remove the marker if it were
		// previously set.
		if marked {
			log.Printf("unmarking existing %s:%s to avoid cleanup\n", kafkaObject.Type, kafkaObject.ID)
			if err := s.Tags.Store.DeleteTags(kafkaObject, []string{TagMarkTimeKey}); err != nil {
				log.Printf("failed to remove %s tag for %s %s: %s\n", TagMarkTimeKey, kafkaObject.Type, kafkaObject.ID, err)
			}
		}
	}

	return nil
}

// DeleteStaleTags deletes any tags that have not had a kafka resource associated with them.
func (s *Server) DeleteStaleTags(ctx context.Context, now func() time.Time, c Config) error {
	sweepTimeSeconds := now().Unix()
	stalenessSeconds := int64(c.TagAllowedStalenessMinutes * 60)

	// Lock.
	if err := s.Locking.Lock(ctx); err != nil {
		return err
	}
	defer s.Locking.UnlockLogError(ctx)

	allTags, _ := s.Tags.Store.GetAllTags()

	for kafkaObject, tags := range allTags {
		markTag, exists := tags[TagMarkTimeKey]
		if !exists {
			continue
		}

		markTimeSeconds, err := strconv.Atoi(markTag)
		if err != nil {
			log.Printf("found non timestamp tag %s in stale tag marker\n", markTag)
		}

		log.Printf("evaluating clean up of %s:%s marked at %d\n", kafkaObject.Type, kafkaObject.ID, markTimeSeconds)
		if sweepTimeSeconds-int64(markTimeSeconds) > stalenessSeconds {
			keys := tags.Keys()
			s.Tags.Store.DeleteTags(kafkaObject, keys)

			log.Printf("deleted tags for non-existent %s %s\n", kafkaObject.Type, kafkaObject.ID)
		}
	}

	return nil
}

// TopicSetFromSlice converts a slice into a TopicSet for convenience
func TopicSetFromSlice(s []string) TopicSet {
	var ts = TopicSet{}
	for _, t := range s {
		ts[t] = nil
	}
	return ts
}
