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
	t := time.NewTicker(time.Duration(c.TagCleanupFrequencyMinutes) * time.Minute)
	defer t.Stop()

	for tc.running {
		<-t.C

		if err := s.Locking.Lock(ctx); err != nil {
			log.Println(err)
			continue
		}
		defer s.Locking.UnlockLogError(ctx)

		err := s.MarkForDeletion(time.Now)
		if err != nil {
			log.Println(err)
			continue
		}

		s.DeleteStaleTags(time.Now, c)
	}
}

// MarkForDeletion marks stored tags that have been stranded without an associated
// kafka resource.
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
			tagSet[TagMarkTimeKey] = markTimeMinutes
			if err := s.Tags.Store.SetTags(kafkaObject, tagSet); err != nil {
				log.Printf("failed to update TagSet for %s %s: %s\n", kafkaObject.Type, kafkaObject.ID, err)
			}
			continue
		}

		// Otherwise, the object exists and we should remove the marker if it were
		// previously set.
		if marked {
			if err := s.Tags.Store.DeleteTags(kafkaObject, []string{TagMarkTimeKey}); err != nil {
				log.Printf("failed to remove TagMarkTimeKey tag for %s %s: %s\n", kafkaObject.Type, kafkaObject.ID, err)
			}
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
			log.Printf("found non timestamp tag %s in stale tag marker\n", markTag)
		}

		if sweepTime-int64(markTime) > int64(c.TagAllowedStalenessMinutes*60) {
			keys := tags.Keys()
			s.Tags.Store.DeleteTags(kafkaObject, keys)

			log.Printf("deleted tags for non-existent %s %s\n", kafkaObject.Type, kafkaObject.ID)
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
