package server

import "context"

func(s *Server) markForDeletion(ctx context.Context) error {

	// Get brokers from ZK.
	brokers, errs := s.ZK.GetAllBrokerMeta(false)
	if errs != nil {
		return ErrFetchingBrokers
	}

	for b, _ := range brokers  {
		tags, err := s.Tags.Store.GetTags(KafkaObject{ID: string(b), Type: "broker"})
	}

}
