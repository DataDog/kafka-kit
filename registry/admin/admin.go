package admin

// Client is an admin client.
type Client interface {
	Close()
	CreateTopic(CreateTopicConfig) error
}

// CreateTopicConfig holds CreateTopic parameters.
type CreateTopicConfig struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
}
