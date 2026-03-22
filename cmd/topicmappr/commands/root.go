package commands

import (
	"fmt"
	"os"

	"github.com/jamiealquiza/envy"
	"github.com/spf13/cobra"
	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
)

var rootCmd = &cobra.Command{
	Use: "topicmappr",
}

// Execute rootCmd.
func Execute() {
	envy.ParseCobra(rootCmd, envy.CobraConfig{Prefix: "TOPICMAPPR", Persistent: true, Recursive: false})

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("kafka-addr", "localhost:9092", "Kafka bootstrap address")
	rootCmd.PersistentFlags().String("kafka-ssl-ca-location", "", "Kafka SSL CA certificate location")
	rootCmd.PersistentFlags().String("kafka-security-protocol", "", "Kafka security protocol")
	rootCmd.PersistentFlags().String("kafka-sasl-mechanism", "", "Kafka SASL mechanism")
	rootCmd.PersistentFlags().String("kafka-sasl-username", "", "Kafka SASL username")
	rootCmd.PersistentFlags().String("kafka-sasl-password", "", "Kafka SASL password")
	rootCmd.PersistentFlags().String("zk-addr", "localhost:2181", "ZooKeeper connect string")
	rootCmd.PersistentFlags().String("zk-prefix", "", "ZooKeeper prefix (if Kafka is configured with a chroot path prefix)")
	rootCmd.PersistentFlags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics")
	rootCmd.PersistentFlags().Bool("ignore-warns", false, "Produce a map even if warnings are encountered")
}

func newKafkaAdminClient(cmd *cobra.Command) (kafkaadmin.KafkaAdmin, error) {
	bs, _ := cmd.Flags().GetString("kafka-addr")
	ca, _ := cmd.Flags().GetString("kafka-ssl-ca-location")
	sec, _ := cmd.Flags().GetString("kafka-security-protocol")
	mech, _ := cmd.Flags().GetString("kafka-sasl-mechanism")
	user, _ := cmd.Flags().GetString("kafka-sasl-username")
	pass, _ := cmd.Flags().GetString("kafka-sasl-password")

	cfg := kafkaadmin.Config{
		BootstrapServers: bs,
		SSLCALocation:    ca,
		SecurityProtocol: sec,
		SASLMechanism:    mech,
		SASLUsername:     user,
		SASLPassword:     pass,
	}

	return kafkaadmin.NewClient(cfg)
}
