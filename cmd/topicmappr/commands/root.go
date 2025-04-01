package commands

import (
	"fmt"
	"os"

	"github.com/jamiealquiza/envy"
	"github.com/spf13/cobra"
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
	rootCmd.PersistentFlags().String("kafka-ssl-ca-location", "/etc/kafka/config/ca.crt", "Kafka ssl ca location")
	rootCmd.PersistentFlags().String("kafka-security-protocol", "SASL_SSL", "Kafka security protocol")
	rootCmd.PersistentFlags().String("kafka-sasl-mechanism", "PLAIN", "Kafka sasl mechanism")
	rootCmd.PersistentFlags().String("kafka-sasl-username", "", "Kafka sasl username")
	rootCmd.PersistentFlags().String("kafka-sasl-password", "", "Kafka sasl password")

	rootCmd.PersistentFlags().String("zk-addr", "localhost:2181", "ZooKeeper connect string")
	rootCmd.PersistentFlags().String("zk-prefix", "", "ZooKeeper prefix (if Kafka is configured with a chroot path prefix)")
	rootCmd.PersistentFlags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics")

	rootCmd.PersistentFlags().Bool("ignore-warns", false, "Produce a map even if warnings are encountered")
}
