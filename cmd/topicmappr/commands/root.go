package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "topicmappr",
	Short: "short",
	Long:  `long`,
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	RootCmd.Flags().String("zk-addr", "localhost:2181", "ZooKeeper connect string (for broker metadata or rebuild-topic lookups)")
	RootCmd.Flags().String("zk-prefix", "", "ZooKeeper namespace prefix (for Kafka brokers)")
	RootCmd.Flags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix (for Kafka metrics)")
}
