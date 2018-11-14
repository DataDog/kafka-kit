package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "topicmappr",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("zk-addr", "localhost:2181", "ZooKeeper connect string (for broker metadata or rebuild-topic lookups) [TOPICMAPPR_ZK_ADDR]")
	rootCmd.PersistentFlags().String("zk-prefix", "", "ZooKeeper namespace prefix (for Kafka brokers) [TOPICMAPPR_ZK_PREFIX]")
}
