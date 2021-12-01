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
	rootCmd.PersistentFlags().String("zk-addr", "localhost:2181", "ZooKeeper connect string")
	rootCmd.PersistentFlags().String("zk-prefix", "", "ZooKeeper prefix (if Kafka is configured with a chroot path prefix)")
	rootCmd.PersistentFlags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics")
	rootCmd.PersistentFlags().Bool("ignore-warns", false, "Produce a map even if warnings are encountered")
}
