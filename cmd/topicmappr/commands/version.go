package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

// This can be set with
// -ldflags "-X github.com/DataDog/kafka-kit/cmd/topicmappr/commands.version=x.x.x"
var version = "0.0.0"

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version)
	},
}
