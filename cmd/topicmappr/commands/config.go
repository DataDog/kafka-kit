package commands

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/kafka-kit/v4/kafkazk"

	"github.com/spf13/cobra"
)

const (
	indent = "\x20\x20"
	div    = 1 << 30
)

var (
	// Characters allowed in Kafka topic names
	topicNormalChar = regexp.MustCompile(`[a-zA-Z0-9_\\-]`)
)

func sanitizeInput(cmd *cobra.Command) {
	// Append trailing slash if not included.
	op := cmd.Flag("out-path").Value.String()
	if op != "" && !strings.HasSuffix(op, "/") {
		cmd.Flags().Set("out-path", op+"/")
	}
}

// topicRegex takes a string of csv values and returns a []*regexp.Regexp.
// The values are either a string literal and become ^value$ or are regex and
// compiled then added.
func topicRegex(s string) []*regexp.Regexp {
	var out []*regexp.Regexp

	// Update string literals to ^value$ regex.
	topicNames := strings.Split(s, ",")
	for n, t := range topicNames {
		if !containsRegex(t) {
			topicNames[n] = fmt.Sprintf(`^%s$`, t)
		}
	}

	// Compile regex patterns.
	for _, t := range topicNames {
		r, err := regexp.Compile(t)
		if err != nil {
			fmt.Printf("Invalid topic regex: %s\n", t)
			os.Exit(1)
		}

		out = append(out, r)
	}

	return out
}

// initZooKeeper inits a ZooKeeper connection if one is needed.
// Scenarios that would require a connection:
//  - the --use-meta flag is true (default), which requests
//    that broker metadata (such as rack ID or registration liveness).
//  - that topics were specified via --topics, which requires
//    topic discovery` via ZooKeeper.
//  - that the --placement flag was set to 'storage', which expects
//    metrics metadata to be stored in ZooKeeper.
func initZooKeeper(zkAddr, kafkaPrefix, metricsPrefix string) (kafkazk.Handler, error) {
	// Suppress underlying ZK client noise.
	log.SetOutput(ioutil.Discard)

	zk, err := kafkazk.NewHandler(&kafkazk.Config{
		Connect:       zkAddr,
		Prefix:        kafkaPrefix,
		MetricsPrefix: metricsPrefix,
	})

	if err != nil {
		return nil, fmt.Errorf("Error connecting to ZooKeeper: %s", err)
	}

	timeout := 250 * time.Millisecond
	time.Sleep(timeout)

	if !zk.Ready() {
		return nil, fmt.Errorf("Failed to connect to ZooKeeper %s within %s", zkAddr, timeout)
	}

	return zk, nil
}

// containsRegex takes a topic name reference and returns whether or not
// it should be interpreted as regex.
func containsRegex(t string) bool {
	// Check each character of the topic name. If it doesn't contain a legal Kafka
	// topic name character, we're going to assume it's regex.
	for _, c := range t {
		if !topicNormalChar.MatchString(string(c)) {
			return true
		}
	}

	return false
}

func brokerStringToSlice(s string) []int {
	ids := map[int]bool{}
	var info int

	parts := strings.Split(s, ",")
	var is []int

	// Iterate and convert each broker ID.
	for _, p := range parts {
		i, err := strconv.Atoi(strings.TrimSpace(p))
		// Err and exit on bad input.
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if ids[i] {
			fmt.Printf("ID %d supplied as duplicate, excluding\n", i)
			info++
			continue
		}

		ids[i] = true
		is = append(is, i)
	}

	// Formatting purposes.
	if info > 0 {
		fmt.Println()
	}

	return is
}

func defaultsAndExit() {
	fmt.Println()
	os.Exit(1)
}
