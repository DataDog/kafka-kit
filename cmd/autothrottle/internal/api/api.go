package api

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"

	"github.com/DataDog/kafka-kit/v4/cmd/autothrottle/internal/throttlestore"
	"github.com/DataDog/kafka-kit/v4/kafkazk"
)

// APIConfig holds configuration params for the admin API.
type APIConfig struct {
	Listen   string
	ZKPrefix string
}

var (
	overrideRateZnode     = "override_rate"
	OverrideRateZnodePath string
	incorrectMethodError  = errors.New("disallowed method")
)

func Init(c *APIConfig, zk kafkazk.Handler) {
	chroot := fmt.Sprintf("/%s", c.ZKPrefix)
	OverrideRateZnodePath = fmt.Sprintf("%s/%s", chroot, overrideRateZnode)

	m := http.NewServeMux()

	// Check ZK for override rate config znode.
	var exists bool
	for _, path := range []string{chroot, OverrideRateZnodePath} {
		var err error
		exists, err = zk.Exists(path)
		if err != nil {
			log.Fatal(err)
		}

		if !exists {
			// Create chroot.
			err = zk.Create(path, "")
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// If the znode exists, check if it's using the legacy (non-json) format.
	// If it is, update it to the json format.
	// TODO(jamie): we can probably remove this by now.
	if exists {
		r, _ := zk.Get(OverrideRateZnodePath)
		if rate, err := strconv.Atoi(string(r)); err == nil {
			// Populate the updated config.
			tor := throttlestore.ThrottleOverrideConfig{Rate: rate}
			err := throttlestore.StoreThrottleOverride(zk, OverrideRateZnodePath, tor)
			if err != nil {
				log.Fatal(err)
			}

			log.Println("Throttle override config format updated")
		}
	}

	// Routes. A global rate vs broker-specific rate is distinguished in whether
	// or not there's a trailing slash (and in a properly formed request, the
	// addition of a broker ID in the request path).
	m.HandleFunc("/throttle", func(w http.ResponseWriter, req *http.Request) { throttleGetSet(w, req, zk) })
	m.HandleFunc("/throttle/", func(w http.ResponseWriter, req *http.Request) { throttleGetSet(w, req, zk) })
	m.HandleFunc("/throttle/remove", func(w http.ResponseWriter, req *http.Request) { throttleRemove(w, req, zk) })
	m.HandleFunc("/throttle/remove/", func(w http.ResponseWriter, req *http.Request) { throttleRemove(w, req, zk) })

	// Start listener.
	go func() {
		err := http.ListenAndServe(c.Listen, m)
		if err != nil {
			log.Fatal(err)
		}
	}()
}

// throttleGetSet conditionally handles the request depending on the HTTP method.
func throttleGetSet(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	logReq(req)

	switch req.Method {
	case http.MethodGet:
		// Get a throttle rate.
		getThrottle(w, req, zk)
	case http.MethodPost:
		// Set a throttle rate.
		setThrottle(w, req, zk)
	default:
		// Invalid method.
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeNLError(w, incorrectMethodError)
		return
	}
}

// throttleRemove removes either the global, broker-specific throttle, or all broker-specific throttles.
func throttleRemove(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	logReq(req)

	switch req.Method {
	case http.MethodPost:
		// Remove the throttle.
		removeThrottle(w, req, zk)
	default:
		// Invalid method.
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeNLError(w, incorrectMethodError)
		return
	}
}

// getThrottle returns the throttle rate applied to all brokers.
func getThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	// Determine whether this is a global or broker-specific throttle lookup.
	var id string
	paths := parsePaths(req)
	if len(paths) > 1 {
		var err error
		id, err = brokerIDFromPath(req)
		if err != nil {
			writeNLError(w, err)
			return
		}
	}

	configPath := OverrideRateZnodePath

	// A non-0 ID means that this is broker specific.
	if id != "" {
		// Update the config path.
		configPath = fmt.Sprintf("%s/%s", configPath, id)
	}

	r, err := throttlestore.FetchThrottleOverride(zk, configPath)

	respMessage := fmt.Sprintf("a throttle override is configured at %dMB/s, autoremove==%v\n", r.Rate, r.AutoRemove)
	noOverrideMessage := "no throttle override is set\n"

	// Update the response message.
	if id != "" {
		respMessage = fmt.Sprintf("broker %s: %s", id, respMessage)
		noOverrideMessage = fmt.Sprintf("broker %s: %s", id, noOverrideMessage)
	}

	// Handle errors.
	if err != nil {
		switch err {
		case throttlestore.ErrNoOverrideSet:
			// Do nothing, let the rate condition handle
			// no override set messages.
		default:
			writeNLError(w, err)
			return
		}
	}

	switch r.Rate {
	case 0:
		io.WriteString(w, noOverrideMessage)
	default:
		io.WriteString(w, respMessage)
	}
}

// setThrottle sets a throtle rate that applies to all brokers.
func setThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	// Check rate param.
	rate, err := parseRateParam(req)
	if err != nil {
		writeNLError(w, err)
		return
	}

	// Check autoremove param.
	autoRemove, err := parseAutoRemoveParam(req)
	if err != nil {
		writeNLError(w, err)
		return
	}

	// Populate configs.
	rateCfg := throttlestore.ThrottleOverrideConfig{
		Rate:       rate,
		AutoRemove: autoRemove,
	}

	// Determine whether this is a global or broker-specific override.
	var id string
	paths := parsePaths(req)
	if len(paths) > 1 {
		id, err = brokerIDFromPath(req)
		if err != nil {
			writeNLError(w, err)
			return
		}
	}

	updateMessage := fmt.Sprintf("throttle successfully set to %dMB/s, autoremove==%v\n", rate, autoRemove)
	configPath := OverrideRateZnodePath

	writeOverride(w, id, configPath, updateMessage, err, zk, rateCfg)
}

// removeThrottle removes the throttle rate for a specific broker, the global rate, or for all brokers.
func removeThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	// Removing a rate means setting it to 0.
	c := throttlestore.ThrottleOverrideConfig{
		Rate:       0,
		AutoRemove: false,
	}

	// Determine whether this is a global or broker-specific throttle lookup.
	var id string
	paths := parsePaths(req)
	if len(paths) > 2 {
		var err error
		id, err = brokerIDFromPath(req)
		if err != nil {
			writeNLError(w, err)
			return
		}
	}

	configPath := OverrideRateZnodePath
	updateMessage := "throttle removed\n"

	var err error

	if id == "all" {
		// Instead of specifying a broker, the string 'all' means clear all overrides we have by setting to 0.
		var children []string
		var parentPath = OverrideRateZnodePath
		children, err = zk.Children(parentPath)

		sort.Strings(children)

		// iterate through all broker ids we have under the parent
		for _, childId := range children {
			if _, err := strconv.Atoi(childId); err != nil {
				var invalidBrokerMsg = fmt.Sprintf("invalid node %q is not an integer under path %q", childId, parentPath)
				io.WriteString(w, invalidBrokerMsg)
			}
			writeOverride(w, childId, configPath, updateMessage, err, zk, c)
		}
	} else {
		writeOverride(w, id, configPath, updateMessage, err, zk, c)
	}
}

func writeOverride(w http.ResponseWriter, id string, configPath string, updateMessage string, err error, zk kafkazk.Handler, c throttlestore.ThrottleOverrideConfig) {
	// A non-0 ID means that this is broker specific.
	if id != "" {
		configPath, updateMessage = formatConfigAndMessage(configPath, id, updateMessage)
	}

	err = throttlestore.StoreThrottleOverride(zk, configPath, c)

	if err != nil {
		switch err {
		case throttlestore.ErrNoOverrideSet:
			// Do nothing.
		default:
			writeNLError(w, err)
			return
		}
	}

	io.WriteString(w, updateMessage)
}

func formatConfigAndMessage(configPath string, id string, updateMessage string) (string, string) {
	configPath = fmt.Sprintf("%s/%s", configPath, id)
	updateMessage = fmt.Sprintf("broker %s: %s", id, updateMessage)
	return configPath, updateMessage
}
