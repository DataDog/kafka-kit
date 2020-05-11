package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/DataDog/kafka-kit/kafkazk"
)

// APIConfig holds configuration params for the admin API.
type APIConfig struct {
	Listen   string
	ZKPrefix string
}

var (
	overrideRateZnode     = "override_rate"
	overrideRateZnodePath string
	incorrectMethodError  = errors.New("disallowed method")
)

func initAPI(c *APIConfig, zk kafkazk.Handler) {
	chroot := fmt.Sprintf("/%s", c.ZKPrefix)
	overrideRateZnodePath = fmt.Sprintf("%s/%s", chroot, overrideRateZnode)

	m := http.NewServeMux()

	// Check ZK for override rate config znode.
	var exists bool
	for _, path := range []string{chroot, overrideRateZnodePath} {
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
		r, _ := zk.Get(overrideRateZnodePath)
		if rate, err := strconv.Atoi(string(r)); err == nil {
			// Populate the updated config.
			err := storeThrottleOverride(zk, overrideRateZnodePath, ThrottleOverrideConfig{Rate: rate})
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

	// Deprecated routes.
	m.HandleFunc("/get_throttle", func(w http.ResponseWriter, req *http.Request) { getThrottleDeprecated(w, req, zk) })
	m.HandleFunc("/set_throttle", func(w http.ResponseWriter, req *http.Request) { setThrottleDeprecated(w, req, zk) })
	m.HandleFunc("/remove_throttle", func(w http.ResponseWriter, req *http.Request) { removeThrottleDeprecated(w, req, zk) })

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

// throttleRemove removes either the global or broker-specific throttle.
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

// getThrottle sets a throtle rate that applies to all brokers.
func getThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	// Determine whether this is a global or broker-specific throttle lookup.
	var id int
	paths := parsePaths(req)
	if len(paths) > 1 {
		var err error
		id, err = brokerIDFromPath(req)
		if err != nil {
			writeNLError(w, err)
			return
		}
	}

	configPath := overrideRateZnodePath

	// A non-0 ID means that this is broker specific.
	if id != 0 {
		// Update the config path.
		configPath = fmt.Sprintf("%s/%d", configPath, id)
	}

	r, err := fetchThrottleOverride(zk, configPath)

	respMessage := fmt.Sprintf("a throttle override is configured at %dMB/s, autoremove==%v\n", r.Rate, r.AutoRemove)
	noOverrideMessage := "no throttle override is set\n"

	// Update the response message.
	if id != 0 {
		respMessage = fmt.Sprintf("broker %d: %s", id, respMessage)
		noOverrideMessage = fmt.Sprintf("broker %d: %s", id, noOverrideMessage)
	}

	// Handle errors.
	if err != nil {
		switch err {
		case errNoOverideSet:
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

// setThrottle returns the throttle rate applied to all brokers.
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
	rateCfg := ThrottleOverrideConfig{
		Rate:       rate,
		AutoRemove: autoRemove,
	}

	// Determine whether this is a global or broker-specific override.
	var id int
	paths := parsePaths(req)
	if len(paths) > 1 {
		id, err = brokerIDFromPath(req)
		if err != nil {
			writeNLError(w, err)
			return
		}
	}

	updateMessage := fmt.Sprintf("throttle successfully set to %dMB/s, autoremove==%v\n", rate, autoRemove)
	configPath := overrideRateZnodePath

	// A non-0 ID means that this is broker specific.
	if id != 0 {
		// Update the message, config path.
		updateMessage = fmt.Sprintf("broker %d: %s", id, updateMessage)
		configPath = fmt.Sprintf("%s/%d", configPath, id)
	}

	// Set the config.
	err = storeThrottleOverride(zk, configPath, rateCfg)
	if err != nil {
		writeNLError(w, err)
		return
	} else {
		io.WriteString(w, updateMessage)
	}
}

// removeThrottle removes the throttle rate applied to all brokers.
func removeThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	c := ThrottleOverrideConfig{
		Rate:       0,
		AutoRemove: false,
	}

	// Determine whether this is a global or broker-specific throttle lookup.
	var id int
	paths := parsePaths(req)
	if len(paths) > 2 {
		var err error
		id, err = brokerIDFromPath(req)
		if err != nil {
			writeNLError(w, err)
			return
		}
	}

	configPath := overrideRateZnodePath
	updateMessage := "throttle removed\n"

	// A non-0 ID means that this is broker specific.
	if id != 0 {
		configPath = fmt.Sprintf("%s/%d", configPath, id)
		updateMessage = fmt.Sprintf("broker %d: %s", id, updateMessage)
	}

	// Removing a rate means setting it to 0.
	err := storeThrottleOverride(zk, configPath, c)
	if err != nil {
		switch err {
		case errNoOverideSet:
			// Do nothing.
		default:
			writeNLError(w, err)
			return
		}
	}

	io.WriteString(w, updateMessage)
}
