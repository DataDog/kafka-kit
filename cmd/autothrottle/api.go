package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/DataDog/kafka-kit/kafkazk"
)

// APIConfig holds configuration params for the admin API.
type APIConfig struct {
	Listen      string
	ZKPrefix    string
	RateSetting string
}

var (
	overrideRateZnode     = "override_rate"
	overrideRateZnodePath string
	incorrectMethod       = "disallowed method\n"
)

func initAPI(c *APIConfig, zk kafkazk.Handler) {
	c.RateSetting = overrideRateZnode
	overrideRateZnodePath = fmt.Sprintf("/%s/%s", c.ZKPrefix, overrideRateZnode)

	m := http.NewServeMux()

	// Check ZK for override rate config znode.
	exists, err := zk.Exists(overrideRateZnodePath)
	if err != nil {
		log.Fatal(err)
	}

	if !exists {
		// Create chroot.
		err = zk.Create("/"+c.ZKPrefix, "")
		if err != nil {
			log.Fatal(err)
		}
		// Create overrideZKPath.
		err = zk.Create(overrideRateZnodePath, "")
		if err != nil {
			log.Fatal(err)
		}
	}

	// If the znode exists, check if it's using the legacy (non-json)
	// format. If it is, update it to the json format.
	// TODO(jamie): we can probably remove this by now.
	if exists {
		r, _ := zk.Get(overrideRateZnodePath)
		if rate, err := strconv.Atoi(string(r)); err == nil {
			// Populate the updated config.
			err := setThrottleOverride(zk, overrideRateZnodePath, ThrottleOverrideConfig{Rate: rate})
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

	urlPathTrimmed := strings.Trim(req.URL.Path, "/")
	urlPaths := strings.Split(urlPathTrimmed, "/")

	switch req.Method {
	// Get a throttle rate.
	case http.MethodGet:
		if len(urlPaths) < 2 {
			getGlobalThrottle(w, req, zk)
		} else {
			// getBrokerThrottle(w, req, zk)
		}
	// Set a throttle rate.
	case http.MethodPost:
		if len(urlPaths) < 2 {
			setGlobalThrottle(w, req, zk)
		}
	// Invalid method.
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, incorrectMethod)
		return
	}
}

// throttleRemove removes either the global or broker-specific throttle.
func throttleRemove(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	logReq(req)

	urlPathTrimmed := strings.Trim(req.URL.Path, "/")
	urlPaths := strings.Split(urlPathTrimmed, "/")

	switch req.Method {
	// Remove the throttle.
	case http.MethodPost:
		if len(urlPaths) < 3 {
			removeGlobalThrottle(w, req, zk)
		}
	// Invalid method.
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, incorrectMethod)
		return
	}
}

// getGlobalThrottle sets a throtle rate that applies to all brokers.
func getGlobalThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	r, err := getThrottleOverride(zk, overrideRateZnodePath)
	if err != nil {
		io.WriteString(w, err.Error())
		return
	}

	switch r.Rate {
	case 0:
		io.WriteString(w, "no throttle override is set\n")
	default:
		resp := fmt.Sprintf("a throttle override is configured at %dMB/s, autoremove==%v\n",
			r.Rate, r.AutoRemove)
		io.WriteString(w, resp)
	}
}

// setGlobalThrottle returns the throttle rate applied to all brokers.
func setGlobalThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	// Get rate param.
	r := req.URL.Query().Get("rate")
	var rate int
	var err error

	rate, err = strconv.Atoi(r)

	switch {
	case r == "":
		io.WriteString(w, "rate param must be supplied\n")
		return
	case r == "0":
		io.WriteString(w, "rate param must be >0\n")
		return
	case err != nil:
		io.WriteString(w, "rate param must be supplied as an integer\n")
		return
	}

	// Get automatic rate removal param.

	c := req.URL.Query().Get("autoremove")
	var remove bool

	if c != "" {
		remove, err = strconv.ParseBool(c)
		if err != nil {
			io.WriteString(w, "autoremove param must be a bool\n")
			return
		}
	}

	// Populate configs.

	rateCfg := ThrottleOverrideConfig{
		Rate:       rate,
		AutoRemove: remove,
	}

	err = setThrottleOverride(zk, overrideRateZnodePath, rateCfg)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("%s\n", err))
	} else {
		io.WriteString(w, fmt.Sprintf("throttle successfully set to %dMB/s, autoremove==%v\n",
			rate, remove))
	}
}

// removeGlobalThrottle removes the throttle rate applied to all brokers.
func removeGlobalThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	c := ThrottleOverrideConfig{
		Rate:       0,
		AutoRemove: false,
	}

	err := setThrottleOverride(zk, overrideRateZnodePath, c)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("%s\n", err))
	} else {
		io.WriteString(w, "throttle successfully removed\n")
	}
}

func logReq(req *http.Request) {
	log.Printf("[API] %s %s %s\n", req.Method, req.RequestURI, req.RemoteAddr)
}
