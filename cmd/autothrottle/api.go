package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/mrmuggymuggy/kafka-kit/kafkazk"
)

// APIConfig holds configuration
// params for the admin API.
type APIConfig struct {
	Listen      string
	ZKPrefix    string
	RateSetting string
}

var (
	rateSettingsZNode = "override_rate"
	incorrectMethod   = "disallowed method\n"
)

func initAPI(c *APIConfig, zk kafkazk.Handler) {
	c.RateSetting = rateSettingsZNode

	p := fmt.Sprintf("/%s/%s", c.ZKPrefix, c.RateSetting)
	m := http.NewServeMux()

	// Check ZK for znode.
	exists, err := zk.Exists(p)
	if err != nil {
		log.Fatal(err)
	}

	if !exists {
		err = zk.Create("/"+c.ZKPrefix, "")
		if err != nil {
			log.Fatal(err)
		}
		err = zk.Create(p, "")
		if err != nil {
			log.Fatal(err)
		}
	}

	m.HandleFunc("/get_throttle", func(w http.ResponseWriter, req *http.Request) { getThrottle(w, req, zk, p) })
	m.HandleFunc("/set_throttle", func(w http.ResponseWriter, req *http.Request) { setThrottle(w, req, zk, p) })
	m.HandleFunc("/remove_throttle", func(w http.ResponseWriter, req *http.Request) { removeThrottle(w, req, zk, p) })

	go func() {
		err := http.ListenAndServe(c.Listen, m)
		if err != nil {
			log.Fatal(err)
		}
	}()
}

func getThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler, p string) {
	logReq(req)
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, incorrectMethod)
		return
	}

	t, err := zk.Get(p)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("Error getting throttle: %s\n", err.Error()))
		return
	}

	switch string(t) {
	case "":
		io.WriteString(w, "No throttle is set\n")
	default:
		resp := fmt.Sprintf("A throttle override is configured at %sMB/s\n", t)
		io.WriteString(w, resp)
	}
}

func setThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler, p string) {
	logReq(req)
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, incorrectMethod)
		return
	}

	rate := req.URL.Query().Get("rate")

	if rate == "" {
		io.WriteString(w, "Rate param must be supplied\n")
		return
	}

	if _, err := strconv.Atoi(rate); err != nil {
		io.WriteString(w, "Rate param must be supplied as an integer\n")
		return
	}

	err := zk.Set(p, rate)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("Error setting throttle: %s\n", err))
	} else {
		io.WriteString(w, fmt.Sprintf("Throttle successfully set to %sMB/s\n", rate))
	}
}

func removeThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler, p string) {
	logReq(req)
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, incorrectMethod)
		return
	}

	err := zk.Set(p, "")
	if err != nil {
		io.WriteString(w, fmt.Sprintf("Error setting throttle: %s\n", err))
	} else {
		io.WriteString(w, "Throttle successfully removed\n")
	}
}

func logReq(req *http.Request) {
	log.Printf("[API] %s %s %s\n", req.Method, req.RequestURI, req.RemoteAddr)
}
