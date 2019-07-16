package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/DataDog/kafka-kit/kafkazk"
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

	// Check ZK for override rate config znode.
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

	// If the znode exists, check if it's using the legacy (non-json)
	// format. If it is, update it to the json format.
	if exists {
		r, _ := zk.Get(p)
		if rate, err := strconv.Atoi(string(r)); err == nil {
			// Populate the updated config.
			err := setThrottleOverride(zk, p, ThrottleOverrideConfig{Rate: rate})
			if err != nil {
				log.Fatal(err)
			}

			log.Println("Throttle override config format updated")
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

	r, err := getThrottleOverride(zk, p)
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

func setThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler, p string) {
	logReq(req)
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, incorrectMethod)
		return
	}

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

	err = setThrottleOverride(zk, p, rateCfg)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("%s\n", err))
	} else {
		io.WriteString(w, fmt.Sprintf("throttle successfully set to %dMB/s, autoremove==%v\n",
			rate, remove))
	}
}

func removeThrottle(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler, p string) {
	logReq(req)
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, incorrectMethod)
		return
	}

	c := ThrottleOverrideConfig{
		Rate:       0,
		AutoRemove: false,
	}

	err := setThrottleOverride(zk, p, c)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("%s\n", err))
	} else {
		io.WriteString(w, "throttle successfully removed\n")
	}
}

func logReq(req *http.Request) {
	log.Printf("[API] %s %s %s\n", req.Method, req.RequestURI, req.RemoteAddr)
}
