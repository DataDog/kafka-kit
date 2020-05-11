package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/DataDog/kafka-kit/kafkazk"
)

var errDeprecated = "WARN: this route is deprecated - refer to documentation\n"

func getThrottleDeprecated(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	logReq(req)
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeNLError(w, incorrectMethodError)
		return
	}

	r, err := fetchThrottleOverride(zk, overrideRateZnodePath)
	if err != nil {
		io.WriteString(w, err.Error())
		return
	}

	io.WriteString(w, errDeprecated)

	switch r.Rate {
	case 0:
		io.WriteString(w, "no throttle override is set\n")
	default:
		resp := fmt.Sprintf("a throttle override is configured at %dMB/s, autoremove==%v\n",
			r.Rate, r.AutoRemove)
		io.WriteString(w, resp)
	}
}

func setThrottleDeprecated(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	logReq(req)
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeNLError(w, incorrectMethodError)
		return
	}

	// Get rate param.

	r := req.URL.Query().Get("rate")
	var rate int
	var err error

	rate, err = strconv.Atoi(r)

	io.WriteString(w, errDeprecated)

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

	err = storeThrottleOverride(zk, overrideRateZnodePath, rateCfg)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("%s\n", err))
	} else {
		io.WriteString(w, fmt.Sprintf("throttle successfully set to %dMB/s, autoremove==%v\n",
			rate, remove))
	}
}

func removeThrottleDeprecated(w http.ResponseWriter, req *http.Request, zk kafkazk.Handler) {
	logReq(req)
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeNLError(w, incorrectMethodError)
		return
	}

	c := ThrottleOverrideConfig{
		Rate:       0,
		AutoRemove: false,
	}

	io.WriteString(w, errDeprecated)

	err := storeThrottleOverride(zk, overrideRateZnodePath, c)
	if err != nil {
		io.WriteString(w, fmt.Sprintf("%s\n", err))
	} else {
		io.WriteString(w, "throttle successfully removed\n")
	}
}
