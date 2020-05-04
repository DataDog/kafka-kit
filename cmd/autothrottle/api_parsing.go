package main

import (
	"errors"
	"net/http"
	"strconv"
  "log"
)

func parseRateParam(req *http.Request) (int, error) {
	r := req.URL.Query().Get("rate")
	rate, err := strconv.Atoi(r)

	switch {
	case r == "":
		return 0, errors.New("rate param must be supplied\n")
	case r == "0":
		return 0, errors.New("rate param must be >0\n")
	case err != nil:
		return 0, errors.New("rate param must be supplied as an integer\n")
	}

	return rate, nil
}

func parseAutoRemoveParam(req *http.Request) (bool, error) {
	c := req.URL.Query().Get("autoremove")
	var autoRemove bool
	var err error

	if c != "" {
		autoRemove, err = strconv.ParseBool(c)
		if err != nil {
			return autoRemove, errors.New("autoremove param must be a bool\n")
		}
	}

	return autoRemove, nil
}

func logReq(req *http.Request) {
	log.Printf("[API] %s %s %s\n", req.Method, req.RequestURI, req.RemoteAddr)
}
