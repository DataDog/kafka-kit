package main

import (
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
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

func parsePaths(req *http.Request) []string {
	urlPathTrimmed := strings.Trim(req.URL.Path, "/")
	return strings.Split(urlPathTrimmed, "/")
}

func brokerIDFromPath(req *httpRequest) (int, error) {
	paths := parsePaths(req)
	if len(paths) < 2 {
		return 0, errors.New("broker ID not provided\n")
	}

	id, err := strconv.Atoi(paths[1])
	if err != nil {
		return 0, errors.New("broker param must be provided as integer")
	}

	return id, nil
}

func logReq(req *http.Request) {
	log.Printf("[API] %s %s %s\n", req.Method, req.RequestURI, req.RemoteAddr)
}
