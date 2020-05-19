package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

var errBrokerIDNotProvided = errors.New("broker ID not provided")

// parseRateParam takes a *http.Request and returns the specified
// 'rate' request parameter formatted as a int.
func parseRateParam(req *http.Request) (int, error) {
	r := req.URL.Query().Get("rate")
	rate, err := strconv.Atoi(r)

	switch {
	case r == "":
		return 0, errors.New("rate param must be specified")
	case r == "0":
		return 0, errors.New("rate param must be >0")
	case err != nil:
		return 0, errors.New("rate param must be supplied as an integer")
	}

	return rate, nil
}

// parseAutoRemoveParam takes a *http.Request and returns the specified
// autoremove parameter as a bool.
func parseAutoRemoveParam(req *http.Request) (bool, error) {
	c := req.URL.Query().Get("autoremove")
	var autoRemove bool
	var err error

	if c != "" {
		autoRemove, err = strconv.ParseBool(c)
		if err != nil {
			return autoRemove, errors.New("autoremove param must be a bool")
		}
	}

	return autoRemove, nil
}

// parsePaths takes a *http.Request and returns a []string elements of the full
// request path, stripped of all '/' chars.
func parsePaths(req *http.Request) []string {
	urlPathTrimmed := strings.Trim(req.URL.Path, "/")
	return strings.Split(urlPathTrimmed, "/")
}

// brokerIDFromPath takes a *http.Request and returns a broker ID from the
// path elements.
func brokerIDFromPath(req *http.Request) (int, error) {
	paths := parsePaths(req)
	if len(paths) < 2 {
		return 0, errBrokerIDNotProvided
	}

	var idStr string

	// If we're calling remove vs get/set, i.e. /throttle/remove/123
	// vs /throttle/123.
	if paths[1] == "remove" {
		if len(paths) < 3 {
			return 0, errBrokerIDNotProvided
		}
		// Path elements = [throttle, remove, 1230].
		idStr = paths[2]
	} else {
		// Path elements = [throttle, 1230].
		idStr = paths[1]
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		return 0, errors.New("broker param must be provided as integer")
	}

	return id, nil
}

// writeNLError writes the provided error with an appended newline to the
// provided http.ResponseWriter.
func writeNLError(w http.ResponseWriter, err error) {
	fmt.Fprintf(w, "%s\n", err)
}

// logReq logs *http.Request parameters.
func logReq(req *http.Request) {
	log.Printf("[API] %s %s %s\n", req.Method, req.RequestURI, req.RemoteAddr)
}
