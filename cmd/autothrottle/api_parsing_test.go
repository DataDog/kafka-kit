package main

import (
	"fmt"
	"net/http"
	"testing"
)

func TestParseRateParam(t *testing.T) {
	// Test all 4 conditions in parseRateParam.
	var requests = [4]*http.Request{}

	// Populate the requests array.
	for i, rate := range []string{"", "0", "text", "100"} {
		url := fmt.Sprintf("http://localhost?rate=%s", rate)
		req, _ := http.NewRequest("POST", url, nil)
		requests[i] = req
	}

	type response struct {
		rate int
		err  error
	}

	expected := []response{
		{
			rate: 0,
			err:  errRateParamUnspecified,
		},
		{
			rate: 0,
			err:  errRateParamIsZero,
		},
		{
			rate: 0,
			err:  errRateParamNotInt,
		},
		{
			rate: 100,
			err:  nil,
		},
	}

	for i := range requests {
		rate, err := parseRateParam(requests[i])

		if rate != expected[i].rate {
			t.Errorf("Expected rate '%d', got '%d'", expected[i].rate, rate)
		}

		if err != expected[i].err {
			t.Errorf("Expected error '%s', got '%s'", expected[i].err, err)
		}
	}
}

func TestParseAutoRemoveParam(t *testing.T) {
	var requests = [3]*http.Request{}

	url := "http://localhost?autoremove=text"
	req, _ := http.NewRequest("POST", url, nil)
	requests[0] = req

	url = fmt.Sprintf("http://localhost?autoremove=%v", true)
	req, _ = http.NewRequest("POST", url, nil)
	requests[1] = req

	url = fmt.Sprintf("http://localhost?autoremove=%v", false)
	req, _ = http.NewRequest("POST", url, nil)
	requests[2] = req

	type response struct {
		autoRemove bool
		err        error
	}

	expected := []response{
		{
			autoRemove: false,
			err:        errAutoRemoveNotBool,
		},
		{
			autoRemove: true,
			err:        nil,
		},
		{
			autoRemove: false,
			err:        nil,
		},
	}

	for i := range requests {
		autoRemove, err := parseAutoRemoveParam(requests[i])

		if autoRemove != expected[i].autoRemove {
			t.Errorf("Expected bool '%v', got '%v'", expected[i].autoRemove, autoRemove)
		}

		if err != expected[i].err {
			t.Errorf("Expected error '%s', got '%s'", expected[i].err, err)
		}
	}
}

func TestParsePaths(t *testing.T) {
	url := "http://localhost/throttle/add"
	req, _ := http.NewRequest("POST", url, nil)

	out := parsePaths(req)
	expected := []string{"throttle", "add"}

	if len(out) != len(expected) {
		t.Fatalf("Expected len '%d', got '%d'", len(expected), len(out))
	}

	for i := range expected {
		if out[i] != expected[i] {
			t.Errorf("Expected output '%v', got '%v'", expected, out)
			break
		}
	}
}

func TestBrokerIDFromPath(t *testing.T) {
	expected := "1234"

	// Test path for adding throttle.
	url := fmt.Sprintf("http://localhost/throttle/%s", expected)
	req, _ := http.NewRequest("POST", url, nil)

	out, _ := brokerIDFromPath(req)

	if out != expected {
		t.Errorf("Expected broker ID '%s', got '%s'", expected, out)
	}

	// Test path for removing throttle.
	url = fmt.Sprintf("http://localhost/throttle/remove/%s", expected)
	req, _ = http.NewRequest("POST", url, nil)

	out, _ = brokerIDFromPath(req)

	if out != expected {
		t.Errorf("Expected broker ID '%s', got '%s'", expected, out)
	}
}
