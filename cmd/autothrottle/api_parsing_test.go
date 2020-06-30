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
		response{
			rate: 0,
			err:  errRateParamUnspecified,
		},
		response{
			rate: 0,
			err:  errRateParamIsZero,
		},
		response{
			rate: 0,
			err:  errRateParamNotInt,
		},
		response{
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
		response{
			autoRemove: false,
			err:        errAutoRemoveNotBool,
		},
		response{
			autoRemove: true,
			err:        nil,
		},
		response{
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
