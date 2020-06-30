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
