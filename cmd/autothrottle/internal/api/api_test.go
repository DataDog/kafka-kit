package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

func TestSetThrottle(t *testing.T) {
	// GIVEN
	zk := kafkazk.NewZooKeeperStub()
	req, err := http.NewRequest("POST", "/throttle?rate=5&autoremove=false", nil)
	if err != nil {
		t.Fatal(err)
	}

	responseRecorder := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleGetSet(w, req, zk) })

	// WHEN
	handler.ServeHTTP(responseRecorder, req)

	// THEN
	checkResults(http.StatusOK, "throttle successfully set to 5MB/s, autoremove==false\n", responseRecorder, t)
}

func TestSetBrokerThrottle(t *testing.T) {
	// GIVEN
	zk := kafkazk.NewZooKeeperStub()
	req, err := http.NewRequest("POST", "/throttle/123?rate=5&autoremove=false", nil)
	if err != nil {
		t.Fatal(err)
	}

	responseRecorder := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleGetSet(w, req, zk) })

	// WHEN
	handler.ServeHTTP(responseRecorder, req)

	// THEN
	checkResults(http.StatusOK, "broker 123: throttle successfully set to 5MB/s, autoremove==false\n", responseRecorder, t)
}

func TestGetThrottle(t *testing.T) {
	// GIVEN
	overrideRateZnode = "override_rate"
	zk := kafkazk.NewZooKeeperStub()

	setReq, err := http.NewRequest("POST", "/throttle?rate=5&autoremove=false", nil)
	getReq, err := http.NewRequest("GET", "/throttle", nil)
	if err != nil {
		t.Fatal(err)
	}

	setRecorder := httptest.NewRecorder()
	getRecorder := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleGetSet(w, req, zk) })

	// WHEN
	handler.ServeHTTP(setRecorder, setReq)
	handler.ServeHTTP(getRecorder, getReq)

	// THEN
	checkResults(http.StatusOK, "a throttle override is configured at 5MB/s, autoremove==false\n", getRecorder, t)
}

func TestGetBrokerThrottle(t *testing.T) {
	// GIVEN
	overrideRateZnode = "override_rate"
	zk := kafkazk.NewZooKeeperStub()

	setReq, err := http.NewRequest("POST", "/throttle/123?rate=5&autoremove=false", nil)
	getReq, err := http.NewRequest("GET", "/throttle/123", nil)
	if err != nil {
		t.Fatal(err)
	}

	setRecorder := httptest.NewRecorder()
	getRecorder := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleGetSet(w, req, zk) })

	// WHEN
	handler.ServeHTTP(setRecorder, setReq)
	handler.ServeHTTP(getRecorder, getReq)

	// THEN
	checkResults(http.StatusOK, "broker 123: a throttle override is configured at 5MB/s, autoremove==false\n", getRecorder, t)
}

func TestRemoveThrottle(t *testing.T) {
	// GIVEN
	overrideRateZnode = "override_rate"
	zk := kafkazk.NewZooKeeperStub()

	setReq, err := http.NewRequest("POST", "/throttle?rate=5&autoremove=false", nil)
	removeReq, err := http.NewRequest("POST", "/throttle/remove", nil)
	getReq, err := http.NewRequest("GET", "/throttle", nil)
	if err != nil {
		t.Fatal(err)
	}

	setRecorder := httptest.NewRecorder()
	getRecorder := httptest.NewRecorder()
	removeRecorder := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleGetSet(w, req, zk) })
	removeHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleRemove(w, req, zk) })

	// WHEN
	handler.ServeHTTP(setRecorder, setReq)
	removeHandler.ServeHTTP(removeRecorder, removeReq)
	handler.ServeHTTP(getRecorder, getReq)

	// THEN
	checkResults(http.StatusOK, "throttle removed\n", removeRecorder, t)
	checkResults(http.StatusOK, "no throttle override is set\n", getRecorder, t)
}

func TestRemoveBrokerThrottle(t *testing.T) {
	// GIVEN
	overrideRateZnode = "override_rate"
	zk := kafkazk.NewZooKeeperStub()

	setReq, err := http.NewRequest("POST", "/throttle/123?rate=5&autoremove=false", nil)
	removeReq, err := http.NewRequest("POST", "/throttle/remove/123", nil)
	getReq, err := http.NewRequest("GET", "/throttle/123", nil)
	if err != nil {
		t.Fatal(err)
	}

	setRecorder := httptest.NewRecorder()
	getRecorder := httptest.NewRecorder()
	removeRecorder := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleGetSet(w, req, zk) })
	removeHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleRemove(w, req, zk) })

	// WHEN
	handler.ServeHTTP(setRecorder, setReq)
	removeHandler.ServeHTTP(removeRecorder, removeReq)
	handler.ServeHTTP(getRecorder, getReq)

	// THEN
	checkResults(http.StatusOK, "broker 123: throttle removed\n", removeRecorder, t)
	checkResults(http.StatusOK, "broker 123: no throttle override is set\n", getRecorder, t)
}

func TestRemoveAllBrokerThrottle(t *testing.T) {
	// GIVEN
	overrideRateZnode = "override_rate"
	OverrideRateZnodePath = fmt.Sprintf("%s/%s", "zkChroot", overrideRateZnode)
	zk := kafkazk.NewZooKeeperStub()

	setReq, err := http.NewRequest("POST", "/throttle/123?rate=5&autoremove=false", nil)
	setReq2, err := http.NewRequest("POST", "/throttle/456?rate=10&autoremove=false", nil)
	removeReq, err := http.NewRequest("POST", "/throttle/remove/all", nil)
	getReq, err := http.NewRequest("GET", "/throttle/123", nil)
	getReq2, err := http.NewRequest("GET", "/throttle/456", nil)
	if err != nil {
		t.Fatal(err)
	}

	setRecorder := httptest.NewRecorder()
	getRecorder := httptest.NewRecorder()
	getRecorder2 := httptest.NewRecorder()
	removeRecorder := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleGetSet(w, req, zk) })
	removeHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { throttleRemove(w, req, zk) })

	// WHEN
	handler.ServeHTTP(setRecorder, setReq)
	handler.ServeHTTP(setRecorder, setReq2)
	removeHandler.ServeHTTP(removeRecorder, removeReq)
	handler.ServeHTTP(getRecorder, getReq)
	handler.ServeHTTP(getRecorder2, getReq2)

	// THEN
	checkResults(http.StatusOK, "broker 123: throttle removed\nbroker 456: throttle removed\n", removeRecorder, t)
	checkResults(http.StatusOK, "broker 123: no throttle override is set\n", getRecorder, t)
	checkResults(http.StatusOK, "broker 456: no throttle override is set\n", getRecorder2, t)
}

func checkResults(statusCode int, expectedMessage string, rr *httptest.ResponseRecorder, t *testing.T) {
	if status := rr.Code; status != statusCode {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, statusCode)
	}

	// Check the response body is what we expect.
	if rr.Body.String() != expectedMessage {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expectedMessage)
	}
}
