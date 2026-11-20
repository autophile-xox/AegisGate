package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFaultPlanDecisions(t *testing.T) {
	plan := faultPlan{
		Seed: 42, DelayMS: 2, SpikeMS: 30, SpikeEvery: 4, SpikeLength: 1,
		BurstEvery: 4, BurstLength: 2, DropPermille: 300,
	}
	want := []decision{
		{32, true, false}, {2, true, true}, {2, false, false}, {2, false, false},
		{32, true, true}, {2, true, true}, {2, false, false}, {2, false, false},
	}
	for index, expected := range want {
		if got := plan.decide(uint64(index + 1)); got != expected {
			t.Errorf("request %d: got %+v, want %+v", index+1, got, expected)
		}
	}
	for _, invalid := range []faultPlan{
		{DelayMS: -1}, {SpikeEvery: 2, SpikeLength: 3}, {BurstLength: 1},
		{DropPermille: 1001}, {HealthStatus: 301},
	} {
		if invalid.validate() == nil {
			t.Errorf("accepted invalid plan %+v", invalid)
		}
	}
}

func TestControllerRunsFaultsAndRecoversOnSameAddress(t *testing.T) {
	sim := simulator{name: "test", address: "127.0.0.1:0"}
	if err := sim.apply(faultPlan{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sim.apply(faultPlan{Unavailable: true}) })
	address := sim.address
	base := "http://" + address
	control := httptest.NewServer(sim.control())
	t.Cleanup(control.Close)
	client := &http.Client{
		Timeout:   time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}

	get := func(path string, want int) *http.Response {
		t.Helper()
		response, err := client.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != want {
			t.Fatalf("GET %s: status %d, body %q, error %v", path, response.StatusCode, body, err)
		}
		return response
	}
	post := func(plan faultPlan) {
		t.Helper()
		data, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Post(control.URL+"/faults", "application/json", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("POST /faults: got %d", response.StatusCode)
		}
	}

	get("/health", 204)
	post(faultPlan{SpikeEvery: 4, SpikeLength: 1, SpikeMS: 25, BurstEvery: 4, BurstLength: 2})
	start := time.Now()
	first := get("/data", 503)
	if time.Since(start) < 25*time.Millisecond ||
		first.Header.Get("X-Backend-ID") != "test" || first.Header.Get("X-Sim-Sequence") != "1" {
		t.Fatalf("first burst response did not have the expected delay and identity: %v", first.Header)
	}
	get("/data", 503)
	get("/data", 200)

	post(faultPlan{DropPermille: 1000})
	if response, err := client.Get(base + "/data"); err == nil {
		response.Body.Close()
		t.Fatal("expected a TCP disconnect")
	}

	post(faultPlan{Unavailable: true})
	if connection, err := net.DialTimeout("tcp", address, time.Second); err == nil {
		connection.Close()
		t.Fatal("data listener accepted a connection while unavailable")
	}
	post(faultPlan{HealthStatus: 503})
	get("/health", 503)
	if value := get("/data", 200).Header.Get("X-Sim-Sequence"); value != "1" {
		t.Fatalf("expected a fresh sequence after recovery, got %q", value)
	}
	response, err := client.Get(control.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var stats counters
	if err := json.NewDecoder(response.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.Requests != 5 || stats.Unavailable != 2 || stats.Successes != 2 ||
		stats.Disconnects != 1 || stats.Health != 2 {
		t.Fatalf("unexpected cumulative counters: %+v", stats)
	}
}

func TestControlRejectsInvalidPayload(t *testing.T) {
	sim := simulator{address: "127.0.0.1:0"}
	handler := sim.control()
	for _, body := range []string{
		"{\"extra\": true}", "{\"drop_permille\": 1100}", "{}{}", strings.Repeat(" ", 4097),
	} {
		request := httptest.NewRequest(http.MethodPost, "/faults", strings.NewReader(body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Errorf("invalid POST received status %d", response.Code)
		}
	}
}
