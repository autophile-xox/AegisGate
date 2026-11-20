package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var payload = strings.Repeat("aegisgate", 16)

type counters struct {
	Requests    uint64 `json:"requests"`
	Successes   uint64 `json:"successes"`
	Unavailable uint64 `json:"responses_503"`
	Disconnects uint64 `json:"disconnects"`
	Cancelled   uint64 `json:"cancelled"`
	Health      uint64 `json:"health_requests"`
}

type simulator struct {
	name string

	mu       sync.Mutex
	address  string
	data     *http.Server
	listener net.Listener
	plan     faultPlan
	sequence uint64
	counts   counters
}

func (s *simulator) apply(plan faultPlan) error {
	if err := plan.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case plan.Unavailable && s.data != nil:
		// Close the listener and every existing data connection. The control listener stays up.
		_ = s.data.Close()
		_ = s.listener.Close()
		s.data = nil
		s.listener = nil
	case !plan.Unavailable && s.data == nil:
		listener, err := net.Listen("tcp", s.address)
		if err != nil {
			return err
		}
		s.address = listener.Addr().String()
		server := &http.Server{
			Handler:           http.HandlerFunc(s.serveData),
			ReadHeaderTimeout: 2 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       30 * time.Second,
			MaxHeaderBytes:    16 * 1024,
		}
		s.data = server
		s.listener = listener
		go func() {
			if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
				log.Printf("data listener: %v", err)
			}
		}()
	}
	s.plan = plan
	s.sequence = 0
	return nil
}

func (s *simulator) serveData(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if r.URL.Path == "/health" {
		s.counts.Health++
		status := s.plan.HealthStatus
		s.mu.Unlock()
		if status == 0 {
			status = http.StatusNoContent
		}
		w.WriteHeader(status)
		return
	}
	s.sequence++
	sequence := s.sequence
	result := s.plan.decide(sequence)
	s.counts.Requests++
	s.mu.Unlock()

	if result.delayMS > 0 {
		timer := time.NewTimer(time.Duration(result.delayMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			s.mu.Lock()
			s.counts.Cancelled++
			s.mu.Unlock()
			return
		case <-timer.C:
		}
	}
	if result.drop {
		connection, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			http.Error(w, "TCP close unavailable", http.StatusInternalServerError)
			return
		}
		if tcp, ok := connection.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = connection.Close()
		s.mu.Lock()
		s.counts.Disconnects++
		s.mu.Unlock()
		return
	}
	w.Header().Set("X-Backend-ID", s.name)
	w.Header().Set("X-Sim-Sequence", strconv.FormatUint(sequence, 10))
	status := http.StatusOK
	if result.reject {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	_, err := io.WriteString(w, payload)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err != nil:
		s.counts.Cancelled++
	case result.reject:
		s.counts.Unavailable++
	default:
		s.counts.Successes++
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (s *simulator) control() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /faults", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		plan := s.plan
		s.mu.Unlock()
		writeJSON(w, plan)
	})
	mux.HandleFunc("POST /faults", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		var plan faultPlan
		if err := decoder.Decode(&plan); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "expected one fault plan", http.StatusBadRequest)
			return
		}
		if err := plan.validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.apply(plan); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, plan)
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		stats := s.counts
		s.mu.Unlock()
		writeJSON(w, stats)
	})
	return mux
}

func loopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	return err == nil && net.ParseIP(host).IsLoopback()
}

func run() error {
	listen := flag.String("listen", "127.0.0.1:9000", "HTTP data address")
	control := flag.String("control", "127.0.0.1:9100", "loopback fault controller address")
	name := flag.String("name", "backend", "value of the X-Backend-ID response header")
	seed := flag.Uint64("seed", 1, "initial fault seed")
	delay := flag.Int("delay-ms", 0, "initial response delay in milliseconds")
	flag.Parse()
	if !loopback(*control) {
		return errors.New("control must be a numeric loopback IP and port")
	}
	if *name == "" || len(*name) > 64 ||
		strings.IndexFunc(*name, func(r rune) bool { return r < ' ' || r > '~' }) >= 0 {
		return errors.New("name must be a single non-empty HTTP header value of up to 64 bytes")
	}

	listener, err := net.Listen("tcp", *control)
	if err != nil {
		return err
	}
	defer listener.Close()
	sim := simulator{name: *name, address: *listen}
	if err := sim.apply(faultPlan{Seed: *seed, DelayMS: *delay}); err != nil {
		return err
	}
	dataAddress := sim.address
	defer sim.apply(faultPlan{Unavailable: true})
	server := &http.Server{
		Handler: sim.control(), ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 10 * time.Second,
	}
	defer server.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	fmt.Printf("upstream %s ready data=%s control=%s\n", *name, dataAddress, listener.Addr())
	select {
	case <-ctx.Done():
		return nil
	case err := <-served:
		return err
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
