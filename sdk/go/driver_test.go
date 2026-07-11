package vmon

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ws "github.com/coder/websocket"
)

type meshRoundTripFunc func(*http.Request) (*http.Response, error)

func (function meshRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestMeshDriverTransportFailoverStickyCooldownAndAllFail(t *testing.T) {
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32
	now := time.Unix(1_000, 0)
	client := &http.Client{Transport: meshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Host {
		case "first.test":
			firstCalls.Add(1)
			return nil, errors.New("connection refused")
		case "second.test":
			secondCalls.Add(1)
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok")), Request: request}, nil
		default:
			return nil, errors.New("unexpected endpoint")
		}
	})}
	driver, err := newMeshDriver(
		DSNConfig{Endpoints: []string{"http://first.test", "http://second.test"}, Timeout: time.Second},
		withMeshHTTPClient(client),
		withMeshClock(func() time.Time { return now }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	for range 2 {
		response, endpoint, requestErr := driver.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/healthz"})
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		response.Body.Close()
		if endpoint != "http://second.test" {
			t.Fatalf("endpoint = %q", endpoint)
		}
	}
	response, endpoint, err := driver.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/healthz", Endpoint: "http://first.test"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if endpoint != "http://second.test" {
		t.Fatalf("cooldown endpoint = %q", endpoint)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 3 {
		t.Fatalf("calls first=%d second=%d; affinity did not respect cooldown or success was not sticky", firstCalls.Load(), secondCalls.Load())
	}
	info := driver.Endpoints()
	if info[0].Healthy || !info[1].Healthy {
		t.Fatalf("endpoint health = %#v", info)
	}
	now = now.Add(endpointCooldown)
	info = driver.Endpoints()
	if !info[0].Healthy {
		t.Fatalf("endpoint remained unhealthy after cooldown: %#v", info[0])
	}
	response, endpoint, err = driver.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/healthz", Endpoint: "http://first.test"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if endpoint != "http://second.test" || firstCalls.Load() != 2 {
		t.Fatalf("expired cooldown endpoint=%q first calls=%d", endpoint, firstCalls.Load())
	}

	failing := &http.Client{Transport: meshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, errors.New(request.URL.Host)
	})}
	allFail, err := newMeshDriver(DSNConfig{Endpoints: []string{"http://one.test", "http://two.test"}, Timeout: time.Second}, withMeshHTTPClient(failing))
	if err != nil {
		t.Fatal(err)
	}
	defer allFail.Close()
	_, _, err = allFail.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/healthz"})
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Endpoint != "http://two.test" {
		t.Fatalf("all-fail error = %T %v", err, err)
	}
}

func TestMeshDriverHTTPStatusDoesNotFailOverAndAffinityWins(t *testing.T) {
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32
	client := &http.Client{Transport: meshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusInternalServerError
		if request.URL.Host == "first.test" {
			firstCalls.Add(1)
		} else {
			secondCalls.Add(1)
			status = http.StatusTeapot
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("status")), Request: request}, nil
	})}
	driver, err := newMeshDriver(DSNConfig{Endpoints: []string{"http://first.test", "http://second.test"}, Timeout: time.Second}, withMeshHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	response, endpoint, err := driver.Do(context.Background(), DriverRequest{Method: http.MethodPost, Path: "/x", Endpoint: "http://second.test"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if endpoint != "http://second.test" || response.StatusCode != http.StatusTeapot || firstCalls.Load() != 0 || secondCalls.Load() != 1 {
		t.Fatalf("endpoint=%q status=%d calls=%d/%d", endpoint, response.StatusCode, firstCalls.Load(), secondCalls.Load())
	}
}

func TestMeshDriverCreatesTransportsLazily(t *testing.T) {
	client := &http.Client{Transport: meshRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("ok")),
			Request:    request,
		}, nil
	})}
	driver, err := newMeshDriver(
		DSNConfig{Endpoints: []string{"http://first.test", "http://second.test"}, Timeout: time.Second},
		withMeshHTTPClient(client),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	if len(driver.transports) != 0 {
		t.Fatalf("transports created eagerly: %d", len(driver.transports))
	}
	_ = driver.Endpoints()
	if len(driver.transports) != 0 {
		t.Fatalf("Endpoints created transports: %d", len(driver.transports))
	}
	response, _, err := driver.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/healthz"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if len(driver.transports) != 1 {
		t.Fatalf("transports after first request=%d, want 1", len(driver.transports))
	}
}

func TestMeshDriverStreamSurvivesConfiguredTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		time.Sleep(75 * time.Millisecond)
		_, _ = io.WriteString(response, "event")
	}))
	defer server.Close()
	driver, err := newMeshDriver(DSNConfig{Endpoints: []string{server.URL}, Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()

	_, _, err = driver.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/events"})
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("ordinary request error=%T %v, want TransportError", err, err)
	}
	response, _, err := driver.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/events", Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "event" {
		t.Fatalf("body=%q", body)
	}
}

func TestMeshDriverLazyDiscoveryMergeAndFailover(t *testing.T) {
	var seedURL string
	var peerURL string
	peer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/mesh/status" {
			response.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(response, `{"self":{"advertise":"`+peerURL+`"},"peers":[]}`)
			return
		}
		_, _ = io.WriteString(response, "peer")
	}))
	defer peer.Close()
	peerURL = peer.URL
	seed := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/mesh/status" {
			response.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(response, `{"self":{"advertise":"`+seedURL+`"},"peers":[{"advertise":"`+peerURL+`"}]}`)
			return
		}
		_, _ = io.WriteString(response, "seed")
	}))
	seedURL = seed.URL
	driver, err := newMeshDriver(DSNConfig{Endpoints: []string{seed.URL}, Discover: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	response, endpoint, err := driver.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/work"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if endpoint != seed.URL || len(driver.Endpoints()) != 2 || driver.Endpoints()[1].Source != "discovered" {
		t.Fatalf("discovered roster = %#v", driver.Endpoints())
	}
	seed.Close()
	response, endpoint, err = driver.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/work"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if endpoint != peer.URL {
		t.Fatalf("failover endpoint = %q, want %q", endpoint, peer.URL)
	}
}

func TestMeshDriverResolveSandbox(t *testing.T) {
	missing := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(response, `{"code":"not_found","message":"missing"}`)
	}))
	defer missing.Close()
	found := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(response, `{}`)
	}))
	defer found.Close()
	driver, err := newMeshDriver(DSNConfig{Endpoints: []string{missing.URL, found.URL}, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	endpoint, err := driver.ResolveSandbox(context.Background(), "box/a", missing.URL)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != found.URL {
		t.Fatalf("resolved endpoint = %q", endpoint)
	}
}

func TestMeshDriverUDSHTTPAndWebSocket(t *testing.T) {
	socketFile, err := os.CreateTemp("", "vmon-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	socketPath := socketFile.Name()
	if err := socketFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(socketPath) })
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/http":
			_, _ = io.WriteString(response, "uds")
		case "/ws":
			connection, acceptErr := ws.Accept(response, request, nil)
			if acceptErr != nil {
				return
			}
			defer connection.CloseNow()
			messageType, payload, readErr := connection.Read(request.Context())
			if readErr == nil {
				_ = connection.Write(request.Context(), messageType, payload)
			}
		default:
			http.NotFound(response, request)
		}
	})}
	go server.Serve(listener)
	defer server.Close()

	driver, err := newMeshDriver(DSNConfig{Endpoints: []string{"vmon+unix://" + socketPath}, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	response, endpoint, err := driver.Do(context.Background(), DriverRequest{Method: http.MethodGet, Path: "/http"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || string(body) != "uds" || endpoint != "vmon+unix://"+socketPath {
		t.Fatalf("UDS HTTP endpoint=%q body=%q err=%v", endpoint, body, err)
	}
	socket, endpoint, err := driver.Dial(context.Background(), "/ws", url.Values{"x": {"1"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	if err := socket.Write(context.Background(), WebSocketBinaryMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	messageType, payload, err := socket.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "vmon+unix://"+socketPath || messageType != WebSocketBinaryMessage || string(payload) != "hello" {
		t.Fatalf("UDS WS endpoint=%q type=%v payload=%q", endpoint, messageType, payload)
	}
}
