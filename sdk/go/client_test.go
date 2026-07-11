package vmon

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientSandboxFlow(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := make(map[string]int)
	handlerErr := make(chan error, 1)
	report := func(err error) {
		select {
		case handlerErr <- err:
		default:
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-token" {
			report(fmt.Errorf("authorization = %q", request.Header.Get("Authorization")))
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		key := request.Method + " " + request.URL.EscapedPath()
		mu.Lock()
		calls[key]++
		mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		switch key {
		case "POST /v1/sandboxes":
			body, err := io.ReadAll(request.Body)
			if err != nil {
				report(err)
				return
			}
			expected := `{"image":"alpine","name":"box-1","env":{"A":"1","Z":"2"},"secrets":[{"name":"runtime","values":{"API_KEY":"hidden"}}],"volumes":{"/data":"data"}}`
			if string(body) != expected {
				report(fmt.Errorf("create body = %s; want %s", body, expected))
			}
			writer.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(writer, sandboxJSON("box-1", "running", nil))
		case "GET /v1/sandboxes/box-1":
			_, _ = io.WriteString(writer, sandboxJSON("box-1", "running", nil))
		case "POST /v1/sandboxes/box-1/exec":
			var body struct {
				Command []string `json:"cmd"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				report(err)
				return
			}
			if strings.Join(body.Command, " ") != "printf hello" {
				report(fmt.Errorf("exec command = %q", body.Command))
			}
			_, _ = fmt.Fprintf(
				writer,
				`{"exit":0,"stdout_b64":%q,"stderr_b64":%q}`,
				base64.StdEncoding.EncodeToString([]byte("hello")),
				base64.StdEncoding.EncodeToString([]byte("warn")),
			)
		case "PUT /v1/sandboxes/box-1/files":
			body, err := io.ReadAll(request.Body)
			if err != nil {
				report(err)
				return
			}
			if string(body) != "payload" || request.URL.Query().Get("path") != "/tmp/input.json" {
				report(fmt.Errorf("file write body=%q query=%q", body, request.URL.RawQuery))
			}
			_, _ = io.WriteString(writer, `{"ok":true}`)
		case "POST /v1/sandboxes/box-1/terminate":
			_, _ = io.WriteString(writer, sandboxJSON("box-1", "terminated", int64Pointer(0)))
		case "DELETE /v1/sandboxes/box-1":
			_, _ = io.WriteString(writer, sandboxJSON("box-1", "terminated", int64Pointer(0)))
		case "GET /v1/sandboxes/missing":
			writer.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(writer, `{"code":"not_found","message":"sandbox missing"}`)
		default:
			writer.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(writer, `{"code":"unexpected","message":"unexpected test route"}`)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithToken("test-token"))
	if err != nil {
		t.Fatal(err)
	}
	secretValues := map[string]string{"API_KEY": "hidden"}
	secret, err := NewSecret("runtime", secretValues)
	if err != nil {
		t.Fatal(err)
	}
	secretValues["API_KEY"] = "mutated"
	volume, err := NewVolume("data")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	sandbox, err := client.CreateSandbox(ctx, SandboxCreateRequest{
		Image:   "alpine",
		Name:    "box-1",
		Env:     map[string]string{"Z": "2", "A": "1"},
		Secrets: []Secret{secret},
		Volumes: map[string]VolumeMount{"/data": volume.Mount(false)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sandbox.ID != "box-1" || sandbox.Identifier() != "box-1" {
		t.Fatalf("created sandbox = %#v", sandbox)
	}
	if strings.Contains(fmt.Sprint(secret), "hidden") {
		t.Fatal("secret String exposed a value")
	}

	got, err := client.GetSandbox(ctx, "box-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "running" {
		t.Fatalf("get status = %q", got.Status)
	}
	poll, err := client.PollSandbox(ctx, "box-1")
	if err != nil {
		t.Fatal(err)
	}
	if !poll.Exists || poll.Done {
		t.Fatalf("poll = %#v", poll)
	}
	result, err := sandbox.ExecCapture(ctx, ExecRequest{Command: []string{"printf", "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 0 || string(result.Stdout) != "hello" || string(result.Stderr) != "warn" {
		t.Fatalf("exec result = %#v", result)
	}
	if err := sandbox.WriteFile(ctx, "/tmp/input.json", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := sandbox.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if sandbox.Status != "terminated" {
		t.Fatalf("terminate status = %q", sandbox.Status)
	}
	if err := sandbox.Remove(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = client.GetSandbox(ctx, "missing")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("missing error type = %T (%v)", err, err)
	}
	if apiErr.StatusCode != http.StatusNotFound || apiErr.Code != "not_found" || apiErr.Message != "sandbox missing" {
		t.Fatalf("API error = %#v", apiErr)
	}

	select {
	case err := <-handlerErr:
		t.Fatal(err)
	default:
	}
	mu.Lock()
	defer mu.Unlock()
	for _, key := range []string{
		"POST /v1/sandboxes",
		"GET /v1/sandboxes/box-1",
		"POST /v1/sandboxes/box-1/exec",
		"PUT /v1/sandboxes/box-1/files",
		"POST /v1/sandboxes/box-1/terminate",
		"DELETE /v1/sandboxes/box-1",
		"GET /v1/sandboxes/missing",
	} {
		if calls[key] == 0 {
			t.Errorf("route %q was not called", key)
		}
	}
}

func TestPathAndQueryEscaping(t *testing.T) {
	t.Parallel()

	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests <- request.Method + " " + request.RequestURI
		writer.Header().Set("Content-Type", "application/json")
		if request.Method == http.MethodPut {
			_, _ = io.WriteString(writer, `{"ok":true}`)
			return
		}
		_, _ = io.WriteString(writer, `{"sandboxes":[]}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.WriteFile(
		context.Background(),
		"box/a b?",
		"/tmp/a b&c?d",
		[]byte("x"),
	); err != nil {
		t.Fatal(err)
	}
	_, err = client.ListSandboxes(context.Background(), SandboxListOptions{
		Tags: map[string]string{"z": "a b", "a": "x&y"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := <-requests, "PUT /v1/sandboxes/box%2Fa%20b%3F/files?path=%2Ftmp%2Fa+b%26c%3Fd"; got != want {
		t.Fatalf("request URI = %q; want %q", got, want)
	}
	if got, want := <-requests, "GET /v1/sandboxes?tag=a%3Dx%26y&tag=z%3Da+b"; got != want {
		t.Fatalf("request URI = %q; want %q", got, want)
	}
}

func TestRequestCancellation(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	canceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		close(started)
		<-request.Context().Done()
		close(canceled)
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	requestErr := make(chan error, 1)
	go func() {
		_, requestError := client.Info(ctx)
		requestErr <- requestError
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("request never reached server")
	}
	cancel()
	select {
	case err = <-requestErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %T %v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("client request did not observe cancellation")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("server request context was not canceled")
	}
}

func TestExtendAndMigrateResponses(t *testing.T) {
	t.Parallel()

	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		requests <- request.Method + " " + request.URL.EscapedPath() + " " + string(body)
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/v1/sandboxes/box/extend":
			_, _ = io.WriteString(writer, `{"deadline_unix":1700000045}`)
		case "/v1/sandboxes/box/migrate":
			_, _ = io.WriteString(
				writer,
				`{"accepted":true,"migration_id":"migration-1","owner":"node-b"}`,
			)
		default:
			writer.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(writer, `{"code":"not_found","message":"unexpected route"}`)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	extended, err := client.ExtendSandbox(context.Background(), "box", 45)
	if err != nil {
		t.Fatal(err)
	}
	if extended.DeadlineUnix != 1700000045 {
		t.Fatalf("extend result = %#v", extended)
	}
	migrated, err := client.MigrateSandbox(context.Background(), "box", "node-b")
	if err != nil {
		t.Fatal(err)
	}
	if migrated["accepted"] != true ||
		migrated["migration_id"] != "migration-1" ||
		migrated["owner"] != "node-b" {
		t.Fatalf("migrate result = %#v", migrated)
	}

	if got, want := <-requests, `POST /v1/sandboxes/box/extend {"secs":45}`; got != want {
		t.Fatalf("extend request = %q; want %q", got, want)
	}
	if got, want := <-requests, `POST /v1/sandboxes/box/migrate {"target":"node-b"}`; got != want {
		t.Fatalf("migrate request = %q; want %q", got, want)
	}
}

func TestResponseBodiesAreClosedAndErrorsAreBounded(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		body := &trackingBody{Reader: strings.NewReader(`{"ok":true}`)}
		client := clientWithRoundTripper(t, func(*http.Request) (*http.Response, error) {
			return testResponse(http.StatusOK, body), nil
		})
		health, err := client.Health(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !health.OK || !body.closed.Load() {
			t.Fatalf("health=%#v closed=%v", health, body.closed.Load())
		}
	})

	t.Run("nested error", func(t *testing.T) {
		body := &trackingBody{Reader: strings.NewReader(`{"detail":{"code":"quota","message":"limit reached"}}`)}
		client := clientWithRoundTripper(t, func(*http.Request) (*http.Response, error) {
			return testResponse(http.StatusTooManyRequests, body), nil
		})
		_, err := client.Health(context.Background())
		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("error type = %T", err)
		}
		if apiErr.Code != "quota" || apiErr.Message != "limit reached" || !body.closed.Load() {
			t.Fatalf("error=%#v closed=%v", apiErr, body.closed.Load())
		}
	})

	t.Run("oversized error", func(t *testing.T) {
		body := &trackingBody{Reader: strings.NewReader(strings.Repeat("x", int(maxErrorResponseBytes)+100))}
		client := clientWithRoundTripper(t, func(*http.Request) (*http.Response, error) {
			return testResponse(http.StatusInternalServerError, body), nil
		})
		_, err := client.Health(context.Background())
		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("error type = %T", err)
		}
		if !apiErr.Truncated || len(apiErr.Message) > maxErrorMessageBytes || !body.closed.Load() {
			t.Fatalf("error=%#v message length=%d closed=%v", apiErr, len(apiErr.Message), body.closed.Load())
		}
	})
}

func TestEventStream(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(writer, ": keepalive\n\ndata: {\"event\":\"created\",\"id\":\"box\"}\n\n")
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := client.Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	event, err := stream.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event["event"] != "created" || event["id"] != "box" {
		t.Fatalf("event = %#v", event)
	}
}

func sandboxJSON(id, status string, returnCode *int64) string {
	returnCodeJSON := "null"
	if returnCode != nil {
		returnCodeJSON = fmt.Sprintf("%d", *returnCode)
	}
	return fmt.Sprintf(
		`{"id":%q,"name":%q,"status":%q,"created_at":1,"last_active":2,"expires_at":null,"terminated_at":null,"error":null,"tags":{},"returncode":%s}`,
		id,
		id,
		status,
		returnCodeJSON,
	)
}

func int64Pointer(value int64) *int64 {
	return &value
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type trackingBody struct {
	io.Reader
	closed atomic.Bool
}

func (body *trackingBody) Close() error {
	body.closed.Store(true)
	return nil
}

func clientWithRoundTripper(t *testing.T, tripper roundTripFunc) *Client {
	t.Helper()
	client, err := NewClient("http://vmon.test", WithHTTPClient(&http.Client{Transport: tripper}))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testResponse(status int, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       body,
	}
}
