package vmon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type remoteTestFailure struct {
	kind    string
	message string
	stack   string
}

type remoteTestReply struct {
	result  any
	stdout  string
	failure *remoteTestFailure
	delay   time.Duration
}

type remoteTestAPI struct {
	t      *testing.T
	server *httptest.Server

	mu           sync.Mutex
	nextID       int
	statuses     map[string]string
	files        map[string]map[string][]byte
	createBodies []map[string]any
	terminated   []string
	invocations  [][]any
	invoke       func([]any) remoteTestReply
}

func newRemoteTestAPI(t *testing.T, invoke func([]any) remoteTestReply) *remoteTestAPI {
	t.Helper()
	api := &remoteTestAPI{
		t:        t,
		statuses: make(map[string]string),
		files:    make(map[string]map[string][]byte),
		invoke:   invoke,
	}
	api.server = httptest.NewServer(http.HandlerFunc(api.serveHTTP))
	t.Cleanup(api.server.Close)
	return api
}

func (api *remoteTestAPI) client(t *testing.T) *Client {
	t.Helper()
	client, err := NewClient(api.server.URL, WithToken("test-token"))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func (api *remoteTestAPI) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	if request.Header.Get("Authorization") != "Bearer test-token" {
		writer.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(writer, `{"code":"unauthorized","message":"bad token"}`)
		return
	}
	segments, err := decodedRemoteTestPath(request.URL)
	if err != nil {
		api.t.Errorf("decode path: %v", err)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if request.Method == http.MethodPost && slices.Equal(segments, []string{"v1", "sandboxes"}) {
		api.createSandbox(writer, request)
		return
	}
	if len(segments) == 3 && segments[0] == "v1" && segments[1] == "sandboxes" && request.Method == http.MethodGet {
		api.getSandbox(writer, segments[2])
		return
	}
	if len(segments) != 4 || segments[0] != "v1" || segments[1] != "sandboxes" {
		remoteTestNotFound(writer, "unknown route")
		return
	}
	sandboxID, operation := segments[2], segments[3]
	switch {
	case request.Method == http.MethodPut && operation == "files":
		api.writeFile(writer, request, sandboxID)
	case request.Method == http.MethodDelete && operation == "files":
		api.deleteFile(writer, request, sandboxID)
	case request.Method == http.MethodPost && operation == "exec":
		api.exec(writer, request, sandboxID)
	case request.Method == http.MethodPost && operation == "terminate":
		api.terminateSandbox(writer, sandboxID)
	default:
		remoteTestNotFound(writer, "unknown operation")
	}
}

func (api *remoteTestAPI) createSandbox(writer http.ResponseWriter, request *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		api.t.Errorf("decode create: %v", err)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	api.mu.Lock()
	api.nextID++
	id := fmt.Sprintf("remote-%d", api.nextID)
	api.statuses[id] = "running"
	api.files[id] = make(map[string][]byte)
	api.createBodies = append(api.createBodies, body)
	api.mu.Unlock()
	writer.WriteHeader(http.StatusCreated)
	_, _ = io.WriteString(writer, sandboxJSON(id, "running", nil))
}

func (api *remoteTestAPI) getSandbox(writer http.ResponseWriter, id string) {
	api.mu.Lock()
	status, exists := api.statuses[id]
	api.mu.Unlock()
	if !exists {
		remoteTestNotFound(writer, "sandbox missing")
		return
	}
	_, _ = io.WriteString(writer, sandboxJSON(id, status, nil))
}

func (api *remoteTestAPI) writeFile(writer http.ResponseWriter, request *http.Request, id string) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		api.t.Errorf("read file body: %v", err)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	path := request.URL.Query().Get("path")
	api.mu.Lock()
	files, exists := api.files[id]
	if exists {
		files[path] = bytes.Clone(body)
	}
	api.mu.Unlock()
	if !exists || path == "" {
		remoteTestNotFound(writer, "sandbox or path missing")
		return
	}
	_, _ = io.WriteString(writer, `{"ok":true}`)
}

func (api *remoteTestAPI) deleteFile(writer http.ResponseWriter, request *http.Request, id string) {
	path := request.URL.Query().Get("path")
	api.mu.Lock()
	files, exists := api.files[id]
	if exists {
		delete(files, path)
	}
	api.mu.Unlock()
	if !exists || path == "" {
		remoteTestNotFound(writer, "sandbox or path missing")
		return
	}
	_, _ = io.WriteString(writer, `{"ok":true}`)
}

func (api *remoteTestAPI) exec(writer http.ResponseWriter, request *http.Request, id string) {
	var body struct {
		Command []string `json:"cmd"`
	}
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		api.t.Errorf("decode exec: %v", err)
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if slices.Equal(body.Command, []string{"node", "--version"}) {
		remoteTestCapture(writer, 0, "v22.0.0\n", "")
		return
	}
	if len(body.Command) != 3 || body.Command[0] != "node" || body.Command[1] != remoteFunctionRunnerPath {
		remoteTestCapture(writer, 127, "", "unsupported command")
		return
	}

	api.mu.Lock()
	payload := bytes.Clone(api.files[id][body.Command[2]])
	api.mu.Unlock()
	var invocation struct {
		Source     string `json:"source"`
		ExportName string `json:"exportName"`
		Arguments  []any  `json:"args"`
	}
	if err := json.Unmarshal(payload, &invocation); err != nil {
		remoteTestCapture(writer, 1, "", "invalid invocation")
		return
	}
	api.mu.Lock()
	api.invocations = append(api.invocations, slices.Clone(invocation.Arguments))
	api.mu.Unlock()
	reply := api.invoke(invocation.Arguments)
	if reply.delay > 0 {
		time.Sleep(reply.delay)
	}
	var response any
	if reply.failure == nil {
		response = map[string]any{"ok": true, "result": reply.result, "stdout": reply.stdout}
	} else {
		response = map[string]any{
			"ok":     false,
			"stdout": reply.stdout,
			"error": map[string]any{
				"type": reply.failure.kind, "message": reply.failure.message, "stack": reply.failure.stack,
			},
		}
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		api.t.Errorf("encode invocation response: %v", err)
		remoteTestCapture(writer, 1, "", err.Error())
		return
	}
	remoteTestCapture(writer, 0, string(encoded), "")
}

func (api *remoteTestAPI) terminateSandbox(writer http.ResponseWriter, id string) {
	api.mu.Lock()
	status, exists := api.statuses[id]
	if exists {
		api.statuses[id] = "terminated"
		api.terminated = append(api.terminated, id)
	}
	api.mu.Unlock()
	if !exists {
		remoteTestNotFound(writer, "sandbox missing")
		return
	}
	_, _ = io.WriteString(writer, sandboxJSON(id, status, nil))
}

func (api *remoteTestAPI) setStatus(id, status string) {
	api.mu.Lock()
	defer api.mu.Unlock()
	if _, exists := api.statuses[id]; !exists {
		api.t.Fatalf("unknown test sandbox %q", id)
	}
	api.statuses[id] = status
}

func (api *remoteTestAPI) forget(id string) {
	api.mu.Lock()
	defer api.mu.Unlock()
	delete(api.statuses, id)
	delete(api.files, id)
}

func (api *remoteTestAPI) snapshot() (creates int, terminated []string, invocations [][]any) {
	api.mu.Lock()
	defer api.mu.Unlock()
	invocations = make([][]any, len(api.invocations))
	for index, arguments := range api.invocations {
		invocations[index] = slices.Clone(arguments)
	}
	return len(api.createBodies), slices.Clone(api.terminated), invocations
}

func TestRemoteFunctionReusesAndReprovisionsSandbox(t *testing.T) {
	t.Parallel()
	api := newRemoteTestAPI(t, func(arguments []any) remoteTestReply {
		left, right := arguments[0].(float64), arguments[1].(float64)
		return remoteTestReply{result: map[string]any{"sum": left + right}, stdout: "guest output\n"}
	})
	var stdout bytes.Buffer
	function, err := NewRemoteFunction[struct {
		Sum int `json:"sum"`
	}](
		api.client(t),
		RemoteFunctionSourceSpec{Source: "export function add(a, b) { return { sum: a + b }; }", ExportName: "add"},
		RemoteFunctionOptions{Stdout: &stdout},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	first, err := function.Remote(ctx, 2, 5)
	if err != nil || first.Sum != 7 {
		t.Fatalf("first remote = %#v, %v", first, err)
	}
	second, err := function.Remote(ctx, 10, 4)
	if err != nil || second.Sum != 14 {
		t.Fatalf("second remote = %#v, %v", second, err)
	}
	creates, _, _ := api.snapshot()
	if creates != 1 {
		t.Fatalf("cached remote created %d sandboxes; want 1", creates)
	}

	api.setStatus("remote-1", "stopped")
	if _, err := function.Remote(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	api.forget("remote-2")
	if _, err := function.Remote(ctx, 3, 4); err != nil {
		t.Fatal(err)
	}
	creates, terminated, _ := api.snapshot()
	if creates != 3 || !slices.Contains(terminated, "remote-1") {
		t.Fatalf("reprovision state creates=%d terminated=%v", creates, terminated)
	}

	errorsByCall := make(chan error, 2)
	for range 2 {
		go func() { errorsByCall <- function.Terminate(ctx) }()
	}
	for range 2 {
		if err := <-errorsByCall; err != nil {
			t.Fatal(err)
		}
	}
	if err := function.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	_, terminated, _ = api.snapshot()
	if countValue(terminated, "remote-3") != 1 {
		t.Fatalf("cached sandbox terminated %d times: %v", countValue(terminated, "remote-3"), terminated)
	}
	if stdout.String() != strings.Repeat("guest output\n", 4) {
		t.Fatalf("forwarded stdout = %q", stdout.String())
	}

	creates, _, _ = api.snapshot()
	if _, err := function.Remote(ctx, math.NaN()); err == nil || !strings.Contains(err.Error(), "JSON-serializable") {
		t.Fatalf("non-JSON argument error = %v", err)
	}
	if after, _, _ := api.snapshot(); after != creates {
		t.Fatalf("invalid arguments created a sandbox: before=%d after=%d", creates, after)
	}
}

func TestRemoteFunctionMapOrderingFailureAndCleanup(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	api := newRemoteTestAPI(t, func(arguments []any) remoteTestReply {
		value := arguments[0]
		if value == "boom" {
			return remoteTestReply{
				stdout:  "before failure\n",
				failure: &remoteTestFailure{kind: "TypeError", message: "exploded", stack: "remote stack"},
			}
		}
		number := int(value.(float64))
		delay := 10 * time.Millisecond
		if number == 1 {
			delay = 300 * time.Millisecond
		}
		return remoteTestReply{result: number * 10, delay: delay}
	})
	function, err := NewRemoteFunction[int](
		api.client(t),
		RemoteFunctionSourceSpec{Source: "export const scale = (value) => value * 10;", ExportName: "scale"},
		RemoteFunctionOptions{Stdout: &stdout},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	ordered, err := function.Map(ctx, []any{1, 2, 3}, RemoteMapOptions{
		Concurrency: 2,
		Order:       RemoteInputOrder,
	})
	if err != nil || !slices.Equal(ordered, []int{10, 20, 30}) {
		t.Fatalf("ordered map = %v, %v", ordered, err)
	}
	completion, err := function.StarMap(ctx, [][]any{{1}, {2}, {3}}, RemoteMapOptions{
		Concurrency: 2,
		Order:       RemoteCompletionOrder,
	})
	if err != nil || !slices.Equal(completion, []int{20, 30, 10}) {
		t.Fatalf("completion map = %v, %v", completion, err)
	}
	creates, terminated, _ := api.snapshot()
	if creates != 4 || len(terminated) != 4 {
		t.Fatalf("successful map cleanup creates=%d terminated=%v", creates, terminated)
	}

	_, err = function.Map(ctx, []any{"boom", "not-scheduled"}, RemoteMapOptions{
		Concurrency: 1,
		Order:       RemoteInputOrder,
	})
	var remoteError *RemoteFunctionError
	if !errors.As(err, &remoteError) {
		t.Fatalf("failure type = %T, %v", err, err)
	}
	if remoteError.RemoteType != "TypeError" || remoteError.Message != "exploded" || remoteError.RemoteStack != "remote stack" {
		t.Fatalf("remote error = %#v", remoteError)
	}
	if stdout.String() != "before failure\n" {
		t.Fatalf("failure stdout = %q", stdout.String())
	}
	_, terminated, invocations := api.snapshot()
	if len(terminated) != 5 {
		t.Fatalf("failed map did not clean its worker: %v", terminated)
	}
	if got := invocations[len(invocations)-1]; len(got) != 1 || got[0] != "boom" {
		t.Fatalf("map scheduled after first failure: tail=%v", invocations)
	}

	creates, _, _ = api.snapshot()
	if _, err := function.Map(ctx, []any{1}, RemoteMapOptions{}); err == nil {
		t.Fatal("explicit zero concurrency was accepted")
	}
	if after, _, _ := api.snapshot(); after != creates {
		t.Fatalf("invalid map options created a sandbox: before=%d after=%d", creates, after)
	}
}

func TestRemoteFunctionValidatesSource(t *testing.T) {
	t.Parallel()
	api := newRemoteTestAPI(t, func([]any) remoteTestReply { return remoteTestReply{} })
	client := api.client(t)
	cases := []RemoteFunctionSourceSpec{
		{Source: "", ExportName: "handler"},
		{Source: "export const handler = () => null;", ExportName: "not-valid!"},
	}
	for _, source := range cases {
		if _, err := NewRemoteFunction[any](client, source); err == nil {
			t.Fatalf("accepted invalid source %#v", source)
		}
	}
}

func decodedRemoteTestPath(target *url.URL) ([]string, error) {
	parts := strings.Split(strings.Trim(target.EscapedPath(), "/"), "/")
	for index, part := range parts {
		decoded, err := url.PathUnescape(part)
		if err != nil {
			return nil, err
		}
		parts[index] = decoded
	}
	return parts, nil
}

func remoteTestCapture(writer http.ResponseWriter, exit int64, stdout, stderr string) {
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"exit":       exit,
		"stdout_b64": base64.StdEncoding.EncodeToString([]byte(stdout)),
		"stderr_b64": base64.StdEncoding.EncodeToString([]byte(stderr)),
	})
}

func remoteTestNotFound(writer http.ResponseWriter, message string) {
	writer.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(writer).Encode(map[string]any{"code": "not_found", "message": message})
}

func countValue(values []string, target string) int {
	count := 0
	for _, value := range values {
		if value == target {
			count++
		}
	}
	return count
}
