package vmon

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	ws "github.com/coder/websocket"
)

func TestProcessFrames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/sandboxes/box/exec" {
			t.Errorf("path=%q", request.URL.Path)
			http.NotFound(writer, request)
			return
		}
		connection, err := ws.Accept(writer, request, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.CloseNow()
		_, first, err := connection.Read(request.Context())
		if err != nil {
			t.Error(err)
			return
		}
		var exec ExecRequest
		if err = json.Unmarshal(first, &exec); err != nil || len(exec.Command) != 1 || exec.Command[0] != "echo" {
			t.Errorf("request=%s err=%v", first, err)
			return
		}
		_ = connection.Write(request.Context(), ws.MessageText, []byte(`{"stream":"stdout","b64":"b2sK"}`))
		_ = connection.Write(request.Context(), ws.MessageText, []byte(`{"exit":0}`))
	}))
	defer server.Close()
	client, err := Connect(server.URL, WithDiscovery(false))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	process, err := client.Sandboxes.Ref("box").Exec(context.Background(), ExecRequest{Command: []string{"echo"}})
	if err != nil {
		t.Fatal(err)
	}
	event, err := process.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(event.Data) != "ok\n" || event.Stream != StreamStdout {
		t.Fatalf("event=%+v", event)
	}
	exit, err := process.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if exit.Code != 0 {
		t.Fatalf("exit=%+v", exit)
	}
}

func TestShellConsumesReadyFrame(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := ws.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		_, _, _ = connection.Read(request.Context())
		_ = connection.Write(request.Context(), ws.MessageText, []byte(`{"ready":"box-shell"}`))
		_ = connection.Write(request.Context(), ws.MessageText, []byte(`{"exit":7}`))
	}))
	defer server.Close()
	client, err := Connect(server.URL, WithDiscovery(false))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	process, err := client.Shell(context.Background(), ShellRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if process.SandboxID != "box-shell" {
		t.Fatalf("sandbox=%q", process.SandboxID)
	}
	exit, err := process.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if exit.Code != 7 {
		t.Fatalf("exit=%d", exit.Code)
	}
}

type relocationWebSocketDriver struct {
	target   string
	mu       sync.Mutex
	dials    []string
	resolves int
}

func (driver *relocationWebSocketDriver) Do(context.Context, DriverRequest) (*http.Response, string, error) {
	return nil, "", io.EOF
}
func (driver *relocationWebSocketDriver) Dial(ctx context.Context, path string, query url.Values, endpoint string) (*WebSocketConn, string, error) {
	driver.mu.Lock()
	driver.dials = append(driver.dials, endpoint)
	driver.mu.Unlock()
	if endpoint == "old" {
		return nil, "old", &APIError{StatusCode: http.StatusNotFound, Code: "not_found", Message: "moved"}
	}
	target := driver.target + path
	if encoded := query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	connection, response, err := ws.Dial(ctx, target, nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		return nil, endpoint, err
	}
	return &WebSocketConn{conn: connection}, endpoint, nil
}
func (driver *relocationWebSocketDriver) ResolveSandbox(_ context.Context, id, hint string) (string, error) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	driver.resolves++
	if id != "box" || hint != "old" {
		return "", &ProtocolError{Operation: "test resolve", Message: "unexpected affinity"}
	}
	return driver.target, nil
}
func (driver *relocationWebSocketDriver) Endpoints() []EndpointInfo {
	return []EndpointInfo{{URL: "old", Healthy: true}, {URL: driver.target, Healthy: true}}
}
func (driver *relocationWebSocketDriver) Refresh(context.Context, bool) error { return nil }
func (driver *relocationWebSocketDriver) Close() error                        { return nil }

func TestSandboxWebSocketsRelocateOnceOnNotFound(t *testing.T) {
	tests := []struct {
		name string
		path string
		open func(context.Context, *Sandbox) (func() error, error)
	}{
		{
			name: "exec",
			path: "/v1/sandboxes/box/exec",
			open: func(ctx context.Context, sandbox *Sandbox) (func() error, error) {
				process, err := sandbox.Exec(ctx, ExecRequest{Command: []string{"true"}})
				if err != nil {
					return nil, err
				}
				return process.Close, nil
			},
		},
		{
			name: "attach",
			path: "/v1/sandboxes/box/attach",
			open: func(ctx context.Context, sandbox *Sandbox) (func() error, error) {
				stream, err := sandbox.Attach(ctx)
				if err != nil {
					return nil, err
				}
				return stream.Close, nil
			},
		},
		{
			name: "port",
			path: "/v1/sandboxes/box/ports/8080/ws/chat",
			open: func(ctx context.Context, sandbox *Sandbox) (func() error, error) {
				socket, err := sandbox.Ports.WebSocket(ctx, 8080, "chat", nil)
				if err != nil {
					return nil, err
				}
				return socket.Close, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requests := make(chan *http.Request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				connection, err := ws.Accept(writer, request, nil)
				if err != nil {
					return
				}
				defer connection.CloseNow()
				requests <- request.Clone(context.Background())
				_, _, _ = connection.Read(request.Context())
			}))
			defer server.Close()
			driver := &relocationWebSocketDriver{target: server.URL}
			sandbox := NewClient(driver).Sandboxes.Ref("box")
			sandbox.endpoint = "old"
			sandbox.connectToken = "secret"

			closeSocket, err := test.open(context.Background(), sandbox)
			if err != nil {
				t.Fatal(err)
			}
			request := <-requests
			if request.URL.Path != test.path {
				t.Fatalf("path=%q, want %q", request.URL.Path, test.path)
			}
			if test.name == "port" && request.URL.Query().Get("connect_token") != "secret" {
				t.Fatalf("connect token=%q", request.URL.Query().Get("connect_token"))
			}
			driver.mu.Lock()
			dials := append([]string(nil), driver.dials...)
			resolves := driver.resolves
			driver.mu.Unlock()
			if len(dials) != 2 || dials[0] != "old" || dials[1] != server.URL || resolves != 1 || sandbox.endpoint != server.URL {
				t.Fatalf("dials=%v resolves=%d endpoint=%q", dials, resolves, sandbox.endpoint)
			}
			_ = closeSocket()
		})
	}
}
