package vmon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type stubDriver struct {
	do        func(context.Context, DriverRequest) (*http.Response, string, error)
	resolve   func(context.Context, string, string) (string, error)
	endpoints []EndpointInfo
}

func (driver *stubDriver) Do(ctx context.Context, request DriverRequest) (*http.Response, string, error) {
	return driver.do(ctx, request)
}
func (driver *stubDriver) Dial(context.Context, string, url.Values, string) (*WebSocketConn, string, error) {
	return nil, "", io.EOF
}
func (driver *stubDriver) ResolveSandbox(ctx context.Context, id, hint string) (string, error) {
	if driver.resolve != nil {
		return driver.resolve(ctx, id, hint)
	}
	return hint, nil
}
func (driver *stubDriver) Endpoints() []EndpointInfo           { return driver.endpoints }
func (driver *stubDriver) Refresh(context.Context, bool) error { return nil }
func (driver *stubDriver) Close() error                        { return nil }
func jsonResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestClientServicesAndBoundSandbox(t *testing.T) {
	var requests []DriverRequest
	driver := &stubDriver{endpoints: []EndpointInfo{{URL: "http://node-a", Healthy: true}}, do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
		requests = append(requests, request)
		switch request.Path {
		case "/v1/sandboxes":
			return jsonResponse(200, `{"id":"box","status":"running"}`), "http://node-a", nil
		case "/v1/sandboxes/box/metrics":
			return jsonResponse(200, `{"cpu":1}`), "http://node-a", nil
		default:
			return jsonResponse(404, `{"code":"not_found","message":"missing"}`), "http://node-a", nil
		}
	}}
	client := NewClient(driver)
	if client.Sandboxes == nil || client.Snapshots == nil || client.Volumes == nil || client.Pools == nil || client.Mesh == nil {
		t.Fatal("client services were not initialized")
	}
	sandbox, err := client.Sandboxes.Create(context.Background(), SandboxCreateRequest{Image: "alpine"})
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := sandbox.Metrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if metrics["cpu"] != float64(1) {
		t.Fatalf("metrics=%v", metrics)
	}
	if requests[1].Endpoint != "http://node-a" {
		t.Fatalf("sandbox affinity=%q", requests[1].Endpoint)
	}
}

func TestSandboxRelocatesOnceOnNotFound(t *testing.T) {
	calls := 0
	resolved := 0
	driver := &stubDriver{endpoints: []EndpointInfo{{URL: "a", Healthy: true}, {URL: "b", Healthy: true}}, resolve: func(_ context.Context, id, hint string) (string, error) {
		resolved++
		if id != "box" || hint != "a" {
			t.Fatalf("resolve(%q,%q)", id, hint)
		}
		return "b", nil
	}, do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
		calls++
		if request.Endpoint == "a" {
			return jsonResponse(404, `{"code":"not_found","message":"moved"}`), "a", nil
		}
		if request.Endpoint != "b" {
			t.Fatalf("endpoint=%q", request.Endpoint)
		}
		return jsonResponse(200, `{"id":"box","status":"running"}`), "b", nil
	}}
	client := NewClient(driver)
	sandbox := client.Sandboxes.Ref("box")
	sandbox.endpoint = "a"
	if _, err := sandbox.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || resolved != 1 || sandbox.endpoint != "b" {
		t.Fatalf("calls=%d resolved=%d endpoint=%q", calls, resolved, sandbox.endpoint)
	}
}

func TestSandboxListFiltersNodeAndOnlySkipsTransportErrors(t *testing.T) {
	t.Run("transport error", func(t *testing.T) {
		driver := &stubDriver{
			endpoints: []EndpointInfo{{URL: "a", Healthy: true}, {URL: "b", Healthy: true}},
			do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
				if request.Endpoint == "a" {
					return nil, "", &TransportError{Endpoint: "a", Err: io.EOF}
				}
				return jsonResponse(http.StatusOK, `{"sandboxes":[{"id":"other","node":"node-a"},{"id":"wanted","node":"node-b"}]}`), "b", nil
			},
		}
		sandboxes, err := NewClient(driver).Sandboxes.List(context.Background(), SandboxListOptions{Node: "node-b"})
		if err != nil {
			t.Fatal(err)
		}
		if len(sandboxes) != 1 || sandboxes[0].ID != "wanted" || sandboxes[0].endpoint != "b" {
			t.Fatalf("sandboxes=%#v", sandboxes)
		}
	})

	t.Run("API error", func(t *testing.T) {
		driver := &stubDriver{
			endpoints: []EndpointInfo{{URL: "a", Healthy: true}, {URL: "b", Healthy: true}},
			do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
				if request.Endpoint == "a" {
					return jsonResponse(http.StatusConflict, `{"code":"busy","message":"try later"}`), "a", nil
				}
				return jsonResponse(http.StatusOK, `{"sandboxes":[]}`), "b", nil
			},
		}
		_, err := NewClient(driver).Sandboxes.List(context.Background())
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Code != "busy" {
			t.Fatalf("error=%T %v", err, err)
		}
	})

	t.Run("protocol error", func(t *testing.T) {
		driver := &stubDriver{
			endpoints: []EndpointInfo{{URL: "a", Healthy: true}, {URL: "b", Healthy: true}},
			do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
				if request.Endpoint == "a" {
					return jsonResponse(http.StatusOK, `{}`), "a", nil
				}
				return jsonResponse(http.StatusOK, `{"sandboxes":[]}`), "b", nil
			},
		}
		_, err := NewClient(driver).Sandboxes.List(context.Background())
		var protocolErr *ProtocolError
		if !errors.As(err, &protocolErr) {
			t.Fatalf("error=%T %v", err, err)
		}
	})
}

func TestSandboxListFansOutConcurrentlyAndEmptySuccessWins(t *testing.T) {
	var active atomic.Int32
	var peak atomic.Int32
	driver := &stubDriver{
		endpoints: []EndpointInfo{{URL: "a", Healthy: true}, {URL: "b", Healthy: true}},
		do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				seen := peak.Load()
				if current <= seen || peak.CompareAndSwap(seen, current) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			if got := request.Query["tag"]; len(got) != 2 || got[0] != "a=1" || got[1] != "z=2" {
				t.Errorf("tag query=%v", got)
			}
			if request.Endpoint == "a" {
				return nil, "", &TransportError{Endpoint: "a", Err: io.EOF}
			}
			return jsonResponse(http.StatusOK, `{"sandboxes":[]}`), "b", nil
		},
	}
	sandboxes, err := NewClient(driver).Sandboxes.List(context.Background(), SandboxListOptions{Tags: map[string]string{"z": "2", "a": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sandboxes) != 0 {
		t.Fatalf("sandboxes=%v", sandboxes)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrent requests=%d, want 2", peak.Load())
	}
}

func TestPoolClearListsAndDeletesEveryReference(t *testing.T) {
	var deleted map[string]bool
	deleted = make(map[string]bool)
	driver := &stubDriver{do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
		switch {
		case request.Method == http.MethodGet && request.Path == "/v1/pools":
			return jsonResponse(http.StatusOK, `{"image:one":{"ready":1},"image/two":{"ready":2}}`), "node", nil
		case request.Method == http.MethodDelete:
			deleted[request.Path] = true
			return jsonResponse(http.StatusNoContent, ""), "node", nil
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.Path)
			return nil, "", nil
		}
	}}
	if err := NewClient(driver).Pools.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 || !deleted["/v1/pools/image:one"] || !deleted["/v1/pools/image%2Ftwo"] {
		t.Fatalf("deleted=%v", deleted)
	}
}

func TestStrictCollectionEnvelopes(t *testing.T) {
	tests := []struct {
		name string
		body string
		call func(*Client) error
	}{
		{name: "files list", call: func(client *Client) error {
			_, err := client.Sandboxes.Ref("box").Files.List(context.Background(), ".")
			return err
		}},
		{name: "snapshot fork", call: func(client *Client) error {
			_, err := client.Snapshots.Fork(context.Background(), "base", ForkRequest{})
			return err
		}},
		{name: "snapshots list", call: func(client *Client) error {
			_, err := client.Snapshots.List(context.Background())
			return err
		}},
		{name: "volumes list", call: func(client *Client) error {
			_, err := client.Volumes.List(context.Background())
			return err
		}},
		{name: "pools list null", body: "null", call: func(client *Client) error {
			_, err := client.Pools.List(context.Background())
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := test.body
			if body == "" {
				body = `{}`
			}
			driver := &stubDriver{do: func(_ context.Context, _ DriverRequest) (*http.Response, string, error) {
				return jsonResponse(http.StatusOK, body), "node", nil
			}}
			err := test.call(NewClient(driver))
			var protocolErr *ProtocolError
			if !errors.As(err, &protocolErr) {
				t.Fatalf("missing envelope error=%T %v", err, err)
			}
		})
	}
}

func TestExtendPreservesSandboxIDWhenResponseOmitsIt(t *testing.T) {
	driver := &stubDriver{do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
		if request.Path != "/v1/sandboxes/box/extend" {
			t.Fatalf("path=%q", request.Path)
		}
		return jsonResponse(http.StatusOK, `{"status":"running"}`), "node", nil
	}}
	sandbox := NewClient(driver).Sandboxes.Ref("box")
	extended, err := sandbox.Extend(context.Background(), 30)
	if err != nil {
		t.Fatal(err)
	}
	if extended.ID != "box" || sandbox.ID != "box" {
		t.Fatalf("extended ID=%q sandbox ID=%q", extended.ID, sandbox.ID)
	}
}

func TestPortProxyReturnsRawDownstreamNon2xx(t *testing.T) {
	driver := &stubDriver{
		endpoints: []EndpointInfo{{URL: "node", Healthy: true}},
		do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
			if request.Path != "/v1/sandboxes/box/ports/8080/status" || !request.Stream {
				t.Fatalf("request=%+v", request)
			}
			response := jsonResponse(http.StatusTeapot, "downstream failure")
			response.Header.Set("X-Downstream", "yes")
			return response, "node", nil
		},
	}
	sandbox := NewClient(driver).Sandboxes.Ref("box")
	sandbox.endpoint = "node"
	sandbox.connectToken = "secret"
	response, err := sandbox.Ports.HTTP(context.Background(), 8080, ProxyRequest{Method: http.MethodGet, Path: "status", Body: strings.NewReader("")})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusTeapot || response.Header.Get("X-Downstream") != "yes" || string(body) != "downstream failure" {
		t.Fatalf("status=%d header=%q body=%q", response.StatusCode, response.Header.Get("X-Downstream"), body)
	}
}

func TestPortProxyOnlyRelocatesDaemonNotFound(t *testing.T) {
	t.Run("downstream 404 is not replayed", func(t *testing.T) {
		calls := 0
		resolves := 0
		driver := &stubDriver{
			endpoints: []EndpointInfo{{URL: "a", Healthy: true}, {URL: "b", Healthy: true}},
			resolve: func(context.Context, string, string) (string, error) {
				resolves++
				return "b", nil
			},
			do: func(_ context.Context, _ DriverRequest) (*http.Response, string, error) {
				calls++
				return jsonResponse(http.StatusNotFound, `{"message":"app route missing"}`), "a", nil
			},
		}
		sandbox := NewClient(driver).Sandboxes.Ref("box")
		sandbox.endpoint, sandbox.connectToken = "a", "secret"
		response, err := sandbox.Ports.HTTP(context.Background(), 8080, ProxyRequest{Method: http.MethodPost, Body: strings.NewReader("side effect")})
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if calls != 1 || resolves != 0 || string(body) != `{"message":"app route missing"}` {
			t.Fatalf("calls=%d resolves=%d body=%q", calls, resolves, body)
		}
	})

	t.Run("daemon not_found relocates once", func(t *testing.T) {
		calls := 0
		resolves := 0
		driver := &stubDriver{
			endpoints: []EndpointInfo{{URL: "a", Healthy: true}, {URL: "b", Healthy: true}},
			resolve: func(context.Context, string, string) (string, error) {
				resolves++
				return "b", nil
			},
			do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
				calls++
				if request.Endpoint == "a" {
					return jsonResponse(http.StatusNotFound, `{"code":"not_found","message":"moved"}`), "a", nil
				}
				return jsonResponse(http.StatusCreated, "created"), "b", nil
			},
		}
		sandbox := NewClient(driver).Sandboxes.Ref("box")
		sandbox.endpoint, sandbox.connectToken = "a", "secret"
		response, err := sandbox.Ports.HTTP(context.Background(), 8080, ProxyRequest{Method: http.MethodPost, Body: strings.NewReader("side effect")})
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if calls != 2 || resolves != 1 || response.StatusCode != http.StatusCreated {
			t.Fatalf("calls=%d resolves=%d status=%d", calls, resolves, response.StatusCode)
		}
	})
}

func TestFilesLimitsDeleteOptionsAndPathValidation(t *testing.T) {
	var deleteQuery url.Values
	driver := &stubDriver{do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
		switch request.Method {
		case http.MethodGet:
			return jsonResponse(http.StatusOK, "12345"), "node", nil
		case http.MethodDelete:
			deleteQuery = request.Query
			return jsonResponse(http.StatusNoContent, ""), "node", nil
		default:
			return jsonResponse(http.StatusOK, `{"exit":0,"stdout_b64":"","stderr_b64":""}`), "node", nil
		}
	}}
	client := NewClient(driver, WithMaxResponseBytes(4))
	files := client.Sandboxes.Ref("box").Files
	if _, err := files.Read(context.Background(), "large"); !errors.As(err, new(*ResponseTooLargeError)) {
		t.Fatalf("large read error=%T %v", err, err)
	}
	if err := files.Delete(context.Background(), "dir", DeleteOptions{Recursive: true}); err != nil {
		t.Fatal(err)
	}
	if deleteQuery.Get("recursive") != "true" {
		t.Fatalf("delete query=%v", deleteQuery)
	}
	if err := files.Write(context.Background(), "", nil); err == nil {
		t.Fatal("empty write path accepted")
	}
	if _, err := files.Stat(context.Background(), ""); err == nil {
		t.Fatal("empty stat path accepted")
	}
	if err := files.Delete(context.Background(), ""); err == nil {
		t.Fatal("empty delete path accepted")
	}
	if err := files.Mkdir(context.Background(), ""); err == nil {
		t.Fatal("empty mkdir path accepted")
	}
}

func TestWaitReadyProbes(t *testing.T) {
	t.Run("mutually exclusive", func(t *testing.T) {
		sandbox := NewClient(&stubDriver{do: func(context.Context, DriverRequest) (*http.Response, string, error) {
			t.Fatal("driver called for invalid options")
			return nil, "", nil
		}}).Sandboxes.Ref("box")
		_, err := sandbox.WaitReady(context.Background(), WaitReadyOptions{Port: 80, Command: []string{"true"}})
		if err == nil {
			t.Fatal("accepted mutually exclusive probes")
		}
	})

	t.Run("command", func(t *testing.T) {
		driver := &stubDriver{do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
			switch request.Path {
			case "/v1/sandboxes/box":
				return jsonResponse(http.StatusOK, `{"id":"box","status":"running"}`), "node", nil
			case "/v1/sandboxes/box/exec":
				return jsonResponse(http.StatusOK, `{"exit":0,"stdout_b64":"","stderr_b64":""}`), "node", nil
			default:
				t.Fatalf("unexpected path %q", request.Path)
				return nil, "", nil
			}
		}}
		sandbox := NewClient(driver).Sandboxes.Ref("box")
		if _, err := sandbox.WaitReady(context.Background(), WaitReadyOptions{Command: []string{"check"}, Timeout: time.Second, Interval: time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("port", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			connection, acceptErr := listener.Accept()
			if acceptErr == nil {
				_ = connection.Close()
			}
		}()
		port := listener.Addr().(*net.TCPAddr).Port
		driver := &stubDriver{do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
			switch request.Path {
			case "/v1/sandboxes/box":
				return jsonResponse(http.StatusOK, `{"id":"box","status":"running"}`), "node", nil
			case "/v1/sandboxes/box/tunnels":
				body := fmt.Sprintf(`{"tunnels":{"8080":{"host":"127.0.0.1","port":%d}}}`, port)
				return jsonResponse(http.StatusOK, body), "node", nil
			default:
				t.Fatalf("unexpected path %q", request.Path)
				return nil, "", nil
			}
		}}
		sandbox := NewClient(driver).Sandboxes.Ref("box")
		if _, err := sandbox.WaitReady(context.Background(), WaitReadyOptions{Port: 8080, Timeout: time.Second, Interval: time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	})
}
