package vmon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ws "github.com/coder/websocket"
)

func TestStreamingExecProtocol(t *testing.T) {
	t.Parallel()

	handlerErr := make(chan error, 1)
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer close(serverDone)
		if request.Header.Get("Authorization") != "Bearer ws-token" {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(writer, `{"code":"unauthorized","message":"missing token"}`)
			return
		}
		connection, err := ws.Accept(writer, request, nil)
		if err != nil {
			handlerErr <- err
			return
		}
		defer connection.CloseNow()
		_, first, err := connection.Read(request.Context())
		if err != nil {
			handlerErr <- err
			return
		}
		var execRequest struct {
			Command []string `json:"cmd"`
			TTY     bool     `json:"tty"`
		}
		if err := json.Unmarshal(first, &execRequest); err != nil {
			handlerErr <- err
			return
		}
		if strings.Join(execRequest.Command, " ") != "cat -" || !execRequest.TTY {
			handlerErr <- fmt.Errorf("first exec frame = %s", first)
			return
		}

		seenStdin := false
		seenResize := false
		seenEOF := false
		for range 3 {
			_, frame, err := connection.Read(request.Context())
			if err != nil {
				handlerErr <- err
				return
			}
			var value map[string]json.RawMessage
			if err := json.Unmarshal(frame, &value); err != nil {
				handlerErr <- err
				return
			}
			if raw, exists := value["stdin_b64"]; exists {
				var encoded string
				if err := json.Unmarshal(raw, &encoded); err != nil {
					handlerErr <- err
					return
				}
				decoded, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil || string(decoded) != "input" {
					handlerErr <- fmt.Errorf("stdin frame = %s", frame)
					return
				}
				seenStdin = true
			}
			if raw, exists := value["resize"]; exists {
				var dimensions [2]uint16
				if err := json.Unmarshal(raw, &dimensions); err != nil {
					handlerErr <- err
					return
				}
				seenResize = dimensions == [2]uint16{24, 80}
			}
			if raw, exists := value["eof"]; exists {
				var eof bool
				if err := json.Unmarshal(raw, &eof); err != nil {
					handlerErr <- err
					return
				}
				seenEOF = eof
			}
		}
		if !seenStdin || !seenResize || !seenEOF {
			handlerErr <- fmt.Errorf("client frames: stdin=%v resize=%v eof=%v", seenStdin, seenResize, seenEOF)
			return
		}
		frames := []string{
			fmt.Sprintf(`{"stream":"stdout","b64":%q}`, base64.StdEncoding.EncodeToString([]byte("out"))),
			fmt.Sprintf(`{"stream":"stderr","b64":%q}`, base64.StdEncoding.EncodeToString([]byte("err"))),
			`{"exit":7,"signal":null}`,
		}
		for _, frame := range frames {
			if err := connection.Write(request.Context(), ws.MessageText, []byte(frame)); err != nil {
				handlerErr <- err
				return
			}
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithToken("ws-token"))
	if err != nil {
		t.Fatal(err)
	}
	session, err := client.Exec(context.Background(), "box", ExecRequest{
		Command: []string{"cat", "-"},
		TTY:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.WriteStdin(context.Background(), []byte("input")); err != nil {
		t.Fatal(err)
	}
	if err := session.Resize(context.Background(), 24, 80); err != nil {
		t.Fatal(err)
	}
	if err := session.CloseStdin(context.Background()); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit, err := session.Copy(context.Background(), &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if exit.Code != 7 || exit.Signal != nil || stdout.String() != "out" || stderr.String() != "err" {
		t.Fatalf("exit=%#v stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("websocket handler did not finish")
	}
	select {
	case err := <-handlerErr:
		t.Fatal(err)
	default:
	}
}

func TestAttachAndWebSocketCancellation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := ws.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		if strings.Contains(request.URL.Path, "/sandboxes/cancel/") {
			<-request.Context().Done()
			return
		}
		frame := fmt.Sprintf(
			`{"stream":"console","b64":%q}`,
			base64.StdEncoding.EncodeToString([]byte("console")),
		)
		if err := connection.Write(request.Context(), ws.MessageText, []byte(frame)); err != nil {
			return
		}
		_, _, _ = connection.Read(request.Context())
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	attach, err := client.Attach(context.Background(), "box")
	if err != nil {
		t.Fatal(err)
	}
	event, err := attach.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event.Stream != StreamConsole || string(event.Data) != "console" {
		t.Fatalf("attach event = %#v", event)
	}
	if err := attach.Close(); err != nil {
		t.Fatal(err)
	}

	cancelAttach, err := client.Attach(context.Background(), "cancel")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = cancelAttach.Receive(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("receive cancellation error = %T %v", err, err)
	}
	_ = cancelAttach.Close()
}

func TestWebSocketHandshakeAPIError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(writer, `{"code":"denied","message":"no websocket access"}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Exec(context.Background(), "box", ExecRequest{Command: []string{"true"}})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("handshake error type = %T (%v)", err, err)
	}
	if apiErr.StatusCode != http.StatusForbidden || apiErr.Code != "denied" || apiErr.Message != "no websocket access" {
		t.Fatalf("handshake API error = %#v", apiErr)
	}
}

func TestProxyWebSocketEscapingAndMessages(t *testing.T) {
	t.Parallel()

	requestURI := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestURI <- request.RequestURI
		connection, err := ws.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		messageType, data, err := connection.Read(request.Context())
		if err != nil {
			return
		}
		_ = connection.Write(request.Context(), messageType, data)
	}))
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	socket, err := client.ProxyWebSocket(
		context.Background(),
		"box/a",
		8080,
		"api/a b",
		"connect secret",
		map[string][]string{"q": {"x&y"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	if err := socket.Write(context.Background(), WebSocketBinaryMessage, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	messageType, data, err := socket.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if messageType != WebSocketBinaryMessage || !bytes.Equal(data, []byte{1, 2, 3}) {
		t.Fatalf("proxy message type=%v data=%v", messageType, data)
	}
	got := <-requestURI
	want := "/v1/sandboxes/box%2Fa/ports/8080/ws/api/a%20b?connect_token=connect+secret&q=x%26y"
	if got != want {
		t.Fatalf("proxy request URI = %q; want %q", got, want)
	}
}
