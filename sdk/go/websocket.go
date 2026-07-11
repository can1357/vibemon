package vmon

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	ws "github.com/coder/websocket"
)

// WebSocketMessageType identifies a text or binary WebSocket message.
type WebSocketMessageType int

const (
	// WebSocketTextMessage identifies a UTF-8 text message.
	WebSocketTextMessage WebSocketMessageType = 1
	// WebSocketBinaryMessage identifies a binary message.
	WebSocketBinaryMessage WebSocketMessageType = 2
)

// WebSocketConn is a bounded, context-aware WebSocket used by port proxies.
type WebSocketConn struct {
	conn      *ws.Conn
	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

// Read waits for one text or binary message.
func (socket *WebSocketConn) Read(ctx context.Context) (WebSocketMessageType, []byte, error) {
	if socket == nil || socket.conn == nil {
		return 0, nil, errors.New("vmon: websocket is not open")
	}
	messageType, data, err := socket.conn.Read(ctx)
	if err != nil {
		return 0, nil, normalizeWebSocketError(ctx, err)
	}
	switch messageType {
	case ws.MessageText:
		return WebSocketTextMessage, data, nil
	case ws.MessageBinary:
		return WebSocketBinaryMessage, data, nil
	default:
		return 0, nil, &ProtocolError{Operation: "read websocket", Message: "unsupported message type"}
	}
}

// Write sends one complete text or binary message.
func (socket *WebSocketConn) Write(
	ctx context.Context,
	messageType WebSocketMessageType,
	data []byte,
) error {
	if socket == nil || socket.conn == nil {
		return errors.New("vmon: websocket is not open")
	}
	var nativeType ws.MessageType
	switch messageType {
	case WebSocketTextMessage:
		nativeType = ws.MessageText
	case WebSocketBinaryMessage:
		nativeType = ws.MessageBinary
	default:
		return fmt.Errorf("vmon: unsupported websocket message type %d", messageType)
	}
	socket.writeMu.Lock()
	defer socket.writeMu.Unlock()
	if err := socket.conn.Write(ctx, nativeType, data); err != nil {
		return normalizeWebSocketError(ctx, err)
	}
	return nil
}

// Close performs a normal WebSocket close and releases the connection.
func (socket *WebSocketConn) Close() error {
	if socket == nil {
		return nil
	}
	socket.closeOnce.Do(func() {
		if socket.conn != nil {
			socket.closeErr = normalizeWebSocketCloseError(
				socket.conn.Close(ws.StatusNormalClosure, ""),
			)
		}
	})
	return socket.closeErr
}

func (socket *WebSocketConn) closeNow() error {
	if socket == nil {
		return nil
	}
	socket.closeOnce.Do(func() {
		if socket.conn != nil {
			socket.closeErr = normalizeWebSocketCloseError(socket.conn.CloseNow())
		}
	})
	return socket.closeErr
}

func normalizeWebSocketCloseError(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	status := ws.CloseStatus(err)
	if status == ws.StatusNormalClosure || status == ws.StatusGoingAway {
		return nil
	}
	return err
}

func normalizeWebSocketError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	status := ws.CloseStatus(err)
	if status == ws.StatusNormalClosure || status == ws.StatusGoingAway {
		return io.EOF
	}
	return fmt.Errorf("vmon: websocket I/O failed: %w", err)
}

func (client *Client) dialWebSocket(ctx context.Context, path string, query url.Values, endpoint string) (*WebSocketConn, string, error) {
	if client == nil || client.driver == nil {
		return nil, "", errors.New("vmon: client has no driver")
	}
	return client.driver.Dial(ctx, path, query, endpoint)
}

type webSocketEnvelope struct {
	Stream string          `json:"stream"`
	Base64 string          `json:"b64"`
	Exit   json.RawMessage `json:"exit"`
	Signal *int            `json:"signal"`
	Ready  string          `json:"ready"`
	Error  *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func readWebSocketEnvelope(ctx context.Context, socket *WebSocketConn, operation string) (webSocketEnvelope, error) {
	_, data, err := socket.Read(ctx)
	if err != nil {
		return webSocketEnvelope{}, err
	}
	var envelope webSocketEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return webSocketEnvelope{}, &ProtocolError{Operation: operation, Message: "invalid JSON frame", Err: err}
	}
	if envelope.Error != nil {
		code := envelope.Error.Code
		if code == "" {
			code = "internal"
		}
		return webSocketEnvelope{}, &APIError{Code: code, Message: envelope.Error.Message}
	}
	return envelope, nil
}

// Process is a live streaming command or shell.
type Process struct {
	socket      *WebSocketConn
	stateMu     sync.Mutex
	stdinClosed bool
	done        bool
	// SandboxID is set for shell processes after the ready frame.
	SandboxID string
}

func openProcess(ctx context.Context, client *Client, path string, request any, endpoint string, consumeReady bool) (*Process, string, error) {
	socket, used, err := client.dialWebSocket(ctx, path, nil, endpoint)
	if err != nil {
		return nil, "", err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		_ = socket.closeNow()
		return nil, "", fmt.Errorf("vmon: encode process request: %w", err)
	}
	if err = socket.Write(ctx, WebSocketTextMessage, encoded); err != nil {
		_ = socket.closeNow()
		return nil, "", err
	}
	process := &Process{socket: socket}
	if consumeReady {
		envelope, readyErr := readWebSocketEnvelope(ctx, socket, "shell setup")
		if readyErr != nil {
			_ = socket.closeNow()
			return nil, "", readyErr
		}
		if envelope.Ready == "" {
			_ = socket.closeNow()
			return nil, "", &ProtocolError{Operation: "shell setup", Message: "missing ready sandbox name"}
		}
		process.SandboxID = envelope.Ready
	}
	return process, used, nil
}

// Shell opens an existing-sandbox or ephemeral shell and consumes its ready frame.
func (client *Client) Shell(ctx context.Context, request ShellRequest) (*Process, error) {
	process, _, err := openProcess(ctx, client, "/v1/shell", request, "", true)
	return process, err
}

func (sandbox *Sandbox) dial(ctx context.Context, path string, query url.Values) (*WebSocketConn, string, error) {
	fullPath := sandboxPath(sandbox.ID) + path
	socket, endpoint, err := sandbox.client.dialWebSocket(ctx, fullPath, query, sandbox.endpoint)
	if endpoint != "" {
		sandbox.endpoint = endpoint
	}
	var apiErr *APIError
	if err == nil || !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound || len(sandbox.client.driver.Endpoints()) <= 1 {
		return socket, endpoint, err
	}
	endpoint, resolveErr := sandbox.client.driver.ResolveSandbox(ctx, sandbox.ID, sandbox.endpoint)
	if resolveErr != nil {
		return nil, "", resolveErr
	}
	sandbox.endpoint = endpoint
	socket, used, err := sandbox.client.dialWebSocket(ctx, fullPath, query, endpoint)
	if used != "" {
		sandbox.endpoint = used
	}
	return socket, used, err
}

// Exec opens a streaming command in this sandbox.
func (sandbox *Sandbox) Exec(ctx context.Context, request ExecRequest) (*Process, error) {
	if len(request.Command) == 0 {
		return nil, errors.New("vmon: exec command must not be empty")
	}
	socket, endpoint, err := sandbox.dial(ctx, "/exec", nil)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		_ = socket.closeNow()
		return nil, err
	}
	if err := socket.Write(ctx, WebSocketTextMessage, encoded); err != nil {
		_ = socket.closeNow()
		return nil, err
	}
	process := &Process{socket: socket}
	if endpoint != "" {
		sandbox.endpoint = endpoint
	}
	return process, nil
}

// Receive waits for one decoded stream or exit frame.
func (process *Process) Receive(ctx context.Context) (ExecEvent, error) {
	if process == nil || process.socket == nil {
		return ExecEvent{}, errors.New("vmon: process is not open")
	}
	process.stateMu.Lock()
	done := process.done
	process.stateMu.Unlock()
	if done {
		return ExecEvent{}, io.EOF
	}
	envelope, err := readWebSocketEnvelope(ctx, process.socket, "process")
	if err != nil {
		_ = process.closeWithState()
		return ExecEvent{}, err
	}
	if envelope.Stream != "" {
		stream := StreamName(envelope.Stream)
		if stream != StreamStdout && stream != StreamStderr && stream != StreamConsole {
			_ = process.closeWithState()
			return ExecEvent{}, &ProtocolError{Operation: "process", Message: "unknown stream name"}
		}
		data, decodeErr := base64.StdEncoding.DecodeString(envelope.Base64)
		if decodeErr != nil {
			_ = process.closeWithState()
			return ExecEvent{}, &ProtocolError{Operation: "process", Message: "invalid base64 stream payload", Err: decodeErr}
		}
		return ExecEvent{Stream: stream, Data: data}, nil
	}
	if len(envelope.Exit) != 0 {
		var code int64
		if err := json.Unmarshal(envelope.Exit, &code); err != nil {
			_ = process.closeWithState()
			return ExecEvent{}, &ProtocolError{Operation: "process", Message: "invalid exit code", Err: err}
		}
		exit := &ExecExit{Code: code, Signal: envelope.Signal}
		_ = process.closeWithState()
		return ExecEvent{Exit: exit}, nil
	}
	_ = process.closeWithState()
	return ExecEvent{}, &ProtocolError{Operation: "process", Message: "unrecognized frame"}
}

// WriteStdin writes a chunk of bytes to the standard input of the process.
func (process *Process) WriteStdin(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	process.stateMu.Lock()
	closed := process.stdinClosed || process.done
	process.stateMu.Unlock()
	if closed {
		return errors.New("vmon: process stdin is closed")
	}
	return process.writeJSON(ctx, struct {
		Stdin string `json:"stdin_b64"`
	}{base64.StdEncoding.EncodeToString(data)})
}

// CloseStdin sends an EOF signal to the standard input of the process.
func (process *Process) CloseStdin(ctx context.Context) error {
	process.stateMu.Lock()
	if process.stdinClosed {
		process.stateMu.Unlock()
		return nil
	}
	if process.done {
		process.stateMu.Unlock()
		return errors.New("vmon: process is closed")
	}
	process.stdinClosed = true
	process.stateMu.Unlock()
	return process.writeJSON(ctx, struct {
		EOF bool `json:"eof"`
	}{true})
}

// Resize updates the terminal size (rows and columns) of the process.
func (process *Process) Resize(ctx context.Context, rows, columns uint16) error {
	return process.writeJSON(ctx, struct {
		Resize [2]uint16 `json:"resize"`
	}{[2]uint16{rows, columns}})
}
func (process *Process) writeJSON(ctx context.Context, value any) error {
	if process == nil || process.socket == nil {
		return errors.New("vmon: process is not open")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err = process.socket.Write(ctx, WebSocketTextMessage, encoded); err != nil {
		_ = process.closeWithState()
	}
	return err
}

// Copy streams stdout and stderr of the process to the provided writers until the process exits.
func (process *Process) Copy(ctx context.Context, stdout, stderr io.Writer) (ExecExit, error) {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	for {
		event, err := process.Receive(ctx)
		if err != nil {
			return ExecExit{}, err
		}
		if event.Exit != nil {
			return *event.Exit, nil
		}
		writer := stdout
		if event.Stream == StreamStderr {
			writer = stderr
		}
		n, err := writer.Write(event.Data)
		if err != nil {
			return ExecExit{}, err
		}
		if n != len(event.Data) {
			return ExecExit{}, io.ErrShortWrite
		}
	}
}

// Wait blocks until the process exits, discarding its output, and returns the exit status.
func (process *Process) Wait(ctx context.Context) (ExecExit, error) {
	return process.Copy(ctx, io.Discard, io.Discard)
}

// Close terminates the process session and releases its resources.
func (process *Process) Close() error { return process.closeWithState() }
func (process *Process) closeWithState() error {
	if process == nil {
		return nil
	}
	process.stateMu.Lock()
	process.done = true
	process.stateMu.Unlock()
	if process.socket == nil {
		return nil
	}
	return process.socket.Close()
}

// ConsoleStream is a live read-only sandbox console.
type ConsoleStream struct{ socket *WebSocketConn }

// Attach connects to the live interactive serial console of the sandbox.
func (sandbox *Sandbox) Attach(ctx context.Context) (*ConsoleStream, error) {
	socket, endpoint, err := sandbox.dial(ctx, "/attach", nil)
	if endpoint != "" {
		sandbox.endpoint = endpoint
	}
	if err != nil {
		return nil, err
	}
	return &ConsoleStream{socket: socket}, nil
}

// Receive reads and decodes the next console event or stream frame.
func (stream *ConsoleStream) Receive(ctx context.Context) (StreamEvent, error) {
	if stream == nil || stream.socket == nil {
		return StreamEvent{}, errors.New("vmon: console stream is not open")
	}
	envelope, err := readWebSocketEnvelope(ctx, stream.socket, "attach")
	if err != nil {
		return StreamEvent{}, err
	}
	name := StreamName(envelope.Stream)
	if name != StreamConsole && name != StreamStdout && name != StreamStderr {
		return StreamEvent{}, &ProtocolError{Operation: "attach", Message: "unknown stream frame"}
	}
	data, err := base64.StdEncoding.DecodeString(envelope.Base64)
	if err != nil {
		return StreamEvent{}, &ProtocolError{Operation: "attach", Message: "invalid base64 stream payload", Err: err}
	}
	return StreamEvent{Stream: name, Data: data}, nil
}

// Close terminates the console stream session.
func (stream *ConsoleStream) Close() error {
	if stream == nil || stream.socket == nil {
		return nil
	}
	return stream.socket.Close()
}

// WebSocket opens a connect-token-authenticated guest port websocket.
func (ports *Ports) WebSocket(ctx context.Context, port uint16, path string, query url.Values) (*WebSocketConn, error) {
	token, err := ports.token(ctx)
	if err != nil {
		return nil, err
	}
	query = sanitizeProxyQuery(query)
	query.Set("connect_token", token)
	socket, endpoint, err := ports.sandbox.dial(ctx, "/ports/"+strconv.FormatUint(uint64(port), 10)+"/ws/"+escapeRestPath(path), query)
	if endpoint != "" {
		ports.sandbox.endpoint = endpoint
	}
	return socket, err
}
