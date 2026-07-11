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

func (client *Client) dialWebSocket(
	ctx context.Context,
	escapedPath string,
	query url.Values,
) (*WebSocketConn, error) {
	endpoint, err := client.endpoint(escapedPath, query)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("vmon: parse websocket endpoint: %w", err)
	}
	if parsed.Scheme == "http" {
		parsed.Scheme = "ws"
	} else {
		parsed.Scheme = "wss"
	}
	header := make(http.Header)
	client.applyHeaders(header)
	connection, response, err := ws.Dial(ctx, parsed.String(), &ws.DialOptions{
		HTTPClient: client.httpClient,
		HTTPHeader: header,
	})
	if err != nil {
		if connection != nil {
			_ = connection.CloseNow()
		}
		if response != nil {
			return nil, apiErrorFromResponse(response)
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		return nil, fmt.Errorf("vmon: websocket handshake failed: %w", err)
	}
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	connection.SetReadLimit(client.maxResponseBytes)
	return &WebSocketConn{conn: connection}, nil
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

func readWebSocketEnvelope(
	ctx context.Context,
	socket *WebSocketConn,
	operation string,
) (webSocketEnvelope, error) {
	_, data, err := socket.Read(ctx)
	if err != nil {
		return webSocketEnvelope{}, err
	}
	var envelope webSocketEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return webSocketEnvelope{}, &ProtocolError{
			Operation: operation,
			Message:   "invalid JSON frame",
			Err:       err,
		}
	}
	if envelope.Error != nil {
		code := envelope.Error.Code
		if code == "" {
			code = "internal"
		}
		message := envelope.Error.Message
		if message == "" {
			message = "daemon websocket operation failed"
		}
		return webSocketEnvelope{}, &APIError{Code: code, Message: message}
	}
	return envelope, nil
}

// ExecSession is a live streaming exec WebSocket.
type ExecSession struct {
	socket      *WebSocketConn
	stateMu     sync.Mutex
	stdinClosed bool
	done        bool
}

// Exec opens a streaming exec and sends its typed first protocol frame.
func (client *Client) Exec(
	ctx context.Context,
	id string,
	request ExecRequest,
) (*ExecSession, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	if len(request.Command) == 0 || request.Command[0] == "" {
		return nil, errors.New("vmon: exec command must not be empty")
	}
	socket, err := client.dialWebSocket(ctx, sandboxPath(id)+"/exec", nil)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		_ = socket.closeNow()
		return nil, fmt.Errorf("vmon: encode exec request: %w", err)
	}
	if err := socket.Write(ctx, WebSocketTextMessage, encoded); err != nil {
		_ = socket.closeNow()
		return nil, err
	}
	return &ExecSession{socket: socket}, nil
}

// Receive waits for one decoded stream or exit event.
func (session *ExecSession) Receive(ctx context.Context) (ExecEvent, error) {
	if session == nil || session.socket == nil {
		return ExecEvent{}, errors.New("vmon: exec session is not open")
	}
	session.stateMu.Lock()
	if session.done {
		session.stateMu.Unlock()
		return ExecEvent{}, io.EOF
	}
	session.stateMu.Unlock()
	envelope, err := readWebSocketEnvelope(ctx, session.socket, "streaming exec")
	if err != nil {
		_ = session.closeWithState()
		return ExecEvent{}, err
	}
	if envelope.Stream != "" {
		stream := StreamName(envelope.Stream)
		if stream != StreamStdout && stream != StreamStderr && stream != StreamConsole {
			_ = session.closeWithState()
			return ExecEvent{}, &ProtocolError{Operation: "streaming exec", Message: "unknown stream name"}
		}
		data, err := base64.StdEncoding.DecodeString(envelope.Base64)
		if err != nil {
			_ = session.closeWithState()
			return ExecEvent{}, &ProtocolError{
				Operation: "streaming exec",
				Message:   "invalid base64 stream payload",
				Err:       err,
			}
		}
		return ExecEvent{Stream: stream, Data: data}, nil
	}
	if len(envelope.Exit) != 0 {
		var code int64
		if err := json.Unmarshal(envelope.Exit, &code); err != nil {
			_ = session.closeWithState()
			return ExecEvent{}, &ProtocolError{
				Operation: "streaming exec",
				Message:   "invalid exit code",
				Err:       err,
			}
		}
		exit := &ExecExit{Code: code, Signal: envelope.Signal}
		_ = session.closeWithState()
		return ExecEvent{Exit: exit}, nil
	}
	_ = session.closeWithState()
	return ExecEvent{}, &ProtocolError{Operation: "streaming exec", Message: "unrecognized frame"}
}

// WriteStdin sends bytes to the running command's standard input.
func (session *ExecSession) WriteStdin(ctx context.Context, data []byte) error {
	if session == nil || session.socket == nil {
		return errors.New("vmon: exec session is not open")
	}
	if len(data) == 0 {
		return nil
	}
	session.stateMu.Lock()
	closed := session.stdinClosed || session.done
	session.stateMu.Unlock()
	if closed {
		return errors.New("vmon: exec stdin is closed")
	}
	return session.writeJSON(ctx, struct {
		Stdin string `json:"stdin_b64"`
	}{Stdin: base64.StdEncoding.EncodeToString(data)})
}

// CloseStdin sends the exec EOF frame exactly once.
func (session *ExecSession) CloseStdin(ctx context.Context) error {
	if session == nil || session.socket == nil {
		return errors.New("vmon: exec session is not open")
	}
	session.stateMu.Lock()
	if session.stdinClosed {
		session.stateMu.Unlock()
		return nil
	}
	if session.done {
		session.stateMu.Unlock()
		return errors.New("vmon: exec session is closed")
	}
	session.stdinClosed = true
	session.stateMu.Unlock()
	if err := session.writeJSON(ctx, struct {
		EOF bool `json:"eof"`
	}{EOF: true}); err != nil {
		return err
	}
	return nil
}

// Resize changes the dimensions of a TTY exec session.
func (session *ExecSession) Resize(ctx context.Context, rows, columns uint16) error {
	if session == nil || session.socket == nil {
		return errors.New("vmon: exec session is not open")
	}
	return session.writeJSON(ctx, struct {
		Resize [2]uint16 `json:"resize"`
	}{Resize: [2]uint16{rows, columns}})
}

func (session *ExecSession) writeJSON(ctx context.Context, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("vmon: encode websocket frame: %w", err)
	}
	if err := session.socket.Write(ctx, WebSocketTextMessage, encoded); err != nil {
		_ = session.closeWithState()
		return err
	}
	return nil
}

// Copy copies stream events to the supplied writers until the process exits.
func (session *ExecSession) Copy(
	ctx context.Context,
	stdout io.Writer,
	stderr io.Writer,
) (ExecExit, error) {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	for {
		event, err := session.Receive(ctx)
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
		written, err := writer.Write(event.Data)
		if err != nil {
			_ = session.Close()
			return ExecExit{}, err
		}
		if written != len(event.Data) {
			_ = session.Close()
			return ExecExit{}, io.ErrShortWrite
		}
	}
}

// Wait drains process output and waits for its terminal event.
func (session *ExecSession) Wait(ctx context.Context) (ExecExit, error) {
	return session.Copy(ctx, io.Discard, io.Discard)
}

// Close closes the exec WebSocket and releases the session.
func (session *ExecSession) Close() error {
	return session.closeWithState()
}

func (session *ExecSession) closeWithState() error {
	if session == nil {
		return nil
	}
	session.stateMu.Lock()
	session.done = true
	session.stateMu.Unlock()
	if session.socket == nil {
		return nil
	}
	return session.socket.Close()
}

// AttachSession is a live read-only sandbox console attachment.
type AttachSession struct {
	socket *WebSocketConn
}

// Attach opens a streaming console attachment for a sandbox.
func (client *Client) Attach(ctx context.Context, id string) (*AttachSession, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	socket, err := client.dialWebSocket(ctx, sandboxPath(id)+"/attach", nil)
	if err != nil {
		return nil, err
	}
	return &AttachSession{socket: socket}, nil
}

// Receive waits for one decoded console attachment event.
func (session *AttachSession) Receive(ctx context.Context) (StreamEvent, error) {
	if session == nil || session.socket == nil {
		return StreamEvent{}, errors.New("vmon: attach session is not open")
	}
	envelope, err := readWebSocketEnvelope(ctx, session.socket, "attach")
	if err != nil {
		_ = session.socket.closeNow()
		return StreamEvent{}, err
	}
	stream := StreamName(envelope.Stream)
	if stream != StreamConsole && stream != StreamStdout && stream != StreamStderr {
		_ = session.socket.closeNow()
		return StreamEvent{}, &ProtocolError{Operation: "attach", Message: "unknown stream frame"}
	}
	data, err := base64.StdEncoding.DecodeString(envelope.Base64)
	if err != nil {
		_ = session.socket.closeNow()
		return StreamEvent{}, &ProtocolError{Operation: "attach", Message: "invalid base64 stream payload", Err: err}
	}
	return StreamEvent{Stream: stream, Data: data}, nil
}

// Close closes the console attachment.
func (session *AttachSession) Close() error {
	if session == nil || session.socket == nil {
		return nil
	}
	return session.socket.Close()
}

// ShellSession is a live shell exec plus the backing sandbox identifier.
type ShellSession struct {
	// SandboxID is the ready sandbox name returned by the daemon.
	SandboxID string
	// ExecSession carries shell input, output, resize, and exit frames.
	*ExecSession
}

// Shell opens an existing-sandbox or ephemeral WebSocket shell.
func (client *Client) Shell(ctx context.Context, request ShellRequest) (*ShellSession, error) {
	socket, err := client.dialWebSocket(ctx, "/v1/shell", nil)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		_ = socket.closeNow()
		return nil, fmt.Errorf("vmon: encode shell request: %w", err)
	}
	if err := socket.Write(ctx, WebSocketTextMessage, encoded); err != nil {
		_ = socket.closeNow()
		return nil, err
	}
	envelope, err := readWebSocketEnvelope(ctx, socket, "shell setup")
	if err != nil {
		_ = socket.closeNow()
		return nil, err
	}
	if envelope.Ready == "" {
		_ = socket.closeNow()
		return nil, &ProtocolError{Operation: "shell setup", Message: "missing ready sandbox name"}
	}
	return &ShellSession{
		SandboxID:   envelope.Ready,
		ExecSession: &ExecSession{socket: socket},
	}, nil
}

// Exec opens a streaming exec in this sandbox.
func (sandbox *Sandbox) Exec(ctx context.Context, request ExecRequest) (*ExecSession, error) {
	client, err := sandbox.boundClient()
	if err != nil {
		return nil, err
	}
	return client.Exec(ctx, sandbox.Identifier(), request)
}

// Attach opens a console attachment for this sandbox.
func (sandbox *Sandbox) Attach(ctx context.Context) (*AttachSession, error) {
	client, err := sandbox.boundClient()
	if err != nil {
		return nil, err
	}
	return client.Attach(ctx, sandbox.Identifier())
}

// ProxyWebSocket opens a connect-token-authenticated WebSocket to an exposed guest port.
func (client *Client) ProxyWebSocket(
	ctx context.Context,
	id string,
	port uint16,
	rest string,
	connectToken string,
	query url.Values,
) (*WebSocketConn, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	if port == 0 {
		return nil, errors.New("vmon: proxy port must not be zero")
	}
	if connectToken == "" {
		return nil, errors.New("vmon: connect token must not be empty")
	}
	path := sandboxPath(id) + "/ports/" + strconv.FormatUint(uint64(port), 10) + "/ws"
	if escapedRest := escapeRestPath(rest); escapedRest != "" {
		path += "/" + escapedRest
	}
	proxyQuery := cloneValues(query)
	proxyQuery.Set("connect_token", connectToken)
	return client.dialWebSocket(ctx, path, proxyQuery)
}
