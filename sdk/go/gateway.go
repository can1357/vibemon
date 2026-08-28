package vmon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
)

const hostGatewayOperation = "host gateway"

type hostGatewayTarget struct {
	host string
	port uint16
}

func (target hostGatewayTarget) address() string {
	return net.JoinHostPort(target.host, strconv.FormatUint(uint64(target.port), 10))
}

func parseHostGatewayTarget(value string) (hostGatewayTarget, error) {
	authority := value
	var defaultPort uint16
	switch {
	case strings.HasPrefix(value, "http://"):
		authority, defaultPort = strings.TrimPrefix(value, "http://"), 80
	case strings.HasPrefix(value, "https://"):
		authority, defaultPort = strings.TrimPrefix(value, "https://"), 443
	case strings.Contains(value, "://"):
		return hostGatewayTarget{}, errors.New("vmon: gateway target URL must use http or https")
	}
	if strings.ContainsAny(authority, "/?#") {
		return hostGatewayTarget{}, errors.New("vmon: gateway target URL must not contain a path, query, or fragment")
	}

	var host, portText string
	if strings.HasPrefix(authority, "[") {
		end := strings.IndexByte(authority, ']')
		if end < 0 {
			return hostGatewayTarget{}, errors.New("vmon: gateway target has an invalid IPv6 address")
		}
		host = authority[1:end]
		suffix := authority[end+1:]
		if suffix == "" {
			if defaultPort == 0 {
				return hostGatewayTarget{}, errors.New("vmon: gateway target requires a port")
			}
			if host == "" {
				return hostGatewayTarget{}, errors.New("vmon: gateway target requires a non-empty host and nonzero port")
			}
			return hostGatewayTarget{host: host, port: defaultPort}, nil
		}
		if !strings.HasPrefix(suffix, ":") {
			return hostGatewayTarget{}, errors.New("vmon: gateway target has an invalid port")
		}
		portText = suffix[1:]
	} else if colon := strings.LastIndexByte(authority, ':'); colon >= 0 {
		host, portText = authority[:colon], authority[colon+1:]
		if host == "" {
			host = "127.0.0.1"
		}
	} else {
		host = authority
		if defaultPort == 0 {
			return hostGatewayTarget{}, errors.New("vmon: gateway target requires a port")
		}
		if host == "" {
			return hostGatewayTarget{}, errors.New("vmon: gateway target requires a non-empty host and nonzero port")
		}
		return hostGatewayTarget{host: host, port: defaultPort}, nil
	}

	parsedPort, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return hostGatewayTarget{}, errors.New("vmon: gateway target has an invalid port")
	}
	if host == "" || parsedPort == 0 {
		return hostGatewayTarget{}, errors.New("vmon: gateway target requires a non-empty host and nonzero port")
	}
	return hostGatewayTarget{host: host, port: uint16(parsedPort)}, nil
}

// HostGateway is a live relay from a sandbox-private listener to a runner-owned TCP target.
type HostGateway struct {
	// URL is the guest-visible gateway listener URL.
	URL string

	stream grpc.BidiStreamingClient[pb.HostGatewayInput, pb.HostGatewayOutput]
	target string
	ctx    context.Context
	cancel context.CancelFunc

	sendMu        sync.Mutex
	connMu        sync.Mutex
	conns         map[uint64]*hostGatewayConnection
	locallyClosed map[uint64]struct{}
	connWG        sync.WaitGroup

	errMu     sync.Mutex
	runErr    error
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

type hostGatewayConnection struct {
	conn      net.Conn
	writes    chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func (connection *hostGatewayConnection) close() {
	connection.closeOnce.Do(func() {
		close(connection.done)
		_ = connection.conn.Close()
	})
}

// HostGateway attaches the sandbox gateway and relays its connections to target.
// Target accepts host:port, :port, or an HTTP(S) URL without a path, query, or fragment.
func (sandbox *Sandbox) HostGateway(ctx context.Context, target string) (*HostGateway, error) {
	parsedTarget, err := parseHostGatewayTarget(target)
	if err != nil {
		return nil, err
	}
	conn, err := sandbox.streamConn(ctx)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := pb.NewSandboxServiceClient(conn).HostGateway(streamCtx)
	if err != nil {
		cancel()
		return nil, apiErrorFromStatus(err, hostGatewayOperation)
	}
	gateway := &HostGateway{
		stream:        stream,
		target:        parsedTarget.address(),
		ctx:           streamCtx,
		cancel:        cancel,
		conns:         make(map[uint64]*hostGatewayConnection),
		locallyClosed: make(map[uint64]struct{}),
		done:          make(chan struct{}),
	}
	if err := gateway.send(&pb.HostGatewayInput{Input: &pb.HostGatewayInput_Attach{
		Attach: &pb.HostGatewayAttach{SandboxId: sandbox.ID},
	}}); err != nil {
		cancel()
		return nil, err
	}
	first, err := stream.Recv()
	if err != nil {
		cancel()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, apiErrorFromStatus(err, hostGatewayOperation, stream.Trailer())
	}
	ready, ok := first.GetOutput().(*pb.HostGatewayOutput_Ready)
	if !ok || ready.Ready == nil || ready.Ready.GetUrl() == "" {
		cancel()
		return nil, &ProtocolError{Operation: hostGatewayOperation, Message: "missing ready frame"}
	}
	gateway.URL = ready.Ready.GetUrl()
	go gateway.receive()
	return gateway, nil
}

func (gateway *HostGateway) send(input *pb.HostGatewayInput) error {
	gateway.sendMu.Lock()
	defer gateway.sendMu.Unlock()
	if err := gateway.stream.Send(input); err != nil {
		if errors.Is(err, io.EOF) {
			return &ProtocolError{Operation: hostGatewayOperation, Message: "gateway stream closed"}
		}
		return apiErrorFromStatus(err, hostGatewayOperation, gateway.stream.Trailer())
	}
	return nil
}

func (gateway *HostGateway) receive() {
	defer func() {
		gateway.cancel()
		gateway.closeConnections()
		gateway.connWG.Wait()
		close(gateway.done)
	}()
	for {
		output, err := gateway.stream.Recv()
		if err != nil {
			if gateway.ctx.Err() == nil {
				if errors.Is(err, io.EOF) {
					gateway.setError(&ProtocolError{Operation: hostGatewayOperation, Message: "gateway stream closed"})
				} else {
					gateway.setError(apiErrorFromStatus(err, hostGatewayOperation, gateway.stream.Trailer()))
				}
			}
			return
		}
		switch frame := output.GetOutput().(type) {
		case *pb.HostGatewayOutput_Open:
			if frame.Open == nil {
				gateway.fail(&ProtocolError{Operation: hostGatewayOperation, Message: "invalid gateway output frame"})
				return
			}
			if !gateway.open(frame.Open.GetConn()) {
				return
			}
		case *pb.HostGatewayOutput_Data:
			if frame.Data == nil {
				gateway.fail(&ProtocolError{Operation: hostGatewayOperation, Message: "invalid gateway output frame"})
				return
			}
			if !gateway.write(frame.Data.GetConn(), frame.Data.GetData()) {
				return
			}
		case *pb.HostGatewayOutput_Close:
			if frame.Close == nil {
				gateway.fail(&ProtocolError{Operation: hostGatewayOperation, Message: "invalid gateway output frame"})
				return
			}
			if !gateway.closeConnection(frame.Close.GetConn()) {
				return
			}
		default:
			gateway.fail(&ProtocolError{Operation: hostGatewayOperation, Message: "invalid gateway output frame"})
			return
		}
	}
}

func (gateway *HostGateway) open(id uint64) bool {
	gateway.connMu.Lock()
	_, exists := gateway.conns[id]
	_, closing := gateway.locallyClosed[id]
	gateway.connMu.Unlock()
	if exists || closing {
		gateway.fail(&ProtocolError{Operation: hostGatewayOperation, Message: fmt.Sprintf("duplicate open for connection %d", id)})
		return false
	}
	conn, err := (&net.Dialer{}).DialContext(gateway.ctx, "tcp", gateway.target)
	if err != nil {
		gateway.connMu.Lock()
		gateway.locallyClosed[id] = struct{}{}
		gateway.connMu.Unlock()
		if sendErr := gateway.sendClose(id); sendErr != nil && gateway.ctx.Err() == nil {
			gateway.fail(sendErr)
			return false
		}
		return true
	}
	connection := &hostGatewayConnection{conn: conn, writes: make(chan []byte, 32), done: make(chan struct{})}
	gateway.connMu.Lock()
	_, exists = gateway.conns[id]
	_, closing = gateway.locallyClosed[id]
	if exists || closing {
		gateway.connMu.Unlock()
		connection.close()
		gateway.fail(&ProtocolError{Operation: hostGatewayOperation, Message: fmt.Sprintf("duplicate open for connection %d", id)})
		return false
	}
	gateway.conns[id] = connection
	gateway.connWG.Add(2)
	gateway.connMu.Unlock()
	go gateway.readTarget(id, connection)
	go gateway.writeTarget(id, connection)
	return true
}

func (gateway *HostGateway) write(id uint64, data []byte) bool {
	gateway.connMu.Lock()
	connection := gateway.conns[id]
	gateway.connMu.Unlock()
	if connection == nil {
		gateway.fail(&ProtocolError{Operation: hostGatewayOperation, Message: fmt.Sprintf("data for unknown connection %d", id)})
		return false
	}
	payload := append([]byte(nil), data...)
	select {
	case connection.writes <- payload:
		return true
	case <-connection.done:
		gateway.fail(&ProtocolError{Operation: hostGatewayOperation, Message: fmt.Sprintf("data for closed connection %d", id)})
		return false
	case <-gateway.ctx.Done():
		return false
	}
}

func (gateway *HostGateway) closeConnection(id uint64) bool {
	gateway.connMu.Lock()
	connection := gateway.conns[id]
	if connection != nil {
		delete(gateway.conns, id)
		gateway.connMu.Unlock()
		connection.close()
		return true
	}
	if _, locallyClosed := gateway.locallyClosed[id]; locallyClosed {
		delete(gateway.locallyClosed, id)
		gateway.connMu.Unlock()
		return true
	}
	gateway.connMu.Unlock()
	gateway.fail(&ProtocolError{Operation: hostGatewayOperation, Message: fmt.Sprintf("close for unknown connection %d", id)})
	return false
}

func (gateway *HostGateway) readTarget(id uint64, connection *hostGatewayConnection) {
	defer gateway.connWG.Done()
	buffer := make([]byte, 32*1024)
	for {
		count, err := connection.conn.Read(buffer)
		if count > 0 {
			data := append([]byte(nil), buffer[:count]...)
			if sendErr := gateway.send(&pb.HostGatewayInput{Input: &pb.HostGatewayInput_Data{
				Data: &pb.HostGatewayData{Conn: id, Data: data},
			}}); sendErr != nil {
				if gateway.ctx.Err() == nil {
					gateway.fail(sendErr)
				}
				return
			}
		}
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				gateway.targetFailed(id, connection)
			}
			return
		}
	}
}

func (gateway *HostGateway) writeTarget(id uint64, connection *hostGatewayConnection) {
	defer gateway.connWG.Done()
	for {
		select {
		case data := <-connection.writes:
			for len(data) != 0 {
				written, err := connection.conn.Write(data)
				if err != nil {
					if !errors.Is(err, net.ErrClosed) {
						gateway.targetFailed(id, connection)
					}
					return
				}
				if written == 0 {
					gateway.targetFailed(id, connection)
					return
				}
				data = data[written:]
			}
		case <-connection.done:
			return
		case <-gateway.ctx.Done():
			return
		}
	}
}

func (gateway *HostGateway) targetFailed(id uint64, connection *hostGatewayConnection) {
	gateway.connMu.Lock()
	if gateway.conns[id] != connection {
		gateway.connMu.Unlock()
		return
	}
	delete(gateway.conns, id)
	gateway.locallyClosed[id] = struct{}{}
	gateway.connMu.Unlock()
	connection.close()
	if err := gateway.sendClose(id); err != nil && gateway.ctx.Err() == nil {
		gateway.fail(err)
	}
}

func (gateway *HostGateway) sendClose(id uint64) error {
	return gateway.send(&pb.HostGatewayInput{Input: &pb.HostGatewayInput_Close{
		Close: &pb.HostGatewayClose{Conn: id},
	}})
}

func (gateway *HostGateway) closeConnections() {
	gateway.connMu.Lock()
	connections := gateway.conns
	gateway.conns = make(map[uint64]*hostGatewayConnection)
	gateway.locallyClosed = make(map[uint64]struct{})
	gateway.connMu.Unlock()
	for _, connection := range connections {
		connection.close()
	}
}

func (gateway *HostGateway) setError(err error) {
	if err == nil {
		return
	}
	gateway.errMu.Lock()
	if gateway.runErr == nil {
		gateway.runErr = err
	}
	gateway.errMu.Unlock()
}

func (gateway *HostGateway) fail(err error) {
	gateway.setError(err)
	gateway.cancel()
}

// Close cancels the gateway RPC and closes every relayed target connection.
func (gateway *HostGateway) Close() error {
	if gateway == nil {
		return nil
	}
	gateway.closeOnce.Do(func() {
		gateway.cancel()
		<-gateway.done
		gateway.errMu.Lock()
		gateway.closeErr = gateway.runErr
		gateway.errMu.Unlock()
	})
	return gateway.closeErr
}
