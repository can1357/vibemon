package vmon

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
)

type hostGatewayServiceStub struct {
	pb.UnimplementedSandboxServiceServer
	hostGateway func(grpc.BidiStreamingServer[pb.HostGatewayInput, pb.HostGatewayOutput]) error
}

func (stub *hostGatewayServiceStub) HostGateway(stream grpc.BidiStreamingServer[pb.HostGatewayInput, pb.HostGatewayOutput]) error {
	return stub.hostGateway(stream)
}

func startHostGatewayStub(t *testing.T, handler func(grpc.BidiStreamingServer[pb.HostGatewayInput, pb.HostGatewayOutput]) error) *Client {
	t.Helper()
	listener := startGRPCServices(t, func(server *grpc.Server) {
		pb.RegisterSandboxServiceServer(server, &hostGatewayServiceStub{hostGateway: handler})
	})
	return bufconnClient(t, listener)
}

func receiveWithin[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for gateway fixture")
		var zero T
		return zero
	}
}

func TestHostGatewayRelaysBidirectionallyAndCloses(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	targetRead := make(chan []byte, 1)
	targetClosed := make(chan error, 1)
	go func() {
		conn, acceptErr := target.Accept()
		if acceptErr != nil {
			targetClosed <- acceptErr
			return
		}
		defer conn.Close()
		data := make([]byte, len("from-guest"))
		_, readErr := io.ReadFull(conn, data)
		if readErr != nil {
			targetClosed <- readErr
			return
		}
		targetRead <- data
		if _, writeErr := conn.Write([]byte("from-runner")); writeErr != nil {
			targetClosed <- writeErr
			return
		}
		one := make([]byte, 1)
		_, readErr = conn.Read(one)
		targetClosed <- readErr
	}()

	returned := make(chan []byte, 1)
	serverDone := make(chan struct{})
	client := startHostGatewayStub(t, func(stream grpc.BidiStreamingServer[pb.HostGatewayInput, pb.HostGatewayOutput]) error {
		defer close(serverDone)
		first, recvErr := stream.Recv()
		if recvErr != nil {
			return recvErr
		}
		if attach := first.GetAttach(); attach == nil || attach.GetSandboxId() != "box" {
			t.Errorf("attach = %#v", first)
			return nil
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Ready{
			Ready: &pb.HostGatewayReady{Url: "http://172.16.0.1:18080"},
		}}); sendErr != nil {
			return sendErr
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Open{
			Open: &pb.HostGatewayOpen{Conn: 7},
		}}); sendErr != nil {
			return sendErr
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Data{
			Data: &pb.HostGatewayData{Conn: 7, Data: []byte("from-guest")},
		}}); sendErr != nil {
			return sendErr
		}
		input, recvErr := stream.Recv()
		if recvErr != nil {
			return recvErr
		}
		data := input.GetData()
		if data == nil || data.GetConn() != 7 {
			t.Errorf("relay response = %#v", input)
			return nil
		}
		returned <- append([]byte(nil), data.GetData()...)
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Close{
			Close: &pb.HostGatewayClose{Conn: 7},
		}}); sendErr != nil {
			return sendErr
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	gateway, err := client.Sandboxes.Ref("box").HostGateway(context.Background(), target.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if gateway.URL != "http://172.16.0.1:18080" {
		t.Fatalf("URL = %q", gateway.URL)
	}
	if got := receiveWithin(t, targetRead); string(got) != "from-guest" {
		t.Fatalf("target read %q", got)
	}
	if got := receiveWithin(t, returned); string(got) != "from-runner" {
		t.Fatalf("gateway returned %q", got)
	}
	if closeErr := receiveWithin(t, targetClosed); !errors.Is(closeErr, io.EOF) && !errors.Is(closeErr, net.ErrClosed) {
		t.Fatalf("target close error = %v", closeErr)
	}
	if err := gateway.Close(); err != nil {
		t.Fatal(err)
	}
	receiveWithin(t, serverDone)
}

func TestHostGatewayCloseShutsDownTarget(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	targetAccepted := make(chan struct{})
	targetClosed := make(chan error, 1)
	go func() {
		conn, acceptErr := target.Accept()
		if acceptErr != nil {
			targetClosed <- acceptErr
			return
		}
		close(targetAccepted)
		defer conn.Close()
		_, readErr := conn.Read(make([]byte, 1))
		targetClosed <- readErr
	}()

	serverDone := make(chan struct{})
	client := startHostGatewayStub(t, func(stream grpc.BidiStreamingServer[pb.HostGatewayInput, pb.HostGatewayOutput]) error {
		defer close(serverDone)
		if _, recvErr := stream.Recv(); recvErr != nil {
			return recvErr
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Ready{
			Ready: &pb.HostGatewayReady{Url: "http://172.16.0.1:18080"},
		}}); sendErr != nil {
			return sendErr
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Open{
			Open: &pb.HostGatewayOpen{Conn: 9},
		}}); sendErr != nil {
			return sendErr
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})
	gateway, err := client.Sandboxes.Ref("box").HostGateway(context.Background(), target.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	receiveWithin(t, targetAccepted)
	if err := gateway.Close(); err != nil {
		t.Fatal(err)
	}
	if closeErr := receiveWithin(t, targetClosed); !errors.Is(closeErr, io.EOF) && !errors.Is(closeErr, net.ErrClosed) {
		t.Fatalf("target close error = %v", closeErr)
	}
	receiveWithin(t, serverDone)
}

func TestHostGatewayDialFailureSendsClose(t *testing.T) {
	unused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := unused.Addr().String()
	if err := unused.Close(); err != nil {
		t.Fatal(err)
	}
	closed := make(chan uint64, 1)
	client := startHostGatewayStub(t, func(stream grpc.BidiStreamingServer[pb.HostGatewayInput, pb.HostGatewayOutput]) error {
		if _, recvErr := stream.Recv(); recvErr != nil {
			return recvErr
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Ready{
			Ready: &pb.HostGatewayReady{Url: "http://172.16.0.1:18080"},
		}}); sendErr != nil {
			return sendErr
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Open{
			Open: &pb.HostGatewayOpen{Conn: 41},
		}}); sendErr != nil {
			return sendErr
		}
		input, recvErr := stream.Recv()
		if recvErr != nil {
			return recvErr
		}
		if closeFrame := input.GetClose(); closeFrame != nil {
			closed <- closeFrame.GetConn()
			if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Close{
				Close: &pb.HostGatewayClose{Conn: closeFrame.GetConn()},
			}}); sendErr != nil {
				return sendErr
			}
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})
	gateway, err := client.Sandboxes.Ref("box").HostGateway(context.Background(), address)
	if err != nil {
		t.Fatal(err)
	}
	if id := receiveWithin(t, closed); id != 41 {
		t.Fatalf("closed connection = %d", id)
	}
	if err := gateway.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHostGatewayAcceptsReciprocalCloseAfterDialFailure(t *testing.T) {
	unused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := unused.Addr().String()
	if err := unused.Close(); err != nil {
		t.Fatal(err)
	}
	client := startHostGatewayStub(t, func(stream grpc.BidiStreamingServer[pb.HostGatewayInput, pb.HostGatewayOutput]) error {
		if _, recvErr := stream.Recv(); recvErr != nil {
			return recvErr
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Ready{
			Ready: &pb.HostGatewayReady{Url: "http://172.16.0.1:18080"},
		}}); sendErr != nil {
			return sendErr
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Open{
			Open: &pb.HostGatewayOpen{Conn: 41},
		}}); sendErr != nil {
			return sendErr
		}
		input, recvErr := stream.Recv()
		if recvErr != nil {
			return recvErr
		}
		if closeFrame := input.GetClose(); closeFrame == nil || closeFrame.GetConn() != 41 {
			t.Errorf("client close = %#v", input)
			return nil
		}
		if sendErr := stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Close{
			Close: &pb.HostGatewayClose{Conn: 41},
		}}); sendErr != nil {
			return sendErr
		}
		return stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Close{
			Close: &pb.HostGatewayClose{Conn: 99},
		}})
	})
	gateway, err := client.Sandboxes.Ref("box").HostGateway(context.Background(), address)
	if err != nil {
		t.Fatal(err)
	}
	receiveWithin(t, gateway.done)
	var protocolErr *ProtocolError
	if err := gateway.Close(); !errors.As(err, &protocolErr) {
		t.Fatalf("Close() error = %T %v", err, err)
	}
	if protocolErr.Message != "close for unknown connection 99" {
		t.Fatalf("protocol error = %q", protocolErr.Message)
	}
}

func TestHostGatewayRejectsInvalidOrdering(t *testing.T) {
	t.Run("before ready", func(t *testing.T) {
		client := startHostGatewayStub(t, func(stream grpc.BidiStreamingServer[pb.HostGatewayInput, pb.HostGatewayOutput]) error {
			if _, err := stream.Recv(); err != nil {
				return err
			}
			return stream.Send(&pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Open{
				Open: &pb.HostGatewayOpen{Conn: 1},
			}})
		})
		gateway, err := client.Sandboxes.Ref("box").HostGateway(context.Background(), ":8080")
		var protocolErr *ProtocolError
		if gateway != nil || !errors.As(err, &protocolErr) {
			t.Fatalf("HostGateway() = %#v, %T %v", gateway, err, err)
		}
	})

	t.Run("after ready", func(t *testing.T) {
		client := startHostGatewayStub(t, func(stream grpc.BidiStreamingServer[pb.HostGatewayInput, pb.HostGatewayOutput]) error {
			if _, err := stream.Recv(); err != nil {
				return err
			}
			ready := &pb.HostGatewayOutput{Output: &pb.HostGatewayOutput_Ready{
				Ready: &pb.HostGatewayReady{Url: "http://172.16.0.1:18080"},
			}}
			if err := stream.Send(ready); err != nil {
				return err
			}
			if err := stream.Send(ready); err != nil {
				return err
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		})
		gateway, err := client.Sandboxes.Ref("box").HostGateway(context.Background(), ":8080")
		if err != nil {
			t.Fatal(err)
		}
		receiveWithin(t, gateway.done)
		var protocolErr *ProtocolError
		if err := gateway.Close(); !errors.As(err, &protocolErr) {
			t.Fatalf("Close() error = %T %v", err, err)
		}
	})
}

func TestParseHostGatewayTarget(t *testing.T) {
	for input, expected := range map[string]string{
		"service.internal:4000":    "service.internal:4000",
		":4000":                    "127.0.0.1:4000",
		"http://127.0.0.1:4000":    "127.0.0.1:4000",
		"https://service.internal": "service.internal:443",
		"http://[::1]":             "[::1]:80",
		"[::1]:4000":               "[::1]:4000",
	} {
		target, err := parseHostGatewayTarget(input)
		if err != nil || target.address() != expected {
			t.Errorf("parseHostGatewayTarget(%q) = %#v, %v; address %q", input, target, err, target.address())
		}
	}
	invalid := []string{"service.internal", "ftp://service.internal", "http://", "http://service.internal/path", "http://service.internal?x=1", "http://service.internal#x", "[::1", "service.internal:0"}
	for _, input := range invalid {
		if target, err := parseHostGatewayTarget(input); err == nil || !reflect.DeepEqual(target, hostGatewayTarget{}) {
			t.Errorf("parseHostGatewayTarget(%q) = %#v, %v", input, target, err)
		}
	}
}
