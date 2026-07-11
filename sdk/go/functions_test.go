package vmon

import (
	"context"
	"sync"
	"testing"

	pb "github.com/can1357/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
)

type functionStub struct{ pb.UnimplementedFunctionServiceServer }

func (stub *functionStub) Get(context.Context, *pb.GetFunctionRequest) (*pb.FunctionRevision, error) {
	return &pb.FunctionRevision{Ref: &pb.RevisionRef{Function: &pb.FunctionRef{Namespace: "ns", Name: "double"}, RevisionId: "rev-1"}}, nil
}

type functionCallStub struct {
	pb.UnimplementedCallServiceServer
	mu sync.Mutex
	created *pb.CreateCallRequest
	watched *pb.WatchCallRequest
	cancelled bool
}

func (stub *functionCallStub) Create(_ context.Context, request *pb.CreateCallRequest) (*pb.CallRecord, error) {
	stub.mu.Lock(); stub.created = request; stub.mu.Unlock()
	return &pb.CallRecord{Ref: &pb.CallRef{CallId: "call-stable"}, Status: pb.CallStatus_CALL_STATUS_PENDING}, nil
}

func (stub *functionCallStub) GetResult(context.Context, *pb.GetCallResultRequest) (*pb.CallResult, error) {
	value, _ := EncodeValue(42, ValueJSON, CompressionNone)
	return &pb.CallResult{Call: &pb.CallRef{CallId: "call-stable"}, Outcome: &pb.CallResult_Value{Value: value.wire}}, nil
}

func (stub *functionCallStub) Watch(request *pb.WatchCallRequest, stream grpc.ServerStreamingServer[pb.CallEvent]) error {
	stub.mu.Lock(); stub.watched = request; stub.mu.Unlock()
	return stream.Send(&pb.CallEvent{Call: request.Cursor.Call, Sequence: request.Cursor.AfterSequence + 1, Payload: &pb.CallEvent_Status{Status: &pb.StatusEvent{Status: pb.CallStatus_CALL_STATUS_SUCCEEDED}}})
}

func (stub *functionCallStub) Cancel(context.Context, *pb.CancelCallRequest) (*pb.CallRecord, error) {
	stub.mu.Lock(); stub.cancelled = true; stub.mu.Unlock()
	return &pb.CallRecord{Ref: &pb.CallRef{CallId: "call-stable"}, Status: pb.CallStatus_CALL_STATUS_CANCELLING}, nil
}

func TestDeployedFunctionLookupInvokeAndReconstruct(t *testing.T) {
	stub := &functionCallStub{}
	listener := startGRPCServices(t, func(server *grpc.Server) {
		pb.RegisterFunctionServiceServer(server, &functionStub{})
		pb.RegisterCallServiceServer(server, stub)
	})
	client := bufconnClient(t, listener)
	function, err := LookupFunction[int](context.Background(), client, "ns", "double")
	if err != nil { t.Fatal(err) }
	if function.RevisionID() != "rev-1" { t.Fatalf("revision = %q", function.RevisionID()) }
	call, err := function.Spawn(context.Background(), 21)
	if err != nil { t.Fatal(err) }
	if call.ID() != "call-stable" { t.Fatalf("id = %q", call.ID()) }
	rebuilt, err := FunctionCallFromID[int](client, call.ID())
	if err != nil { t.Fatal(err) }
	result, err := rebuilt.Get(context.Background())
	if err != nil || result != 42 { t.Fatalf("result = %d, %v", result, err) }
	stub.mu.Lock(); created := stub.created; stub.mu.Unlock()
	if created == nil || created.Target.Function.RevisionId != "rev-1" || len(created.Inputs) != 1 { t.Fatalf("create = %#v", created) }
}


func TestWatchCursorAndCancel(t *testing.T) {
	stub := &functionCallStub{}
	listener := startGRPCServices(t, func(server *grpc.Server) { pb.RegisterCallServiceServer(server, stub) })
	client := bufconnClient(t, listener)
	call, _ := FunctionCallFromID[int](client, "call-stable")
	events, failures := call.Watch(context.Background(), 41, false)
	event := <-events
	if event.Sequence != 42 { t.Fatalf("sequence = %d", event.Sequence) }
	if err := <-failures; err != nil { t.Fatal(err) }
	if err := call.Cancel(context.Background(), "test"); err != nil { t.Fatal(err) }
	stub.mu.Lock(); cancelled := stub.cancelled; stub.mu.Unlock()
	if !cancelled { t.Fatal("cancel RPC not observed") }
}
