package vmon

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"

	pb "github.com/can1357/vibemon/sdk/go/internal/pb"
)

func TestRemoteFunctionUsesBoundSandboxServices(t *testing.T) {
	var mu sync.Mutex
	execCalls := 0
	stub := &sandboxServiceStub{
		create: func(context.Context, *pb.CreateSandboxRequest) (*pb.JsonView, error) {
			return &pb.JsonView{Json: `{"id":"remote","status":"running"}`}, nil
		},
		fileWrite: func(context.Context, *pb.FileWriteRequest) (*pb.Ok, error) {
			return &pb.Ok{}, nil
		},
		fileDelete: func(context.Context, *pb.FileDeleteRequest) (*pb.Ok, error) {
			return &pb.Ok{}, nil
		},
		terminate: func(context.Context, *pb.SandboxRef) (*pb.JsonView, error) {
			return &pb.JsonView{Json: `{}`}, nil
		},
		execCapture: func(_ context.Context, request *pb.ExecCaptureRequest) (*pb.ExecCaptureResponse, error) {
			if request.GetId() != "remote" {
				t.Errorf("exec sandbox id=%q", request.GetId())
			}
			mu.Lock()
			execCalls++
			calls := execCalls
			mu.Unlock()
			stdout := []byte("v22.0.0\n")
			if calls > 1 {
				payload, _ := json.Marshal(map[string]any{"ok": true, "result": 42, "stdout": "guest output\n"})
				stdout = payload
			}
			return &pb.ExecCaptureResponse{Code: 0, Stdout: stdout}, nil
		},
	}
	client := bufconnClient(t, startSandboxServiceStub(t, stub))
	var output bytes.Buffer
	function, err := NewRemoteFunction[int](client, RemoteFunctionSourceSpec{Source: "export default () => 42", ExportName: "default"}, RemoteFunctionOptions{Stdout: &output})
	if err != nil {
		t.Fatal(err)
	}
	result, err := function.Remote(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result != 42 || output.String() != "guest output\n" {
		t.Fatalf("result=%d output=%q", result, output.String())
	}
	if err = function.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteFunctionRejectsInvalidExport(t *testing.T) {
	_, err := NewRemoteFunction[any](NewClient(&stubDriver{}), RemoteFunctionSourceSpec{Source: "export default 1", ExportName: "not valid"})
	if err == nil {
		t.Fatal("expected invalid export error")
	}
}
