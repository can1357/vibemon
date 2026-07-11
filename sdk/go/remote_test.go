package vmon

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
)

func TestRemoteFunctionUsesBoundSandboxServices(t *testing.T) {
	execCalls := 0
	driver := &stubDriver{endpoints: []EndpointInfo{{URL: "node", Healthy: true}}, do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
		switch request.Method + " " + request.Path {
		case "POST /v1/sandboxes":
			return jsonResponse(200, `{"id":"remote","status":"running"}`), "node", nil
		case "PUT /v1/sandboxes/remote/files":
			return jsonResponse(204, ""), "node", nil
		case "DELETE /v1/sandboxes/remote/files":
			return jsonResponse(204, ""), "node", nil
		case "POST /v1/sandboxes/remote/terminate":
			return jsonResponse(204, ""), "node", nil
		case "POST /v1/sandboxes/remote/exec":
			execCalls++
			stdout := []byte("v22.0.0\n")
			if execCalls > 1 {
				payload, _ := json.Marshal(map[string]any{"ok": true, "result": 42, "stdout": "guest output\n"})
				stdout = payload
			}
			body, _ := json.Marshal(map[string]any{"exit": 0, "stdout_b64": base64.StdEncoding.EncodeToString(stdout), "stderr_b64": ""})
			return jsonResponse(200, string(body)), "node", nil
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.Path)
			return nil, "", nil
		}
	}}
	client := NewClient(driver)
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
