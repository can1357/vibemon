package vmon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestMain doubles as the takeover worker entry point: the tests below (and the
// client stub bridge) re-execute this test binary with VMON_TAKEOVER=1, which
// makes Takeover run the worker loop instead of the test suite.
func TestMain(m *testing.M) {
	registerTakeoverTestFunctions()
	Takeover()
	os.Exit(m.Run())
}

type takeoverTestError struct{ Code int }

func (err *takeoverTestError) Error() string { return fmt.Sprintf("test error %d", err.Code) }

var takeoverFlakyCalls atomic.Int64

func registerTakeoverTestFunctions() {
	Register("add", func(a, b int) int { return a + b })
	Register("concat", func(parts []string, separator string) (string, error) {
		return strings.Join(parts, separator), nil
	})
	Register("norm", func(point struct {
		X float64 `json:"x"`
		Y float64 `json:"y"`
	}) float64 {
		return point.X*point.X + point.Y*point.Y
	})
	Register("sum", func(values ...int) int {
		total := 0
		for _, value := range values {
			total += value
		}
		return total
	})
	Register("ctxEcho", func(ctx context.Context, text string) (string, error) {
		if ctx == nil {
			return "", errors.New("nil context")
		}
		return "ctx:" + text, nil
	})
	Register("fail", func(code int) error { return &takeoverTestError{Code: code} })
	Register("panics", func(message string) int { panic(message) })
	Register("noisy", func() int {
		fmt.Println("hello from worker")
		fmt.Fprintln(os.Stderr, "warn from worker")
		return 7
	})
	Register("flaky", func(threshold int) (int64, error) {
		count := takeoverFlakyCalls.Add(1)
		if count < int64(threshold) {
			return 0, fmt.Errorf("flaky failure %d", count)
		}
		return count, nil
	})
	Register("sleepy", func(milliseconds int) int {
		time.Sleep(time.Duration(milliseconds) * time.Millisecond)
		return milliseconds
	})
	Register("die", func(code int) int {
		os.Exit(code)
		return 0
	})
}

// takeoverWorkerProc drives one re-executed worker over real pipes.
type takeoverWorkerProc struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	frames *bufio.Reader
	stderr bytes.Buffer
	waited atomic.Bool
}

func startTakeoverWorkerProc(t *testing.T) *takeoverWorkerProc {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), takeoverModeEnv+"=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	worker := &takeoverWorkerProc{t: t, cmd: cmd, stdin: stdin, frames: bufio.NewReader(stdout)}
	cmd.Stderr = &worker.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !worker.waited.Load() {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			worker.waited.Store(true)
		}
	})
	hello := worker.next()
	if hello["event"] != "hello" {
		t.Fatalf("first frame = %v", hello)
	}
	version, _ := hello["go"].(string)
	if !strings.HasPrefix(version, "go1") {
		t.Fatalf("hello go version = %q", version)
	}
	return worker
}

func (worker *takeoverWorkerProc) send(op string) {
	worker.t.Helper()
	if _, err := io.WriteString(worker.stdin, op+"\n"); err != nil {
		worker.t.Fatalf("send %s: %v (stderr: %s)", op, err, worker.stderr.String())
	}
}

func (worker *takeoverWorkerProc) next() map[string]any {
	worker.t.Helper()
	deadline := time.AfterFunc(10*time.Second, func() { _ = worker.cmd.Process.Kill() })
	defer deadline.Stop()
	line, err := worker.frames.ReadBytes('\n')
	if err != nil {
		worker.t.Fatalf("read frame: %v (stderr: %s)", err, worker.stderr.String())
	}
	var frame map[string]any
	if err := json.Unmarshal(line, &frame); err != nil {
		worker.t.Fatalf("frame %q: %v", line, err)
	}
	return frame
}

// nextUntil collects frames through the first frame of the given event kind.
func (worker *takeoverWorkerProc) nextUntil(event string) []map[string]any {
	worker.t.Helper()
	var frames []map[string]any
	for {
		frame := worker.next()
		frames = append(frames, frame)
		if frame["event"] == event {
			return frames
		}
	}
}

func (worker *takeoverWorkerProc) waitExit() int {
	worker.t.Helper()
	worker.waited.Store(true)
	err := worker.cmd.Wait()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	worker.t.Fatalf("wait: %v", err)
	return -1
}

func TestTakeoverWorkerCallRoundTrip(t *testing.T) {
	worker := startTakeoverWorkerProc(t)
	worker.send(`{"op":"call","id":1,"name":"add","args":{"json":[2,3]},"mode":"value"}`)
	frame := worker.next()
	if frame["event"] != "result" || frame["json"] != float64(5) || frame["id"] != float64(1) {
		t.Fatalf("frame = %v", frame)
	}
	// Per-parameter decoding into slices, structs, and variadics.
	worker.send(`{"op":"call","id":2,"name":"concat","args":{"json":[["a","b","c"],"-"]}}`)
	if frame = worker.next(); frame["json"] != "a-b-c" {
		t.Fatalf("concat = %v", frame)
	}
	worker.send(`{"op":"call","id":3,"name":"norm","args":{"json":[{"x":3,"y":4}]}}`)
	if frame = worker.next(); frame["json"] != float64(25) {
		t.Fatalf("norm = %v", frame)
	}
	worker.send(`{"op":"call","id":4,"name":"sum","args":{"json":[1,2,3,4]}}`)
	if frame = worker.next(); frame["json"] != float64(10) {
		t.Fatalf("sum = %v", frame)
	}
	worker.send(`{"op":"call","id":5,"name":"ctxEcho","args":{"json":["hi"]}}`)
	if frame = worker.next(); frame["json"] != "ctx:hi" {
		t.Fatalf("ctxEcho = %v", frame)
	}
}

func TestTakeoverWorkerErrorEventsKeepSessionAlive(t *testing.T) {
	worker := startTakeoverWorkerProc(t)
	worker.send(`{"op":"call","id":1,"name":"fail","args":{"json":[3]}}`)
	frame := worker.next()
	if frame["event"] != "error" || frame["etype"] != "*vmon.takeoverTestError" || frame["message"] != "test error 3" {
		t.Fatalf("frame = %v", frame)
	}
	worker.send(`{"op":"call","id":2,"name":"panics","args":{"json":["boom"]}}`)
	frame = worker.next()
	traceback, _ := frame["traceback"].(string)
	if frame["etype"] != "panic" || frame["message"] != "boom" ||
		!strings.Contains(traceback, "goroutine") || !strings.Contains(traceback, "runtime/debug.Stack") {
		t.Fatalf("panic frame = %v", frame)
	}
	worker.send(`{"op":"call","id":3,"name":"unknown","args":{"json":[]}}`)
	frame = worker.next()
	message, _ := frame["message"].(string)
	if frame["etype"] != "UnknownFunction" || !strings.Contains(message, `"unknown"`) || !strings.Contains(message, "add") {
		t.Fatalf("unknown frame = %v", frame)
	}
	worker.send(`{"op":"call","id":4,"name":"add","args":{"json":["x",3]}}`)
	frame = worker.next()
	if frame["etype"] != "ArgumentError" || !strings.Contains(frame["message"].(string), "argument 0") {
		t.Fatalf("bad argument frame = %v", frame)
	}
	worker.send(`{"op":"call","id":5,"name":"add","args":{"json":[1]}}`)
	frame = worker.next()
	if frame["etype"] != "ArgumentError" || !strings.Contains(frame["message"].(string), "expects 2 arguments, got 1") {
		t.Fatalf("arity frame = %v", frame)
	}
	worker.send(`{"op":"call","id":6,"name":"add","args":{"pickle":"AAA="}}`)
	if frame = worker.next(); frame["etype"] != "ProtocolError" {
		t.Fatalf("pickle frame = %v", frame)
	}
	// The session survived every failure above.
	worker.send(`{"op":"call","id":7,"name":"add","args":{"json":[20,22]}}`)
	if frame = worker.next(); frame["event"] != "result" || frame["json"] != float64(42) {
		t.Fatalf("survivor frame = %v", frame)
	}
}

func TestTakeoverWorkerOutEventsPrecedeResult(t *testing.T) {
	worker := startTakeoverWorkerProc(t)
	worker.send(`{"op":"call","id":1,"name":"noisy","args":{"json":[]}}`)
	frames := worker.nextUntil("result")
	var stdout, stderr strings.Builder
	for _, frame := range frames[:len(frames)-1] {
		if frame["event"] != "out" {
			t.Fatalf("unexpected pre-result frame %v", frame)
		}
		data, _ := frame["data"].(string)
		if frame["stream"] == "stderr" {
			stderr.WriteString(data)
		} else {
			stdout.WriteString(data)
		}
	}
	if !strings.Contains(stdout.String(), "hello from worker") {
		t.Fatalf("stdout out events = %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "warn from worker") {
		t.Fatalf("stderr out events = %q", stderr.String())
	}
	if last := frames[len(frames)-1]; last["json"] != float64(7) {
		t.Fatalf("result frame = %v", last)
	}
}

func TestTakeoverWorkerShutdown(t *testing.T) {
	worker := startTakeoverWorkerProc(t)
	worker.send(`{"op":"shutdown","id":9}`)
	frame := worker.next()
	if frame["event"] != "result" || frame["id"] != float64(9) {
		t.Fatalf("frame = %v", frame)
	}
	_ = worker.stdin.Close()
	if code := worker.waitExit(); code != 0 {
		t.Fatalf("exit = %d", code)
	}
}

func TestTakeoverWorkerStdinEOFExitsZero(t *testing.T) {
	worker := startTakeoverWorkerProc(t)
	_ = worker.stdin.Close()
	if code := worker.waitExit(); code != 0 {
		t.Fatalf("exit = %d", code)
	}
}

func TestTakeoverWorkerRejectsUnknownOpAndMode(t *testing.T) {
	worker := startTakeoverWorkerProc(t)
	worker.send(`{"op":"call","id":1,"name":"add","args":{"json":[1,2]},"mode":"iter"}`)
	frame := worker.next()
	if frame["etype"] != "ProtocolError" || !strings.Contains(frame["message"].(string), "mode") {
		t.Fatalf("iter frame = %v", frame)
	}
	worker.send(`{"op":"init","id":2}`)
	if frame = worker.next(); frame["etype"] != "ProtocolError" {
		t.Fatalf("unknown op frame = %v", frame)
	}
	worker.send(`{"op":"call","id":3,"name":"add","args":{"json":[1,2]}}`)
	if frame = worker.next(); frame["json"] != float64(3) {
		t.Fatalf("survivor frame = %v", frame)
	}
}

func TestRegisterRejectsInvalidShapes(t *testing.T) {
	mustPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		fn()
	}
	mustPanic("empty name", func() { Register("", func() {}) })
	mustPanic("nil fn", func() { Register("t-nil", nil) })
	mustPanic("non-func", func() { Register("t-int", 42) })
	mustPanic("three returns", func() { Register("t-three", func() (int, int, error) { return 0, 0, nil }) })
	mustPanic("second not error", func() { Register("t-pair", func() (int, int) { return 0, 0 }) })
	mustPanic("late context", func() { Register("t-ctx", func(int, context.Context) {}) })
	mustPanic("channel param", func() { Register("t-chan", func(chan int) {}) })
	mustPanic("duplicate", func() {
		Register("t-dup", func() {})
		Register("t-dup", func() {})
	})
}
