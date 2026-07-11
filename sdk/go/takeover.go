package vmon

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// takeoverModeEnv marks a process re-executed as an in-sandbox worker.
const takeoverModeEnv = "VMON_TAKEOVER"

const takeoverFlushTimeout = 5 * time.Second

var (
	takeoverErrorInterface   = reflect.TypeOf((*error)(nil)).Elem()
	takeoverContextInterface = reflect.TypeOf((*context.Context)(nil)).Elem()
)

type registeredFunction struct {
	name         string
	value        reflect.Value
	takesContext bool
	hasResult    bool
	hasError     bool
}

var takeoverRegistry = struct {
	sync.Mutex
	functions map[string]*registeredFunction
}{functions: map[string]*registeredFunction{}}

// Register makes fn callable remotely by name from Function handles bound to the same binary.
//
// Supported shapes are func(A...) R, func(A...) (R, error), func(A...) error, and func(A...),
// each optionally taking a leading context.Context. Parameters and the result travel as JSON.
// Register panics on duplicate names and unsupported shapes; call it before Takeover in main.
func Register(name string, fn any) {
	if name == "" {
		panic("vmon: Register: function name must not be empty")
	}
	value := reflect.ValueOf(fn)
	if !value.IsValid() || value.Kind() != reflect.Func || value.IsNil() {
		panic(fmt.Sprintf("vmon: Register(%q): fn must be a non-nil function, got %T", name, fn))
	}
	fnType := value.Type()
	takesContext := fnType.NumIn() > 0 && fnType.In(0) == takeoverContextInterface
	start := 0
	if takesContext {
		start = 1
	}
	for index := start; index < fnType.NumIn(); index++ {
		if fnType.In(index).Implements(takeoverContextInterface) && fnType.In(index).Kind() == reflect.Interface {
			panic(fmt.Sprintf("vmon: Register(%q): context.Context is only supported as the first parameter", name))
		}
		switch fnType.In(index).Kind() {
		case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Complex64, reflect.Complex128:
			panic(fmt.Sprintf(
				"vmon: Register(%q): parameter %d has non-JSON-decodable type %s",
				name, index-start, fnType.In(index),
			))
		}
	}
	switch fnType.NumOut() {
	case 0, 1:
	case 2:
		if fnType.Out(1) != takeoverErrorInterface {
			panic(fmt.Sprintf(
				"vmon: Register(%q): with two return values the second must be error, got %s",
				name, fnType.Out(1),
			))
		}
	default:
		panic(fmt.Sprintf("vmon: Register(%q): at most two return values are supported", name))
	}
	hasError := fnType.NumOut() > 0 && fnType.Out(fnType.NumOut()-1) == takeoverErrorInterface
	hasResult := fnType.NumOut()-boolToInt(hasError) == 1

	takeoverRegistry.Lock()
	defer takeoverRegistry.Unlock()
	if _, exists := takeoverRegistry.functions[name]; exists {
		panic(fmt.Sprintf("vmon: Register(%q): a function with this name is already registered", name))
	}
	takeoverRegistry.functions[name] = &registeredFunction{
		name:         name,
		value:        value,
		takesContext: takesContext,
		hasResult:    hasResult,
		hasError:     hasError,
	}
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func lookupRegistered(name string) *registeredFunction {
	takeoverRegistry.Lock()
	defer takeoverRegistry.Unlock()
	return takeoverRegistry.functions[name]
}

func registeredNames() []string {
	takeoverRegistry.Lock()
	defer takeoverRegistry.Unlock()
	names := make([]string, 0, len(takeoverRegistry.functions))
	for name := range takeoverRegistry.functions {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// Takeover turns the current process into a remote-function worker when re-executed
// inside a sandbox (VMON_TAKEOVER=1) and returns immediately otherwise.
//
// Call it at the top of main, after all Register calls. In worker mode it speaks the
// NDJSON session protocol on stdin/stdout, reroutes user stdout/stderr into protocol
// events, serves calls until a shutdown op or stdin EOF, and never returns.
func Takeover() {
	if os.Getenv(takeoverModeEnv) != "1" {
		return
	}
	os.Exit(runTakeoverWorker())
}

func runTakeoverWorker() int {
	// Keep the real stdout private for protocol frames: dup it, then point fd 1
	// at the stdout pump pipe so stray fd-level writes cannot corrupt frames.
	wireFd, err := takeoverDupFd(1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vmon takeover: dup stdout: %v\n", err)
		return 1
	}
	wire := &takeoverWire{file: os.NewFile(uintptr(wireFd), "vmon-takeover-wire")}
	stdoutPump, err := startTakeoverPump(wire, "stdout")
	if err != nil {
		fmt.Fprintf(os.Stderr, "vmon takeover: stdout pump: %v\n", err)
		return 1
	}
	stderrPump, err := startTakeoverPump(wire, "stderr")
	if err != nil {
		fmt.Fprintf(os.Stderr, "vmon takeover: stderr pump: %v\n", err)
		return 1
	}
	// Best effort: on platforms without dup2/dup3 the Go-level redirection below still applies.
	_ = takeoverDupToFd(int(stdoutPump.writer.Fd()), 1)
	os.Stdout = stdoutPump.writer
	os.Stderr = stderrPump.writer
	pumps := [2]*takeoverPump{stdoutPump, stderrPump}

	wire.send(struct {
		Event string `json:"event"`
		Go    string `json:"go"`
	}{"hello", runtime.Version()})

	reader := bufio.NewReader(os.Stdin)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) != 0 && handleTakeoverOp(wire, pumps, line) {
			return 0
		}
		if readErr != nil {
			return 0
		}
	}
}

// takeoverWire serializes protocol frames onto the private copy of the real stdout.
type takeoverWire struct {
	mu     sync.Mutex
	file   *os.File
	callID atomic.Uint64
}

func (wire *takeoverWire) send(frame any) {
	data, err := json.Marshal(frame)
	if err != nil {
		return
	}
	wire.mu.Lock()
	defer wire.mu.Unlock()
	_, _ = wire.file.Write(append(data, '\n'))
}

func (wire *takeoverWire) sendOut(id uint64, stream, data string) {
	wire.send(struct {
		Event  string `json:"event"`
		ID     uint64 `json:"id"`
		Stream string `json:"stream"`
		Data   string `json:"data"`
	}{"out", id, stream, data})
}

func (wire *takeoverWire) sendResult(id uint64, value json.RawMessage) {
	wire.send(struct {
		Event string          `json:"event"`
		ID    uint64          `json:"id"`
		JSON  json.RawMessage `json:"json"`
	}{"result", id, value})
}

func (wire *takeoverWire) sendError(id uint64, etype, message, traceback string) {
	wire.send(struct {
		Event     string `json:"event"`
		ID        uint64 `json:"id"`
		EType     string `json:"etype"`
		Message   string `json:"message"`
		Traceback string `json:"traceback"`
	}{"error", id, etype, message, traceback})
}

// takeoverPump forwards one redirected output stream as out events. A random
// in-band marker lets the dispatcher barrier ("flush") the pipe before emitting
// the result frame, so out events written by the call precede its result.
type takeoverPump struct {
	writer *os.File
	stream string
	marker []byte
	acks   chan struct{}
}

func startTakeoverPump(wire *takeoverWire, stream string) (*takeoverPump, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	marker := make([]byte, 16)
	if _, err := rand.Read(marker); err != nil {
		return nil, err
	}
	pump := &takeoverPump{writer: writer, stream: stream, marker: marker, acks: make(chan struct{}, 16)}
	go pump.run(reader, wire)
	return pump, nil
}

func (pump *takeoverPump) run(reader *os.File, wire *takeoverWire) {
	buffer := make([]byte, 8192)
	var pending []byte
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			pending = append(pending, buffer[:count]...)
			for {
				if index := bytes.Index(pending, pump.marker); index >= 0 {
					pump.emit(wire, pending[:index])
					pending = append(pending[:0], pending[index+len(pump.marker):]...)
					pump.acks <- struct{}{}
					continue
				}
				// Hold back a potential marker prefix; everything before it streams live.
				hold := len(pump.marker) - 1
				if len(pending) > hold {
					pump.emit(wire, pending[:len(pending)-hold])
					pending = append(pending[:0], pending[len(pending)-hold:]...)
				}
				break
			}
		}
		if err != nil {
			pump.emit(wire, pending)
			return
		}
	}
}

func (pump *takeoverPump) emit(wire *takeoverWire, data []byte) {
	if len(data) == 0 {
		return
	}
	wire.sendOut(wire.callID.Load(), pump.stream, string(data))
}

func (pump *takeoverPump) flush() {
	for {
		select {
		case <-pump.acks:
			continue
		default:
		}
		break
	}
	if _, err := pump.writer.Write(pump.marker); err != nil {
		return
	}
	select {
	case <-pump.acks:
	case <-time.After(takeoverFlushTimeout):
	}
}

func flushTakeoverPumps(pumps [2]*takeoverPump) {
	for _, pump := range pumps {
		pump.flush()
	}
}

type takeoverOp struct {
	Op   string          `json:"op"`
	ID   uint64          `json:"id"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
	Mode string          `json:"mode"`
}

type takeoverCallError struct {
	etype     string
	message   string
	traceback string
}

func handleTakeoverOp(wire *takeoverWire, pumps [2]*takeoverPump, line []byte) (shutdown bool) {
	var op takeoverOp
	if err := json.Unmarshal(bytes.TrimSpace(line), &op); err != nil {
		wire.sendError(0, "ProtocolError", fmt.Sprintf("malformed op line: %v", err), "")
		return false
	}
	switch op.Op {
	case "call":
		wire.callID.Store(op.ID)
		result, callErr := invokeTakeoverCall(op)
		flushTakeoverPumps(pumps)
		wire.callID.Store(0)
		if callErr != nil {
			wire.sendError(op.ID, callErr.etype, callErr.message, callErr.traceback)
		} else {
			wire.sendResult(op.ID, result)
		}
		return false
	case "shutdown":
		flushTakeoverPumps(pumps)
		wire.sendResult(op.ID, json.RawMessage("null"))
		return true
	default:
		wire.sendError(op.ID, "ProtocolError", fmt.Sprintf("unknown op %q", op.Op), "")
		return false
	}
}

func invokeTakeoverCall(op takeoverOp) (json.RawMessage, *takeoverCallError) {
	if op.Mode != "" && op.Mode != "value" {
		return nil, &takeoverCallError{
			etype:   "ProtocolError",
			message: fmt.Sprintf("Go workers do not support call mode %q (no generator streaming)", op.Mode),
		}
	}
	fn := lookupRegistered(op.Name)
	if fn == nil {
		return nil, &takeoverCallError{
			etype: "UnknownFunction",
			message: fmt.Sprintf(
				"no registered function %q (registered: %s)",
				op.Name, strings.Join(registeredNames(), ", "),
			),
		}
	}
	rawArgs, callErr := decodeTakeoverArgs(op.Args)
	if callErr != nil {
		return nil, callErr
	}
	in, callErr := fn.buildArgs(rawArgs)
	if callErr != nil {
		return nil, callErr
	}
	outs, callErr := fn.safeCall(in)
	if callErr != nil {
		return nil, callErr
	}
	if fn.hasError {
		if errValue := outs[len(outs)-1]; !errValue.IsNil() {
			err := errValue.Interface().(error)
			return nil, &takeoverCallError{etype: takeoverErrorType(err), message: err.Error()}
		}
	}
	if !fn.hasResult {
		return json.RawMessage("null"), nil
	}
	encoded, err := json.Marshal(outs[0].Interface())
	if err != nil {
		return nil, &takeoverCallError{
			etype:   "ResultError",
			message: fmt.Sprintf("remote function result must be JSON-serializable: %v", err),
		}
	}
	return encoded, nil
}

func decodeTakeoverArgs(frame json.RawMessage) ([]json.RawMessage, *takeoverCallError) {
	if len(frame) == 0 {
		return nil, nil
	}
	var value struct {
		JSON   *json.RawMessage `json:"json"`
		Pickle *string          `json:"pickle"`
		File   *string          `json:"file"`
	}
	if err := json.Unmarshal(frame, &value); err != nil {
		return nil, &takeoverCallError{etype: "ProtocolError", message: fmt.Sprintf("malformed args frame: %v", err)}
	}
	if value.Pickle != nil || value.File != nil {
		return nil, &takeoverCallError{etype: "ProtocolError", message: "Go workers accept only JSON-encoded arguments"}
	}
	if value.JSON == nil {
		return nil, &takeoverCallError{etype: "ProtocolError", message: "args frame is missing a json value"}
	}
	if string(bytes.TrimSpace(*value.JSON)) == "null" {
		return nil, nil
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(*value.JSON, &raw); err != nil {
		return nil, &takeoverCallError{etype: "ProtocolError", message: fmt.Sprintf("args must be a JSON array: %v", err)}
	}
	return raw, nil
}

func (fn *registeredFunction) buildArgs(raw []json.RawMessage) ([]reflect.Value, *takeoverCallError) {
	fnType := fn.value.Type()
	start := 0
	values := make([]reflect.Value, 0, fnType.NumIn())
	if fn.takesContext {
		values = append(values, reflect.ValueOf(context.Background()))
		start = 1
	}
	fixed := fnType.NumIn() - start
	if fnType.IsVariadic() {
		if len(raw) < fixed-1 {
			return nil, &takeoverCallError{
				etype:   "ArgumentError",
				message: fmt.Sprintf("%s expects at least %d arguments, got %d", fn.name, fixed-1, len(raw)),
			}
		}
	} else if len(raw) != fixed {
		return nil, &takeoverCallError{
			etype:   "ArgumentError",
			message: fmt.Sprintf("%s expects %d arguments, got %d", fn.name, fixed, len(raw)),
		}
	}
	for index, encoded := range raw {
		position := start + index
		var paramType reflect.Type
		if fnType.IsVariadic() && position >= fnType.NumIn()-1 {
			paramType = fnType.In(fnType.NumIn() - 1).Elem()
		} else {
			paramType = fnType.In(position)
		}
		target := reflect.New(paramType)
		if err := json.Unmarshal(encoded, target.Interface()); err != nil {
			return nil, &takeoverCallError{
				etype:   "ArgumentError",
				message: fmt.Sprintf("%s argument %d does not decode into %s: %v", fn.name, index, paramType, err),
			}
		}
		values = append(values, target.Elem())
	}
	return values, nil
}

func (fn *registeredFunction) safeCall(in []reflect.Value) (outs []reflect.Value, callErr *takeoverCallError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			callErr = &takeoverCallError{
				etype:     "panic",
				message:   fmt.Sprint(recovered),
				traceback: string(debug.Stack()),
			}
		}
	}()
	return fn.value.Call(in), nil
}

func takeoverErrorType(err error) string {
	errType := reflect.TypeOf(err)
	if errType == nil {
		return "error"
	}
	return errType.String()
}
