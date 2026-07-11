package vmon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	pb "github.com/can1357/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
)
// ErrCallCancelled reports a distinct durable cancellation outcome.
var ErrCallCancelled = errors.New("vmon: remote call cancelled")


// FunctionOption alters durable call creation without mutating a function handle.
type FunctionOption func(*callOptions)
type callOptions struct { labels map[string]string; resultTTL *uint64; cancellation pb.ClientCancellationPolicy; codec ValueCodec; compression ValueCompression }

// WithCallLabels attaches lookup metadata to subsequently created calls.
func WithCallLabels(labels map[string]string) FunctionOption { return func(o *callOptions) { o.labels = cloneStrings(labels) } }
// WithResultTTLMillis overrides durable result retention.
func WithResultTTLMillis(milliseconds uint64) FunctionOption { return func(o *callOptions) { o.resultTTL = &milliseconds } }
// WithClientCancellation makes creator disconnect request durable cancellation.
func WithClientCancellation() FunctionOption { return func(o *callOptions) { o.cancellation = pb.ClientCancellationPolicy_CLIENT_CANCELLATION_POLICY_CANCEL } }
// WithValueEncoding selects portable argument serialization and compression.
func WithValueEncoding(codec ValueCodec, compression ValueCompression) FunctionOption { return func(o *callOptions) { o.codec, o.compression = codec, compression } }

// Function is a deployed logical function pinned to an immutable revision.
type Function[T any] struct { client *Client; ref *pb.RevisionRef; endpoint string; options callOptions }

// LookupFunction resolves the current deployed revision.
func LookupFunction[T any](ctx context.Context, client *Client, namespace, name string) (*Function[T], error) {
	selector := &pb.FunctionSelector{Selection: &pb.FunctionSelector_Current{Current: &pb.FunctionRef{Namespace: namespace, Name: name}}}
	return lookupFunction[T](ctx, client, selector)
}

// LookupFunctionRevision resolves an exact immutable deployed revision.
func LookupFunctionRevision[T any](ctx context.Context, client *Client, namespace, name, revisionID string) (*Function[T], error) {
	selector := &pb.FunctionSelector{Selection: &pb.FunctionSelector_Pinned{Pinned: &pb.RevisionRef{Function: &pb.FunctionRef{Namespace: namespace, Name: name}, RevisionId: revisionID}}}
	return lookupFunction[T](ctx, client, selector)
}

func lookupFunction[T any](ctx context.Context, client *Client, selector *pb.FunctionSelector) (*Function[T], error) {
	if client == nil { return nil, errors.New("vmon: nil client") }
	var revision *pb.FunctionRevision
	endpoint, err := client.unary(ctx, "", "get function", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error { var err error; revision, err = pb.NewFunctionServiceClient(conn).Get(ctx, &pb.GetFunctionRequest{Function: selector}, opts...); return err })
	if err != nil { return nil, err }
	if revision == nil || revision.Ref == nil { return nil, errors.New("vmon: function lookup returned no revision") }
	return &Function[T]{client: client, ref: revision.Ref, endpoint: endpoint, options: callOptions{codec: ValueJSON}}, nil
}

// WithOptions derives a handle with call options; the pinned revision is unchanged.
func (function *Function[T]) WithOptions(options ...FunctionOption) *Function[T] {
	copy := *function; copy.options.labels = cloneStrings(function.options.labels)
	for _, option := range options { if option != nil { option(&copy.options) } }
	return &copy
}

// RevisionID returns the immutable deployed revision identifier.
func (function *Function[T]) RevisionID() string { if function == nil || function.ref == nil { return "" }; return function.ref.RevisionId }

// Spawn durably creates a unary call and returns immediately.
func (function *Function[T]) Spawn(ctx context.Context, input any) (*FunctionCall[T], error) { return function.spawn(ctx, pb.CallType_CALL_TYPE_UNARY, []any{input}, true, false) }

// Remote invokes a deployed function and waits for its durable result.
func (function *Function[T]) Remote(ctx context.Context, input any) (T, error) {
	call, err := function.spawn(ctx, pb.CallType_CALL_TYPE_UNARY, []any{input}, true, true); if err != nil { var zero T; return zero, err }
	return call.Get(ctx)
}

func (function *Function[T]) spawn(ctx context.Context, kind pb.CallType, values []any, closed, owner bool) (*FunctionCall[T], error) {
	if function == nil || function.client == nil || function.ref == nil { return nil, errors.New("vmon: invalid function handle") }
	inputs := make([]*pb.CallInput, len(values))
	codec := function.options.codec
	if codec == 0 { codec = ValueJSON }
	for index, value := range values { envelope, err := EncodeValue(value, codec, function.options.compression); if err != nil { return nil, err }; inputs[index] = &pb.CallInput{Index: uint64(index), InputId: fmt.Sprintf("%d", index), Value: envelope.wire} }
	requestID, err := randomID()
	if err != nil { return nil, err }
	request := &pb.CreateCallRequest{Type: kind, Target: &pb.CallTarget{Function: function.ref}, Inputs: inputs, InputsClosed: closed, RequestId: requestID, Labels: cloneStrings(function.options.labels)}
	var clientSessionID string
	if owner && function.options.cancellation == pb.ClientCancellationPolicy_CLIENT_CANCELLATION_POLICY_CANCEL {
		request.ClientCancellation = function.options.cancellation
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil { return nil, fmt.Errorf("vmon: create client cancellation session: %w", err) }
		clientSessionID = hex.EncodeToString(random)
		request.ClientSessionIdPresence = &pb.CreateCallRequest_ClientSessionId{ClientSessionId: clientSessionID}
	}
	if function.options.resultTTL != nil { request.ResultTtlMillisPresence = &pb.CreateCallRequest_ResultTtlMillis{ResultTtlMillis: *function.options.resultTTL} }
	var record *pb.CallRecord
	endpoint, err := function.client.unary(ctx, function.endpoint, "create call", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error { var err error; record, err = pb.NewCallServiceClient(conn).Create(ctx, request, opts...); return err })
	if err != nil { return nil, err }; if record == nil || record.Ref == nil { return nil, errors.New("vmon: create call returned no id") }
	return &FunctionCall[T]{client: function.client, id: record.Ref.CallId, endpoint: endpoint, clientSessionID: clientSessionID}, nil
}

// FunctionCall is a durable call handle reconstructible from its stable ID.
type FunctionCall[T any] struct { client *Client; id string; endpoint string; clientSessionID string }

// CallStatus is the public durable call lifecycle state.
type CallStatus uint8
const (
	CallStatusPending CallStatus = iota + 1
	CallStatusQueued
	CallStatusRunning
	CallStatusSucceeded
	CallStatusFailed
	CallStatusCancelling
	CallStatusCancelled
)
func publicCallStatus(status pb.CallStatus) CallStatus { return CallStatus(status) }

// AttemptStats contains durable per-attempt timing and resource usage.
type AttemptStats struct { Attempt uint32; QueuedMillis, StartupMillis, ExecutionMillis, CPUMillis, PeakMemoryBytes uint64 }
// CallStats contains durable aggregate execution statistics.
type CallStats struct { QueueMillis, StartupMillis, ExecutionMillis, WallMillis, CPUMillis, PeakMemoryBytes uint64; Attempts []AttemptStats }
// CallGraph contains durable call ancestry with optional root presence.
type CallGraph struct { ParentCallIDs, ParentInputIDs []string; RootCallID *string }

// FunctionCallFromID reconstructs a durable call handle in any process.
func FunctionCallFromID[T any](client *Client, id string) (*FunctionCall[T], error) { if client == nil || id == "" { return nil, errors.New("vmon: client and call id are required") }; return &FunctionCall[T]{client: client, id: id}, nil }
// ID returns the stable server-assigned call identifier.
func (call *FunctionCall[T]) ID() string { if call == nil { return "" }; return call.id }

// RemoteCallError is a structured language-neutral execution failure.
type RemoteCallError struct { Code, Message, Type string; Retryable bool; Details map[string]string; Frames []ErrorFrame; Cause *RemoteCallError }
// ErrorFrame is one language-neutral remote stack frame.
type ErrorFrame struct { File string; Line uint32; Function string; Code *string }
func (err *RemoteCallError) Error() string { if err == nil { return "<nil>" }; return fmt.Sprintf("vmon remote call (%s): %s", err.Code, err.Message) }
func callError(value *pb.CallError) error { if value == nil { return errors.New("vmon: remote call failed") }; result:=&RemoteCallError{Code:value.Code, Message:value.Message, Type:value.Type, Retryable:value.Retryable, Details:cloneStrings(value.Details)};for _,frame:=range value.Frames{item:=ErrorFrame{File:frame.File,Line:frame.Line,Function:frame.Function};if frame.GetCodePresence()!=nil{code:=frame.GetCode();item.Code=&code};result.Frames=append(result.Frames,item)};if value.GetCause()!=nil{result.Cause=callError(value.GetCause()).(*RemoteCallError)};return result }
func (call *FunctionCall[T]) Status(ctx context.Context) (CallStatus, error) { record, err := call.record(ctx); if err != nil { return 0, err }; return publicCallStatus(record.Status), nil }
// Stats returns durable aggregate execution statistics.
func (call *FunctionCall[T]) Stats(ctx context.Context) (*CallStats, error) { record, err := call.record(ctx); if err != nil { return nil, err };if record.Stats==nil{return nil,nil};source:=record.Stats;result:=&CallStats{QueueMillis:source.QueueMillis,StartupMillis:source.StartupMillis,ExecutionMillis:source.ExecutionMillis,WallMillis:source.WallMillis,CPUMillis:source.CpuMillis,PeakMemoryBytes:source.PeakMemoryBytes};for _,attempt:=range source.Attempts{result.Attempts=append(result.Attempts,AttemptStats{Attempt:attempt.Attempt,QueuedMillis:attempt.QueuedMillis,StartupMillis:attempt.StartupMillis,ExecutionMillis:attempt.ExecutionMillis,CPUMillis:attempt.CpuMillis,PeakMemoryBytes:attempt.PeakMemoryBytes})};return result,nil }
// Graph returns a copy of the durable parent/root relationship.
func (call *FunctionCall[T]) Graph(ctx context.Context) (*CallGraph, error) { record, err := call.record(ctx); if err != nil { return nil, err };if record.Graph==nil{return nil,nil};result:=&CallGraph{ParentCallIDs:append([]string(nil),record.Graph.ParentCallIds...),ParentInputIDs:append([]string(nil),record.Graph.ParentInputIds...)};if record.Graph.GetRootCallIdPresence()!=nil{root:=record.Graph.GetRootCallId();result.RootCallID=&root};return result,nil }
func (call *FunctionCall[T]) record(ctx context.Context) (*pb.CallRecord, error) { var record *pb.CallRecord; endpoint, err := call.client.unary(ctx,call.endpoint,"get call",func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error { var err error; record,err=pb.NewCallServiceClient(conn).Get(ctx,&pb.CallRef{CallId:call.id},opts...); return err }); if err==nil {call.endpoint=endpoint}; return record,err }

// Cancel durably requests cancellation.
func (call *FunctionCall[T]) Cancel(ctx context.Context, reason string) error { requestID, idErr:=randomID(); if idErr!=nil{return idErr}; endpoint, err := call.client.unary(ctx,call.endpoint,"cancel call",func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error { _,err:=pb.NewCallServiceClient(conn).Cancel(ctx,&pb.CancelCallRequest{Call:&pb.CallRef{CallId:call.id},Reason:reason,RequestId:requestID},opts...); return err }); if err==nil {call.endpoint=endpoint}; return err }

// CallEvent is a reconnectable durable event and its cursor.
type CallEvent struct {
	Sequence uint64
	Status CallStatus
	Log []byte
	InputID string
	InputIndex uint64
	YieldIndex uint64
	Attempt uint32
}

func (call *FunctionCall[T]) watchRequest(afterSequence uint64, follow bool) *pb.WatchCallRequest {
	request := &pb.WatchCallRequest{Cursor: &pb.EventCursor{Call: &pb.CallRef{CallId: call.id}, AfterSequence: afterSequence}, Follow: follow}
	if follow && call.clientSessionID != "" {
		request.ClientSessionIdPresence = &pb.WatchCallRequest_ClientSessionId{ClientSessionId: call.clientSessionID}
	}
	return request
}
// Watch streams observable events without presenting creator cancellation capability.
func (call *FunctionCall[T]) Watch(ctx context.Context, afterSequence uint64, follow bool) (<-chan CallEvent, <-chan error) {
	return call.watch(ctx, afterSequence, follow, false)
}
func (call *FunctionCall[T]) watch(ctx context.Context, afterSequence uint64, follow, owner bool) (<-chan CallEvent, <-chan error) {
	events := make(chan CallEvent, 16)
	failures := make(chan error, 1)
	go func() { defer close(events); defer close(failures); endpoint,err:=call.client.grpcEndpoint(call.endpoint); if err!=nil { failures<-err; return }; conn,err:=call.client.conn(endpoint); if err!=nil { failures<-err; return }; request:=call.watchRequest(afterSequence,follow); if !owner {request.ClientSessionIdPresence=nil}; stream,err:=pb.NewCallServiceClient(conn).Watch(ctx,request); if err!=nil { failures<-err; return }; call.endpoint=endpoint; for { event,err:=stream.Recv(); if err==io.EOF{return}; if err!=nil { failures<-err; return }; item:=CallEvent{Sequence:event.Sequence, InputID:event.GetInputId(), InputIndex:event.GetInputIndex(), Attempt:event.GetAttempt()}; if status:=event.GetStatus(); status!=nil {item.Status=publicCallStatus(status.Status)}; if log:=event.GetLog(); log!=nil {item.Log=append([]byte(nil),log.Data...)}; if result:=event.GetResult(); result!=nil {item.InputID=result.GetInputId();item.InputIndex=result.GetInputIndex();item.YieldIndex=result.GetYieldIndex()}; if result:=event.GetYield(); result!=nil {item.InputID=result.GetInputId();item.InputIndex=result.GetInputIndex();item.YieldIndex=result.GetYieldIndex()}; select {case events<-item: case <-ctx.Done(): boundedCancel(call,ctx.Err().Error()); failures<-ctx.Err(); return} } }()
	return events, failures
}

// Get waits for terminal completion and decodes result index zero.
func (call *FunctionCall[T]) Get(ctx context.Context) (T,error) { var zero T; events,failures:=call.watch(ctx,0,true,true); for event:=range events { if event.Status==CallStatusFailed { record,err:=call.record(ctx); if err!=nil{return zero,err}; return zero,callError(record.GetError()) }; if event.Status==CallStatusCancelled {return zero,ErrCallCancelled}; if event.Status==CallStatusSucceeded { return call.Result(ctx,0) } }; if err:=<-failures; err!=nil{return zero,err}; return zero,errors.New("vmon: call event stream ended before completion") }

// Result retrieves and decodes one durable indexed result.
func (call *FunctionCall[T]) Result(ctx context.Context,index uint64)(T,error){ var zero T; var result *pb.CallResult; endpoint,err:=call.client.unary(ctx,call.endpoint,"get call result",func(ctx context.Context,conn grpc.ClientConnInterface,opts ...grpc.CallOption)error{var err error;result,err=pb.NewCallServiceClient(conn).GetResult(ctx,&pb.GetCallResultRequest{Call:&pb.CallRef{CallId:call.id},Index:index},opts...);return err});if err!=nil{return zero,err};call.endpoint=endpoint;if result.GetError()!=nil{return zero,callError(result.GetError())};if result.GetValue()==nil{return zero,errors.New("vmon: result has no value")};err=envelopeFromWire(result.GetValue()).Decode(&zero,func(ref *ArtifactReference)([]byte,error){return call.loadArtifact(ctx,ref)});return zero,err}

func (call *FunctionCall[T]) loadArtifact(ctx context.Context, ref *ArtifactReference) ([]byte, error) {
	endpoint, err := call.client.grpcEndpoint(call.endpoint)
	if err != nil { return nil, err }
	conn, err := call.client.conn(endpoint)
	if err != nil { return nil, err }
	stream, err := pb.NewArtifactServiceClient(conn).Get(ctx, &pb.GetArtifactRequest{Artifact: &pb.ArtifactRef{Digest: &pb.Digest{Algorithm: pb.DigestAlgorithm_DIGEST_ALGORITHM_SHA256, Value: append([]byte(nil), ref.Digest...)}}})
	if err != nil { return nil, err }
	var data []byte
	var offset uint64
	for {
		chunk, err := stream.Recv()
		if err == io.EOF { break }
		if err != nil { return nil, err }
		if chunk.Offset != offset { return nil, fmt.Errorf("vmon: artifact chunk offset %d, expected %d", chunk.Offset, offset) }
		data = append(data, chunk.Data...)
		offset += uint64(len(chunk.Data))
		if chunk.Eof { break }
	}
	call.endpoint = endpoint
	return data, nil
}

// BatchCall is a durable batch handle with indexed result retrieval.
type BatchCall[T any] struct{ *FunctionCall[T]; count uint64 }
// BatchCallFromID reconstructs a batch and obtains its committed input count.
func BatchCallFromID[T any](ctx context.Context, client *Client, id string) (*BatchCall[T], error) {
	call, err := FunctionCallFromID[T](client, id)
	if err != nil { return nil, err }
	record, err := call.record(ctx)
	if err != nil { return nil, err }
	return &BatchCall[T]{FunctionCall: call, count: record.InputCount}, nil
}
// SpawnMap creates a closed durable batch without owning workers or result buffers.
func (function *Function[T]) SpawnMap(ctx context.Context, inputs []any)(*BatchCall[T],error){ call,err:=function.spawn(ctx,pb.CallType_CALL_TYPE_BATCH,inputs,true,false);if err!=nil{return nil,err};return &BatchCall[T]{FunctionCall:call,count:uint64(len(inputs))},nil }
// Map commits a channel of inputs with bounded, one-at-a-time submission pressure.
// It returns after the input channel closes and the server acknowledges closure.
func (function *Function[T]) Map(ctx context.Context, inputs <-chan any) (*BatchCall[T], error) {
	call, err := function.spawn(ctx, pb.CallType_CALL_TYPE_BATCH, nil, false, false)
	if err != nil { return nil, err }
	completed := false
	defer func() { if !completed { boundedCancel(call, "input submission failed") } }()
	endpoint := call.endpoint
	if err != nil { return nil, err }
	conn, err := function.client.conn(endpoint)
	if err != nil { return nil, err }
	stream, err := pb.NewCallServiceClient(conn).StreamInputs(ctx)
	if err != nil { return nil, err }
	if err := stream.Send(&pb.StreamCallInputsRequest{Frame: &pb.StreamCallInputsRequest_Call{Call: &pb.CallRef{CallId: call.id}}}); err != nil { return nil, err }
	var count uint64
	for {
		select {
		case <-ctx.Done():
			boundedCancel(call, ctx.Err().Error())
			return nil, ctx.Err()
		case input, ok := <-inputs:
			if !ok {
				if _, err := stream.CloseAndRecv(); err != nil { return nil, err }
				var record *pb.CallRecord
				_, err = function.client.unary(ctx, endpoint, "close call inputs", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
					var closeErr error
					record, closeErr = pb.NewCallServiceClient(conn).CloseInputs(ctx, &pb.CloseCallInputsRequest{Call: &pb.CallRef{CallId: call.id}, ExpectedInputCount: count}, opts...)
					return closeErr
				})
				if err != nil { return nil, err }
				_ = record
				completed = true
				return &BatchCall[T]{FunctionCall: call, count: count}, nil
			}
			codec := function.options.codec
			if codec == 0 { codec = ValueJSON }
			envelope, err := EncodeValue(input, codec, function.options.compression)
			if err != nil { return nil, err }
			if err := stream.Send(&pb.StreamCallInputsRequest{Frame: &pb.StreamCallInputsRequest_Input{Input: &pb.CallInput{Index: count, InputId: fmt.Sprintf("%d", count), Value: envelope.wire}}}); err != nil { return nil, err }
			count++
		}
	}
}
// Results waits for durable batch completion, then streams indexed results in input order.
func (batch *BatchCall[T]) Results(ctx context.Context)(<-chan T,<-chan error){ values:=make(chan T,16);failures:=make(chan error,1);go func(){defer close(values);defer close(failures);events,eventErrors:=batch.Watch(ctx,0,true);succeeded:=false;for event:=range events{if event.Status==CallStatusSucceeded{succeeded=true;break};if event.Status==CallStatusCancelled{failures<-ErrCallCancelled;return};if event.Status==CallStatusFailed{record,err:=batch.record(ctx);if err!=nil{failures<-err}else{failures<-callError(record.GetError())};return}};if err:=<-eventErrors;err!=nil{failures<-err;return};if !succeeded{failures<-errors.New("vmon: batch event stream ended before completion");return};for index:=uint64(0);index<batch.count;index++{value,err:=batch.Result(ctx,index);if err!=nil{failures<-err;return};select{case values<-value:case<-ctx.Done():boundedCancel(batch.FunctionCall,ctx.Err().Error());failures<-ctx.Err();return}}}();return values,failures }
// GatherCalls waits for durable calls in argument order and returns the first error.
func GatherCalls[T any](ctx context.Context, calls ...*FunctionCall[T]) ([]T, error) {
	results := make([]T, len(calls))
	for index, call := range calls {
		result, err := call.Get(ctx)
		if err != nil { return nil, err }
		results[index] = result
	}
	return results, nil
}

// App identifies a current or pinned deployed application revision.
type App struct{ Namespace,Name,RevisionID string }
// LookupApp resolves the current application revision.
func LookupApp(ctx context.Context,client *Client,namespace,name string)(*App,error){selector:=&pb.AppSelector{Selection:&pb.AppSelector_Current{Current:&pb.AppRef{Namespace:namespace,Name:name}}};var revision *pb.AppRevision;_,err:=client.unary(ctx,"","get app",func(ctx context.Context,conn grpc.ClientConnInterface,opts ...grpc.CallOption)error{var err error;revision,err=pb.NewFunctionServiceClient(conn).GetApp(ctx,&pb.GetAppRequest{App:selector},opts...);return err});if err!=nil{return nil,err};ref:=revision.GetRef();if ref==nil{return nil,errors.New("vmon: app lookup returned no revision")};return &App{Namespace:namespace,Name:name,RevisionID:ref.RevisionId},nil}
// LookupAppRevision resolves an exact immutable application revision.
func LookupAppRevision(ctx context.Context, client *Client, namespace, name, revisionID string) (*App, error) {
	selector := &pb.AppSelector{Selection: &pb.AppSelector_Pinned{Pinned: &pb.AppRevisionRef{App: &pb.AppRef{Namespace: namespace, Name: name}, RevisionId: revisionID}}}
	var revision *pb.AppRevision
	_, err := client.unary(ctx, "", "get app", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var getErr error
		revision, getErr = pb.NewFunctionServiceClient(conn).GetApp(ctx, &pb.GetAppRequest{App: selector}, opts...)
		return getErr
	})
	if err != nil { return nil, err }
	ref := revision.GetRef()
	if ref == nil { return nil, errors.New("vmon: app lookup returned no revision") }
	return &App{Namespace: namespace, Name: name, RevisionID: ref.RevisionId}, nil
}

func boundedCancel[T any](call *FunctionCall[T], reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = call.Cancel(ctx, reason)
}

func randomID() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil { return "", fmt.Errorf("vmon: generate request id: %w", err) }
	return hex.EncodeToString(random), nil
}

func cloneStrings(values map[string]string)map[string]string{if values==nil{return nil};copy:=make(map[string]string,len(values));for key,value:=range values{copy[key]=value};return copy}
