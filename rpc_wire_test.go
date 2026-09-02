package xtframework

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"xtframework/internal/rpcpb"
)

func TestRPCEnvelopeUsesProtobuf(t *testing.T) {
	request := &rpcpb.RegisterRequest{
		SourceNode: 2,
		Location: &rpcpb.ServiceLocation{
			ServiceName: "room",
			ServiceId:   1,
			NodeId:      2,
			NodeAddr:    "127.0.0.1:9002",
		},
	}
	data, err := encodeOperationEnvelope(opRegister, request)
	if err != nil {
		t.Fatal(err)
	}

	var wire rpcpb.RpcEnvelope
	if err := proto.Unmarshal(data, &wire); err != nil {
		t.Fatalf("protobuf envelope decode failed: %v", err)
	}
	if wire.Operation != opRegister || len(wire.Payload) == 0 {
		t.Fatalf("wire envelope = %v", &wire)
	}

	envelope, err := decodeEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	var decoded rpcpb.RegisterRequest
	if err := decodeOperationPayload(envelope, opRegister, &decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(request, &decoded) {
		t.Fatalf("decoded request = %v, want %v", &decoded, request)
	}
}

func TestRPCResultUsesProtobufAndPreservesErrors(t *testing.T) {
	response := &rpcpb.LookupResponse{
		Location: &rpcpb.ServiceLocation{ServiceName: "room", ServiceId: 3, NodeId: 2, NodeAddr: "node-2"},
	}
	data, err := encodeOperationResult(opLookup, response)
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeResult(data)
	if err != nil {
		t.Fatal(err)
	}
	var decoded rpcpb.LookupResponse
	if err := decodeResultPayload(result, opLookup, &decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(response, &decoded) {
		t.Fatalf("decoded response = %v, want %v", &decoded, response)
	}

	for _, test := range []struct {
		code string
		want error
	}{
		{code: "service_not_found", want: ErrServiceNotFound},
		{code: "service_exists", want: ErrServiceExists},
		{code: "node_not_found", want: ErrNodeNotFound},
		{code: "invalid_message", want: ErrInvalidMessage},
	} {
		t.Run(test.code, func(t *testing.T) {
			data, err := encodeResult(rpcResult{Code: test.code, Error: "remote failure"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeResult(data); !errors.Is(err, test.want) {
				t.Fatalf("decodeResult() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRPCOperationRequestsRoundTrip(t *testing.T) {
	protocols := []struct {
		name    string
		op      operation
		message operationProtocol
	}{
		{
			name: "register", op: opRegister,
			message: &rpcpb.RegisterRequest{SourceNode: 2, Location: &rpcpb.ServiceLocation{ServiceName: "room", ServiceId: 1, NodeId: 2, NodeAddr: "node-2"}},
		},
		{
			name: "unregister", op: opUnregister,
			message: &rpcpb.UnregisterRequest{SourceNode: 2, Target: &rpcpb.ServiceKey{Name: "room", Id: 1}},
		},
		{
			name: "lookup", op: opLookup,
			message: &rpcpb.LookupRequest{SourceNode: 2, Target: &rpcpb.ServiceKey{Name: "room", Id: 1}},
		},
		{
			name: "deliver", op: opDeliver,
			message: &rpcpb.DeliverRequest{SourceNode: 2, Source: &rpcpb.ServiceKey{}, Target: &rpcpb.ServiceKey{Name: "room", Id: 1}, Payload: []byte{1}},
		},
	}

	for _, source := range protocols {
		t.Run(source.name, func(t *testing.T) {
			data, err := encodeOperationEnvelope(source.op, source.message)
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := decodeEnvelope(data)
			if err != nil {
				t.Fatal(err)
			}
			decoded := source.message.ProtoReflect().Type().New().Interface().(operationProtocol)
			if err := decodeOperationPayload(envelope, source.op, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(source.message, decoded) {
				t.Fatalf("decoded request = %v, want %v", decoded, source.message)
			}
		})
	}
}

func TestRPCOperationResponsesRoundTrip(t *testing.T) {
	responses := []struct {
		name    string
		op      operation
		message operationProtocol
	}{
		{name: "register", op: opRegister, message: &rpcpb.RegisterResponse{}},
		{name: "unregister", op: opUnregister, message: &rpcpb.UnregisterResponse{}},
		{
			name: "lookup", op: opLookup,
			message: &rpcpb.LookupResponse{Location: &rpcpb.ServiceLocation{ServiceName: "room", ServiceId: 1, NodeId: 2, NodeAddr: "node-2"}},
		},
		{name: "deliver", op: opDeliver, message: &rpcpb.DeliverResponse{Payload: []byte{1, 2, 3}}},
	}

	for _, test := range responses {
		t.Run(test.name, func(t *testing.T) {
			data, err := encodeOperationResult(test.op, test.message)
			if err != nil {
				t.Fatal(err)
			}
			result, err := decodeResult(data)
			if err != nil {
				t.Fatal(err)
			}
			decoded := test.message.ProtoReflect().Type().New().Interface().(operationProtocol)
			if err := decodeResultPayload(result, test.op, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(test.message, decoded) {
				t.Fatalf("decoded response = %v, want %v", decoded, test.message)
			}
		})
	}
}

func TestRPCRejectsMalformedAndUnknownEnvelopes(t *testing.T) {
	if _, err := decodeEnvelope([]byte{0x12, 0xff}); err == nil {
		t.Fatal("malformed protobuf envelope was accepted")
	}
	data, err := proto.Marshal(&rpcpb.RpcEnvelope{Operation: rpcpb.RpcOperation(99), Payload: []byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeEnvelope(data); err == nil {
		t.Fatal("unknown operation was accepted")
	}
	if _, err := decodeResult([]byte{0x0a, 0xff}); err == nil {
		t.Fatal("malformed protobuf result was accepted")
	}
}
