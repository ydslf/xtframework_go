package xtframework

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"xtframework/internal/rpcpb"
)

func TestRPCEnvelopeUsesOperationPrefix(t *testing.T) {
	request := &rpcpb.RegisterRequest{
		SourceNode: 2,
		Location: &rpcpb.ServiceLocation{
			ServiceName: "room",
			ServiceId:   1,
			NodeId:      2,
			NodeAddr:    "127.0.0.1:9002",
		},
	}
	wpk, err := encodeOperationEnvelope(opRegister, request)
	if err != nil {
		t.Fatal(err)
	}
	data := wpk.GetRealData()

	if got := operation(byteOrder.Uint16(data[:2])); got != opRegister {
		t.Fatalf("wire operation = %d, want %d", got, opRegister)
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
	wpk, err := encodeOperationResult(response)
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeResult(wpk.GetRealData())
	if err != nil {
		t.Fatal(err)
	}
	var decoded rpcpb.LookupResponse
	if err := decodeResultPayload(result, &decoded); err != nil {
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
			wpk, err := encodeOperationEnvelope(source.op, source.message)
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := decodeEnvelope(wpk.GetRealData())
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
		message operationProtocol
	}{
		{name: "register", message: &rpcpb.RegisterResponse{}},
		{name: "unregister", message: &rpcpb.UnregisterResponse{}},
		{
			name:    "lookup",
			message: &rpcpb.LookupResponse{Location: &rpcpb.ServiceLocation{ServiceName: "room", ServiceId: 1, NodeId: 2, NodeAddr: "node-2"}},
		},
		{name: "deliver", message: &rpcpb.DeliverResponse{Payload: []byte{1, 2, 3}}},
	}

	for _, test := range responses {
		t.Run(test.name, func(t *testing.T) {
			wpk, err := encodeOperationResult(test.message)
			if err != nil {
				t.Fatal(err)
			}
			result, err := decodeResult(wpk.GetRealData())
			if err != nil {
				t.Fatal(err)
			}
			decoded := test.message.ProtoReflect().Type().New().Interface().(operationProtocol)
			if err := decodeResultPayload(result, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(test.message, decoded) {
				t.Fatalf("decoded response = %v, want %v", decoded, test.message)
			}
		})
	}
}

func TestRPCRejectsMalformedEnvelopes(t *testing.T) {
	if _, err := decodeEnvelope([]byte{0x00}); err == nil {
		t.Fatal("envelope without operation was accepted")
	}
	if _, err := decodeResult([]byte{0x0a, 0xff}); err == nil {
		t.Fatal("malformed protobuf result was accepted")
	}
}
