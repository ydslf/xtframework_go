package xtframework

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"xtframework/internal/rpcpb"
	"xtnet/net/packet"
)

func newRPCReadPacket(data []byte) *packet.ReadPacket {
	return packet.NewReadPacket(data, byteOrder, 0, len(data))
}

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
	wpk, err := encodeEnvelope(opRegister, request)
	if err != nil {
		t.Fatal(err)
	}
	data := wpk.GetRealData()

	if got := operation(byteOrder.Uint16(data[:2])); got != opRegister {
		t.Fatalf("wire operation = %d, want %d", got, opRegister)
	}

	op, payload, err := decodeEnvelope(newRPCReadPacket(data))
	if err != nil {
		t.Fatal(err)
	}
	if op != opRegister {
		t.Fatalf("decoded operation = %d, want %d", op, opRegister)
	}
	var decoded rpcpb.RegisterRequest
	if err := decodeOperationPayload(payload, &decoded); err != nil {
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
	wpk, err := encodeResult("", "", response)
	if err != nil {
		t.Fatal(err)
	}
	data := wpk.GetRealData()
	if len(data) < 4 || data[0] != 0 || data[1] != 0 || data[2] != 0 || data[3] != 0 {
		t.Fatalf("successful result header = %v, want two empty strings", data[:min(len(data), 4)])
	}
	payload, err := decodeResult(newRPCReadPacket(data))
	if err != nil {
		t.Fatal(err)
	}
	var decoded rpcpb.LookupResponse
	if err := decodeResultPayload(payload, &decoded); err != nil {
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
			wpk, err := encodeResult(test.code, "remote failure", nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeResult(newRPCReadPacket(wpk.GetRealData())); !errors.Is(err, test.want) {
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
			wpk, err := encodeEnvelope(source.op, source.message)
			if err != nil {
				t.Fatal(err)
			}
			op, payload, err := decodeEnvelope(newRPCReadPacket(wpk.GetRealData()))
			if err != nil {
				t.Fatal(err)
			}
			if op != source.op {
				t.Fatalf("decoded operation = %d, want %d", op, source.op)
			}
			decoded := source.message.ProtoReflect().Type().New().Interface().(operationProtocol)
			if err := decodeOperationPayload(payload, decoded); err != nil {
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
			wpk, err := encodeResult("", "", test.message)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := decodeResult(newRPCReadPacket(wpk.GetRealData()))
			if err != nil {
				t.Fatal(err)
			}
			decoded := test.message.ProtoReflect().Type().New().Interface().(operationProtocol)
			if err := decodeResultPayload(payload, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(test.message, decoded) {
				t.Fatalf("decoded response = %v, want %v", decoded, test.message)
			}
		})
	}
}

func TestRPCRejectsMalformedEnvelopes(t *testing.T) {
	if _, _, err := decodeEnvelope(newRPCReadPacket([]byte{0x00})); err == nil {
		t.Fatal("envelope without operation was accepted")
	}
	if _, err := decodeResult(newRPCReadPacket([]byte{0x00})); err == nil {
		t.Fatal("result without complete code length was accepted")
	}
	if _, err := decodeResult(newRPCReadPacket([]byte{0x00, 0x02, 'x'})); err == nil {
		t.Fatal("result with incomplete code was accepted")
	}
}
