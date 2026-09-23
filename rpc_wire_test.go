package xtframework

import (
	"bytes"
	"errors"
	"strings"
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
		{code: "node_exists", want: ErrNodeExists},
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
			name: "register-node", op: opRegisterNode,
			message: &rpcpb.RegisterNodeRequest{NodeId: 2, NodeAddr: "node-2"},
		},
		{
			name: "register", op: opRegister,
			message: &rpcpb.RegisterRequest{Location: &rpcpb.ServiceLocation{ServiceName: "room", ServiceId: 1, NodeId: 2, NodeAddr: "node-2"}},
		},
		{
			name: "unregister", op: opUnregister,
			message: &rpcpb.UnregisterRequest{Target: &rpcpb.ServiceKey{Name: "room", Id: 1}},
		},
		{
			name: "lookup", op: opLookup,
			message: &rpcpb.LookupRequest{Target: &rpcpb.ServiceKey{Name: "room", Id: 1}},
		},
		{
			name: "deliver", op: opDeliver,
			message: &rpcpb.DeliverRequest{Source: &rpcpb.ServiceKey{}, Target: &rpcpb.ServiceKey{Name: "room", Id: 1}, MessageId: 42, Payload: []byte{1}},
		},
		{
			name: "deliver-string", op: opDeliver,
			message: &rpcpb.DeliverRequest{Source: &rpcpb.ServiceKey{}, Target: &rpcpb.ServiceKey{Name: "room", Id: 1}, StringMessageId: "player.join", Payload: []byte{1}},
		},
		{
			name: "route-invalidate", op: opRouteInvalidate,
			message: &rpcpb.RouteInvalidate{Target: &rpcpb.ServiceKey{Name: "room", Id: 1}},
		},
		{
			name: "subscribe", op: opSubscribe,
			message: &rpcpb.SubscribeRequest{Subscriber: &rpcpb.ServiceKey{Name: "watcher", Id: 1}, ServiceName: "room"},
		},
		{
			name: "service-discovery", op: opServiceDiscovery,
			message: &rpcpb.ServiceDiscovery{
				Subscriber:  &rpcpb.ServiceKey{Name: "watcher", Id: 1},
				ServiceName: "room",
				EventType:   rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_SNAPSHOT,
				Services:    []*rpcpb.ServiceKey{{Name: "room", Id: 1}},
			},
		},
		{name: "heartbeat", op: opHeartbeat, message: &rpcpb.HeartbeatRequest{}},
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

func TestDecodeDeliverRequestValidatesMessageID(t *testing.T) {
	request := &rpcpb.DeliverRequest{
		Source:    &rpcpb.ServiceKey{},
		Target:    &rpcpb.ServiceKey{Name: "room", Id: 1},
		MessageId: 42,
	}

	_, _, messageID, err := decodeDeliverRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if messageID.kind != serviceMessageIDNumeric || messageID.number != 42 {
		t.Fatalf("message id = %+v, want numeric 42", messageID)
	}

	request.MessageId = 0
	if _, _, _, err := decodeDeliverRequest(request); err == nil {
		t.Fatal("deliver request with zero message id was accepted")
	}

	request.StringMessageId = "player.join"
	_, _, messageID, err = decodeDeliverRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if messageID.kind != serviceMessageIDString || messageID.text != "player.join" {
		t.Fatalf("message id = %+v, want string player.join", messageID)
	}

	request.MessageId = 42
	if _, _, _, err := decodeDeliverRequest(request); err == nil {
		t.Fatal("deliver request with both message id forms was accepted")
	}

	request.MessageId = 0
	request.StringMessageId = strings.Repeat("x", maxStringMessageIDSize+1)
	if _, _, _, err := decodeDeliverRequest(request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("oversized string message id error = %v", err)
	}

	request.StringMessageId = string([]byte{0xff})
	if _, _, _, err := decodeDeliverRequest(request); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("invalid UTF-8 message id error = %v", err)
	}
}

func TestDeliverProtocolPoolsResetStateAndPreserveReturnedPayloads(t *testing.T) {
	source := ServiceKey{Name: "gateway", ID: 2}
	target := ServiceKey{Name: "room", ID: 7}
	requestPayload := []byte("request")

	request := acquireDeliverSendRequest()
	initializeDeliverRequest(request, source, target, stringServiceMessageID("player.join"), requestPayload)
	encoded, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	releaseDeliverSendRequest(request)

	decoded := acquireDeliverReceiveRequest()
	if err := decodeOperationPayload(encoded, decoded); err != nil {
		releaseDeliverReceiveRequest(decoded)
		t.Fatal(err)
	}
	if got, err := serviceKeyFromProto(decoded.Source); err != nil || got != source {
		releaseDeliverReceiveRequest(decoded)
		t.Fatalf("decoded source = %v, %v", got, err)
	}
	if got, err := serviceKeyFromProto(decoded.Target); err != nil || got != target {
		releaseDeliverReceiveRequest(decoded)
		t.Fatalf("decoded target = %v, %v", got, err)
	}
	if decoded.StringMessageId != "player.join" || !bytes.Equal(decoded.Payload, requestPayload) {
		t.Fatalf("decoded request = %+v", decoded)
	}
	retainedRequestPayload := decoded.Payload
	releaseDeliverReceiveRequest(decoded)
	if !bytes.Equal(retainedRequestPayload, requestPayload) {
		t.Fatalf("request payload changed after pool release: %q", retainedRequestPayload)
	}

	cleanRequest := acquireDeliverSendRequest()
	if cleanRequest.Source == nil || cleanRequest.Target == nil {
		releaseDeliverSendRequest(cleanRequest)
		t.Fatal("pooled request lost reusable service keys")
	}
	if cleanRequest.Source.Name != "" || cleanRequest.Source.Id != 0 ||
		cleanRequest.Target.Name != "" || cleanRequest.Target.Id != 0 ||
		cleanRequest.MessageId != 0 || cleanRequest.StringMessageId != "" || cleanRequest.Payload != nil {
		t.Fatalf("pooled request retained state: %+v", cleanRequest)
	}
	releaseDeliverSendRequest(cleanRequest)

	missingSourceWire, err := proto.Marshal(&rpcpb.DeliverRequest{
		Target:    &rpcpb.ServiceKey{Name: "room", Id: 7},
		MessageId: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	missingSource := acquireDeliverReceiveRequest()
	if err := decodeOperationPayload(missingSourceWire, missingSource); err != nil {
		releaseDeliverReceiveRequest(missingSource)
		t.Fatal(err)
	}
	_, _, _, decodeErr := decodeDeliverRequest(missingSource)
	releaseDeliverReceiveRequest(missingSource)
	if decodeErr == nil {
		t.Fatal("pooled decoder accepted a deliver request without source presence")
	}

	responsePayload := []byte("response")
	response := acquireDeliverResponse()
	response.Payload = responsePayload
	retainedResponsePayload := response.Payload
	releaseDeliverResponse(response)
	if !bytes.Equal(retainedResponsePayload, responsePayload) {
		t.Fatalf("response payload changed after pool release: %q", retainedResponsePayload)
	}
	cleanResponse := acquireDeliverResponse()
	if cleanResponse.Payload != nil {
		t.Fatalf("pooled response retained payload: %q", cleanResponse.Payload)
	}
	releaseDeliverResponse(cleanResponse)
}

func TestRPCOperationResponsesRoundTrip(t *testing.T) {
	responses := []struct {
		name    string
		message operationProtocol
	}{
		{name: "register-node", message: &rpcpb.RegisterNodeResponse{}},
		{name: "heartbeat", message: &rpcpb.HeartbeatResponse{}},
		{name: "register", message: &rpcpb.RegisterResponse{}},
		{name: "unregister", message: &rpcpb.UnregisterResponse{}},
		{name: "subscribe", message: &rpcpb.SubscribeResponse{}},
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
