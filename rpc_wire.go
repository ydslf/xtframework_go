package xtframework

import (
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"
	"xtframework/internal/rpcpb"
	"xtnet/net/packet"
)

type operation = rpcpb.RpcOperation

const (
	opRegister   = rpcpb.RpcOperation_RPC_OPERATION_REGISTER
	opUnregister = rpcpb.RpcOperation_RPC_OPERATION_UNREGISTER
	opLookup     = rpcpb.RpcOperation_RPC_OPERATION_LOOKUP
	opDeliver    = rpcpb.RpcOperation_RPC_OPERATION_DELIVER
)

type rpcEnvelope struct {
	Operation operation
	Payload   []byte
}

type rpcResult struct {
	Payload []byte
	Code    string
	Error   string
}

type operationProtocol interface {
	proto.Message
}

func encodeOperationEnvelope(op operation, message operationProtocol) ([]byte, error) {
	payload, err := encodeOperationPayload(message)
	if err != nil {
		return nil, err
	}
	return encodeEnvelope(rpcEnvelope{Operation: op, Payload: payload})
}

func encodeEnvelope(envelope rpcEnvelope) ([]byte, error) {
	if !validOperation(envelope.Operation) {
		return nil, fmt.Errorf("encode rpc envelope: unsupported operation %d", envelope.Operation)
	}
	if len(envelope.Payload) == 0 {
		return nil, fmt.Errorf("encode rpc envelope: operation %d has empty payload", envelope.Operation)
	}
	data, err := proto.Marshal(&rpcpb.RpcEnvelope{Operation: envelope.Operation, Payload: envelope.Payload})
	if err != nil {
		return nil, fmt.Errorf("encode rpc envelope: %w", err)
	}
	return data, nil
}

func decodeEnvelope(data []byte) (rpcEnvelope, error) {
	var wire rpcpb.RpcEnvelope
	if err := proto.Unmarshal(data, &wire); err != nil {
		return rpcEnvelope{}, fmt.Errorf("decode rpc envelope: %w", err)
	}
	if !validOperation(wire.Operation) {
		return rpcEnvelope{}, fmt.Errorf("decode rpc envelope: unsupported operation %d", wire.Operation)
	}
	if len(wire.Payload) == 0 {
		return rpcEnvelope{}, fmt.Errorf("decode rpc envelope: operation %d has empty payload", wire.Operation)
	}
	return rpcEnvelope{Operation: wire.Operation, Payload: wire.Payload}, nil
}

func decodeOperationPayload(envelope rpcEnvelope, want operation, message operationProtocol) error {
	if envelope.Operation != want {
		return fmt.Errorf("decode rpc operation payload: envelope operation %d, want %d", envelope.Operation, want)
	}
	if err := proto.Unmarshal(envelope.Payload, message); err != nil {
		return fmt.Errorf("decode rpc operation %d payload: %w", want, err)
	}
	return nil
}

func encodeOperationResult(message operationProtocol) ([]byte, error) {
	payload, err := encodeOperationPayload(message)
	if err != nil {
		return nil, err
	}
	return encodeResult(rpcResult{Payload: payload})
}

func encodeOperationPayload(message operationProtocol) ([]byte, error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, fmt.Errorf("encode rpc operation payload: message is nil")
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("encode rpc operation payload: %w", err)
	}
	return payload, nil
}

func encodeResult(result rpcResult) ([]byte, error) {
	data, err := proto.Marshal(&rpcpb.RpcResult{
		Payload:      result.Payload,
		ErrorCode:    result.Code,
		ErrorMessage: result.Error,
	})
	if err != nil {
		return nil, fmt.Errorf("encode rpc result: %w", err)
	}
	return data, nil
}

func decodeResult(data []byte) (rpcResult, error) {
	var wire rpcpb.RpcResult
	if err := proto.Unmarshal(data, &wire); err != nil {
		return rpcResult{}, fmt.Errorf("decode rpc result: %w", err)
	}
	result := rpcResult{Payload: wire.Payload, Code: wire.ErrorCode, Error: wire.ErrorMessage}
	if result.Error != "" {
		var cause error
		switch result.Code {
		case "service_not_found":
			cause = ErrServiceNotFound
		case "service_exists":
			cause = ErrServiceExists
		case "node_not_found":
			cause = ErrNodeNotFound
		case "invalid_message":
			cause = ErrInvalidMessage
		}
		if cause != nil {
			return result, fmt.Errorf("%w: remote rpc: %s", cause, result.Error)
		}
		return result, fmt.Errorf("remote rpc: %s", result.Error)
	}
	return result, nil
}

func decodeResultPayload(result rpcResult, message operationProtocol) error {
	if message == nil || !message.ProtoReflect().IsValid() {
		return fmt.Errorf("decode rpc operation result: message is nil")
	}
	if err := proto.Unmarshal(result.Payload, message); err != nil {
		return fmt.Errorf("decode rpc operation result: %w", err)
	}
	return nil
}

func validOperation(op operation) bool {
	switch op {
	case opRegister, opUnregister, opLookup, opDeliver:
		return true
	default:
		return false
	}
}

func serviceKeyToProto(key ServiceKey) *rpcpb.ServiceKey {
	return &rpcpb.ServiceKey{Name: key.Name, Id: int64(key.ID)}
}

func serviceKeyFromProto(key *rpcpb.ServiceKey) (ServiceKey, error) {
	if key == nil {
		return ServiceKey{}, fmt.Errorf("service key is missing")
	}
	id, err := rpcInt(key.Id, "service id")
	if err != nil {
		return ServiceKey{}, err
	}
	return ServiceKey{Name: key.Name, ID: id}, nil
}

func serviceLocationToProto(location ServiceLocation) *rpcpb.ServiceLocation {
	return &rpcpb.ServiceLocation{
		ServiceName: location.ServiceName,
		ServiceId:   int64(location.ServiceID),
		NodeId:      int64(location.NodeID),
		NodeAddr:    location.NodeAddr,
	}
}

func serviceLocationFromProto(location *rpcpb.ServiceLocation) (ServiceLocation, error) {
	if location == nil {
		return ServiceLocation{}, fmt.Errorf("service location is missing")
	}
	serviceID, err := rpcInt(location.ServiceId, "service id")
	if err != nil {
		return ServiceLocation{}, err
	}
	nodeID, err := rpcInt(location.NodeId, "node id")
	if err != nil {
		return ServiceLocation{}, err
	}
	return ServiceLocation{
		ServiceName: location.ServiceName,
		ServiceID:   serviceID,
		NodeID:      nodeID,
		NodeAddr:    location.NodeAddr,
	}, nil
}

func rpcInt(value int64, name string) (int, error) {
	converted := int(value)
	if int64(converted) != value {
		return 0, fmt.Errorf("%s %d overflows int", name, value)
	}
	return converted, nil
}

func writePacket(data []byte) *packet.WritePacket {
	wpk := packet.NewWritePacket(len(data), 5, binary.BigEndian)
	wpk.WriteData(data)
	return wpk
}
