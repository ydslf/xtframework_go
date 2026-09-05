package xtframework

import (
	"fmt"

	"xtframework/internal/rpcpb"
	"xtnet/net/packet"

	"google.golang.org/protobuf/proto"
)

type operation = rpcpb.RpcOperation

const (
	opNone       operation = iota
	opRegister             = rpcpb.RpcOperation_RPC_OPERATION_REGISTER
	opUnregister           = rpcpb.RpcOperation_RPC_OPERATION_UNREGISTER
	opLookup               = rpcpb.RpcOperation_RPC_OPERATION_LOOKUP
	opDeliver              = rpcpb.RpcOperation_RPC_OPERATION_DELIVER
)

type rpcResult struct {
	Payload []byte
	Code    string
	Error   string
}

type operationProtocol interface {
	proto.Message
}

func encodeOperationEnvelope(op operation, message operationProtocol) (*packet.WritePacket, error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, fmt.Errorf("encode rpc operation payload: message is nil")
	}
	msgSize := proto.Size(message)
	wpk := packet.NewWritePacket(msgSize+2, 5, byteOrder)
	wpk.WriteInt16(int16(op))
	buffer := wpk.GetData()[:0]
	data, err := proto.MarshalOptions{}.MarshalAppend(buffer, message)
	if err != nil {
		return nil, err
	}
	if len(data) != msgSize {
		return nil, fmt.Errorf("protobuf size changed during marshal: got %d, want %d", len(data), msgSize)
	}
	wpk.AddPos(msgSize)
	return wpk, nil
}

func decodeEnvelope(rpk *packet.ReadPacket) (operation, []byte, error) {
	if rpk.GetLeftSize() < 2 {
		return opNone, nil, fmt.Errorf("decode rpc envelope: missing operation")
	}
	op := operation(rpk.ReadInt16())
	return op, rpk.GetCurData(), nil
}

func decodeOperationPayload(payLoad []byte, message operationProtocol) error {
	if err := proto.Unmarshal(payLoad, message); err != nil {
		return fmt.Errorf("decode rpc payload: %w", err)
	}
	return nil
}

func encodeOperationResult(message operationProtocol) (*packet.WritePacket, error) {
	payload, err := encodeOperationPayload(message)
	if err != nil {
		return nil, err
	}
	data, err := encodeResult(rpcResult{Payload: payload})
	if err != nil {
		return nil, err
	}
	return writePacket(data), nil
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
	wpk := packet.NewWritePacket(len(data), 5, byteOrder)
	wpk.WriteData(data)
	return wpk
}
