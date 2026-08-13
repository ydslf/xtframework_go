package xtframework

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"xtnet/net/packet"
)

type operation uint8

const (
	opRegister operation = iota + 1
	opUnregister
	opLookup
	opDeliver
)

type rpcEnvelope struct {
	Operation  operation        `json:"operation"`
	SourceNode int              `json:"source_node,omitempty"`
	Source     ServiceKey       `json:"source,omitempty"`
	Target     ServiceKey       `json:"target,omitempty"`
	Location   *ServiceLocation `json:"location,omitempty"`
	Payload    []byte           `json:"payload,omitempty"`
}

type rpcResult struct {
	Location *ServiceLocation `json:"location,omitempty"`
	Payload  []byte           `json:"payload,omitempty"`
	Code     string           `json:"code,omitempty"`
	Error    string           `json:"error,omitempty"`
}

func encodeEnvelope(envelope rpcEnvelope) ([]byte, error) {
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode rpc envelope: %w", err)
	}
	return data, nil
}

func decodeEnvelope(data []byte) (rpcEnvelope, error) {
	var envelope rpcEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return envelope, fmt.Errorf("decode rpc envelope: %w", err)
	}
	return envelope, nil
}

func encodeResult(result rpcResult) []byte {
	data, err := json.Marshal(result)
	if err != nil {
		// rpcResult 仅包含框架定义且可安全进行 JSON 编码的字段。
		return []byte(`{"error":"encode rpc result"}`)
	}
	return data
}

func decodeResult(data []byte) (rpcResult, error) {
	var result rpcResult
	if err := json.Unmarshal(data, &result); err != nil {
		return result, fmt.Errorf("decode rpc result: %w", err)
	}
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

func writePacket(data []byte) *packet.WritePacket {
	wpk := packet.NewWritePacket(len(data), 5, binary.BigEndian)
	wpk.WriteData(data)
	return wpk
}
