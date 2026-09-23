package xtframework

import (
	"fmt"
	"sync"

	"xtframework/internal/rpcpb"
	"xtnet/net/packet"

	"google.golang.org/protobuf/proto"
)

type operation = rpcpb.RpcOperation

const (
	opNone             operation = iota
	opRegisterNode               = rpcpb.RpcOperation_RPC_OPERATION_REGISTER_NODE
	opRegister                   = rpcpb.RpcOperation_RPC_OPERATION_REGISTER
	opUnregister                 = rpcpb.RpcOperation_RPC_OPERATION_UNREGISTER
	opLookup                     = rpcpb.RpcOperation_RPC_OPERATION_LOOKUP
	opDeliver                    = rpcpb.RpcOperation_RPC_OPERATION_DELIVER
	opRouteInvalidate            = rpcpb.RpcOperation_RPC_OPERATION_ROUTE_INVALIDATE
	opHeartbeat                  = rpcpb.RpcOperation_RPC_OPERATION_HEARTBEAT
	opSubscribe                  = rpcpb.RpcOperation_RPC_OPERATION_SUBSCRIBE
	opServiceDiscovery           = rpcpb.RpcOperation_RPC_OPERATION_SERVICE_DISCOVERY
)

type operationProtocol interface {
	proto.Message
}

var (
	// 发送和接收请求使用独立对象池，因为 proto.Unmarshal 会重置根消息，
	// 从而丢弃发送侧需要复用的嵌套 ServiceKey 对象。
	deliverSendRequestPool = sync.Pool{
		New: func() any {
			return &rpcpb.DeliverRequest{
				Source: &rpcpb.ServiceKey{},
				Target: &rpcpb.ServiceKey{},
			}
		},
	}
	deliverReceiveRequestPool = sync.Pool{
		New: func() any { return &rpcpb.DeliverRequest{} },
	}
	deliverResponsePool = sync.Pool{
		New: func() any { return &rpcpb.DeliverResponse{} },
	}
)

const maxRPCStringSize = 1<<15 - 1

func encodeEnvelope(op operation, message operationProtocol) (*packet.WritePacket, error) {
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

func acquireDeliverSendRequest() *rpcpb.DeliverRequest {
	return deliverSendRequestPool.Get().(*rpcpb.DeliverRequest)
}

func initializeDeliverRequest(request *rpcpb.DeliverRequest, source, target ServiceKey, messageID serviceMessageID, payload []byte) {
	request.Source.Name = source.Name
	request.Source.Id = int64(source.ID)
	request.Target.Name = target.Name
	request.Target.Id = int64(target.ID)
	request.Payload = payload
	if messageID.kind == serviceMessageIDString {
		request.StringMessageId = messageID.text
	} else {
		request.MessageId = messageID.number
	}
}

func releaseDeliverSendRequest(request *rpcpb.DeliverRequest) {
	if request == nil {
		return
	}
	source, target := request.Source, request.Target
	proto.Reset(request)
	if source == nil {
		source = &rpcpb.ServiceKey{}
	} else {
		proto.Reset(source)
	}
	if target == nil {
		target = &rpcpb.ServiceKey{}
	} else {
		proto.Reset(target)
	}
	request.Source = source
	request.Target = target
	deliverSendRequestPool.Put(request)
}

func acquireDeliverReceiveRequest() *rpcpb.DeliverRequest {
	return deliverReceiveRequestPool.Get().(*rpcpb.DeliverRequest)
}

func releaseDeliverReceiveRequest(request *rpcpb.DeliverRequest) {
	if request == nil {
		return
	}
	proto.Reset(request)
	deliverReceiveRequestPool.Put(request)
}

func acquireDeliverResponse() *rpcpb.DeliverResponse {
	return deliverResponsePool.Get().(*rpcpb.DeliverResponse)
}

func releaseDeliverResponse(response *rpcpb.DeliverResponse) {
	if response == nil {
		return
	}
	proto.Reset(response)
	deliverResponsePool.Put(response)
}

func encodeResult(code, errMessage string, message operationProtocol) (*packet.WritePacket, error) {
	if len(code) > maxRPCStringSize {
		return nil, fmt.Errorf("encode rpc result: code is too long: %d", len(code))
	}
	if len(errMessage) > maxRPCStringSize {
		return nil, fmt.Errorf("encode rpc result: error message is too long: %d", len(errMessage))
	}
	msgSize := 0
	if message != nil {
		if !message.ProtoReflect().IsValid() {
			return nil, fmt.Errorf("encode rpc result: message is nil")
		}
		msgSize = proto.Size(message)
	}
	wpk := packet.NewWritePacket(4+len(code)+len(errMessage)+msgSize, 5, byteOrder)
	wpk.WriteString(code)
	wpk.WriteString(errMessage)
	if message == nil {
		return wpk, nil
	}
	data, err := proto.MarshalOptions{}.MarshalAppend(wpk.GetData()[:0], message)
	if err != nil {
		return nil, fmt.Errorf("encode rpc result: %w", err)
	}
	if len(data) != msgSize {
		return nil, fmt.Errorf("protobuf size changed during marshal: got %d, want %d", len(data), msgSize)
	}
	wpk.AddPos(msgSize)
	return wpk, nil
}

func decodeResult(rpk *packet.ReadPacket) ([]byte, error) {
	code, err := readResultString(rpk, "code")
	if err != nil {
		return nil, err
	}
	errMessage, err := readResultString(rpk, "error message")
	if err != nil {
		return nil, err
	}
	if code != "" {
		var cause error
		switch code {
		case "service_not_found":
			cause = ErrServiceNotFound
		case "service_exists":
			cause = ErrServiceExists
		case "node_not_found":
			cause = ErrNodeNotFound
		case "node_exists":
			cause = ErrNodeExists
		case "invalid_message":
			cause = ErrInvalidMessage
		}
		if cause != nil {
			return nil, fmt.Errorf("%w: remote rpc: %s", cause, errMessage)
		}
		return nil, fmt.Errorf("remote rpc: %s", errMessage)
	}
	return rpk.GetCurData(), nil
}

func readResultString(rpk *packet.ReadPacket, name string) (string, error) {
	if rpk.GetLeftSize() < 2 {
		return "", fmt.Errorf("decode rpc result: missing %s length", name)
	}
	size := int(rpk.PeakInt16())
	if size < 0 {
		return "", fmt.Errorf("decode rpc result: invalid %s length %d", name, size)
	}
	if rpk.GetLeftSize() < size+2 {
		return "", fmt.Errorf("decode rpc result: incomplete %s", name)
	}
	return rpk.ReadString(), nil
}

func decodeResultPayload(payload []byte, message operationProtocol) error {
	if message == nil || !message.ProtoReflect().IsValid() {
		return fmt.Errorf("decode rpc operation result: message is nil")
	}
	if err := proto.Unmarshal(payload, message); err != nil {
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

func requiredRPCServiceKeys(keys []*rpcpb.ServiceKey, name string) ([]ServiceKey, error) {
	result := make([]ServiceKey, 0, len(keys))
	for i, message := range keys {
		key, err := requiredRPCServiceKey(message, fmt.Sprintf("%s[%d]", name, i))
		if err != nil {
			return nil, err
		}
		result = append(result, key)
	}
	return result, nil
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
