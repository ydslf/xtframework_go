package xtframework

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"xtframework/internal/rpcpb"
	"xtnet/frame"
	"xtnet/net"
	clientagent "xtnet/net/agent/client"
	"xtnet/net/eventhandler"
	"xtnet/net/packet"
	"xtnet/net/rpc"
	"xtnet/net/tcp"
)

type RPCClient struct {
	node      *Node
	nodeID    int
	addr      string
	client    net.IClient
	netRPC    rpc.IRpc
	connected atomic.Bool
	closeOnce sync.Once
}

func newRPCClient(node *Node, nodeID int, addr string) (*RPCClient, error) {
	c := &RPCClient{node: node, nodeID: nodeID, addr: addr}

	events := eventhandler.NewClientEventHandler()
	events.OnClientPacket = func(net.IClient, *packet.ReadPacket) {}
	events.OnConnectionBroken = func(net.IClient) {
		c.connected.Store(false)
		node.removeRPCClient(nodeID, c)
		if nodeID == node.mainNodeID {
			node.routeCache.invalidateAll()
		}
	}

	dispatcher := frame.NewDirectDispatcher()
	netRPC := rpc.NewSync(dispatcher)
	netRPC.SetOnRpcDirect(c.handleRPCDirect)
	netRPC.SetOnRpcRequest(func(net.ISession, int32, *packet.ReadPacket) {})
	agent := clientagent.NewInternal(dispatcher, byteOrder)
	agent.SetEventHandler(events)
	agent.SetNetRpc(netRPC)

	client := tcp.NewClient(addr, agent)
	c.client, c.netRPC = client, netRPC
	if err := client.ConnectSync(node.connectTimeout); err != nil {
		return nil, fmt.Errorf("connect node %d at %s: %w", nodeID, addr, err)
	}
	if nodeID == node.mainNodeID {
		node.routeCache.invalidateAll()
	}
	c.connected.Store(true)
	return c, nil
}

func (c *RPCClient) NodeID() int     { return c.nodeID }
func (c *RPCClient) Addr() string    { return c.addr }
func (c *RPCClient) Connected() bool { return c.connected.Load() }

func (c *RPCClient) handleRPCDirect(_ net.ISession, rpk *packet.ReadPacket) {
	op, payload, err := decodeEnvelope(rpk)
	if err != nil {
		c.node.report(err)
		return
	}
	switch op {
	case opRouteInvalidate:
		if c.nodeID != c.node.mainNodeID || c.node.IsMainNode() {
			c.node.report(fmt.Errorf("route push from node %d is not allowed", c.nodeID))
			return
		}
		var notification rpcpb.RouteInvalidate
		if err := decodeOperationPayload(payload, &notification); err != nil {
			c.node.report(rpcInvalidMessage(err))
			return
		}
		key, err := requiredRPCServiceKey(notification.Target, "route invalidate target")
		if err != nil {
			c.node.report(rpcInvalidMessage(err))
			return
		}
		c.node.routeCache.invalidate(key)
	default:
		c.node.report(fmt.Errorf("unsupported pushed rpc operation %d", op))
	}
}

func (c *RPCClient) send(op operation, message operationProtocol) error {
	session := c.client.GetSession()
	if !c.connected.Load() || session == nil {
		return ErrRPCDisconnected
	}
	wpk, err := encodeEnvelope(op, message)
	if err != nil {
		return err
	}
	c.netRPC.SendDirect(session, wpk)

	return nil
}

func (c *RPCClient) requestAsync(expireMS time.Duration, op operation, request, response operationProtocol, callback func(error)) error {
	session := c.client.GetSession()
	if !c.connected.Load() || session == nil {
		return ErrRPCDisconnected
	}
	wpk, err := encodeEnvelope(op, request)
	if err != nil {
		return err
	}

	c.netRPC.RequestAsync(session, wpk, expireMS, func(rpk *packet.ReadPacket, requestErr error) {
		if requestErr != nil {
			callback(requestErr)
			return
		}
		payload, err := decodeResult(rpk)
		if err == nil {
			err = decodeResultPayload(payload, response)
		}
		callback(err)
	})
	return nil
}

func (c *RPCClient) requestSync(expireMS time.Duration, op operation, request, response operationProtocol) error {
	session := c.client.GetSession()
	if !c.connected.Load() || session == nil {
		return ErrRPCDisconnected
	}
	wpk, err := encodeEnvelope(op, request)
	if err != nil {
		return err
	}

	rpk, err := c.netRPC.RequestSync(session, wpk, expireMS)
	if err != nil {
		return err
	}
	payload, err := decodeResult(rpk)
	if err != nil {
		return err
	}
	return decodeResultPayload(payload, response)
}

func (c *RPCClient) Close() {
	c.closeOnce.Do(func() {
		c.connected.Store(false)
		if c.client != nil {
			c.client.Close(false)
		}
	})
}
