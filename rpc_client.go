package xtframework

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
	}

	netRPC := rpc.NewSync(node.rpcLoop)
	netRPC.SetOnRpcDirect(func(net.ISession, *packet.ReadPacket) {})
	netRPC.SetOnRpcRequest(func(net.ISession, int32, *packet.ReadPacket) {})
	agent := clientagent.NewInternal(node.rpcLoop, byteOrder)
	agent.SetEventHandler(events)
	agent.SetNetRpc(netRPC)

	client := tcp.NewClient(addr, agent)
	c.client, c.netRPC = client, netRPC
	if err := client.ConnectSync(node.connectTimeout); err != nil {
		return nil, fmt.Errorf("connect node %d at %s: %w", nodeID, addr, err)
	}
	c.connected.Store(true)
	return c, nil
}

func (c *RPCClient) NodeID() int     { return c.nodeID }
func (c *RPCClient) Addr() string    { return c.addr }
func (c *RPCClient) Connected() bool { return c.connected.Load() }

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

func (c *RPCClient) request(expireMS time.Duration, op operation, request, response operationProtocol) error {
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
