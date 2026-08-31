package xtframework

import (
	"encoding/binary"
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
	connected := make(chan error, 1)

	events := eventhandler.NewClientEventHandler()
	events.OnConnectSuccess = func(net.IClient) {
		c.connected.Store(true)
		connected <- nil
	}
	events.OnConnectFailed = func(net.IClient) {
		c.connected.Store(false)
		connected <- fmt.Errorf("connect node %d at %s failed", nodeID, addr)
	}
	events.OnClientPacket = func(net.IClient, *packet.ReadPacket) {}
	events.OnConnectionBroken = func(net.IClient) {
		c.connected.Store(false)
		node.removeRPCClient(nodeID, c)
	}

	netRPC := rpc.NewSync(node.rpcLoop)
	netRPC.SetOnRpcDirect(func(net.ISession, *packet.ReadPacket) {})
	netRPC.SetOnRpcRequest(func(net.ISession, int32, *packet.ReadPacket) {})
	agent := clientagent.NewInternal(node.rpcLoop, binary.BigEndian)
	agent.SetEventHandler(events)
	agent.SetNetRpc(netRPC)

	client := tcp.NewClient(addr, agent)
	c.client, c.netRPC = client, netRPC
	if !client.Connect() {
		return nil, fmt.Errorf("connect node %d at %s: invalid client state", nodeID, addr)
	}
	select {
	case err := <-connected:
		return c, err
	case <-time.After(node.connectTimeout):
		if client.GetSession() != nil {
			client.Close(false)
		}
		return nil, fmt.Errorf("connect node %d at %s: timeout after %s", nodeID, addr, node.connectTimeout)
	}
}

func (c *RPCClient) NodeID() int     { return c.nodeID }
func (c *RPCClient) Addr() string    { return c.addr }
func (c *RPCClient) Connected() bool { return c.connected.Load() }

func (c *RPCClient) send(envelope rpcEnvelope) error {
	session := c.client.GetSession()
	if !c.connected.Load() || session == nil {
		return ErrRPCDisconnected
	}
	data, err := encodeEnvelope(envelope)
	if err != nil {
		return err
	}
	c.netRPC.SendDirect(session, writePacket(data))
	return nil
}

func (c *RPCClient) request(expireMS time.Duration, envelope rpcEnvelope) (rpcResult, error) {
	session := c.client.GetSession()
	if !c.connected.Load() || session == nil {
		return rpcResult{}, ErrRPCDisconnected
	}
	data, err := encodeEnvelope(envelope)
	if err != nil {
		return rpcResult{}, err
	}

	rpk, err := c.netRPC.RequestSync(session, writePacket(data), expireMS)
	if err != nil {
		return rpcResult{}, err
	}
	return decodeResult(rpk.GetCurData())
}

func (c *RPCClient) Close() {
	c.closeOnce.Do(func() {
		c.connected.Store(false)
		if c.client != nil {
			c.client.Close(false)
		}
	})
}
