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
	xttimer "xtnet/timer"
)

type RPCClient struct {
	node             *Node
	nodeID           int
	addr             string
	client           net.IClient
	netRPC           rpc.IRpc
	connected        atomic.Bool
	closeOnce        sync.Once
	heartbeatTimer   *xttimer.WheelTimer
	heartbeatPending bool
}

func newRPCClient(node *Node, nodeID int, addr string) (*RPCClient, error) {
	c := &RPCClient{
		node:           node,
		nodeID:         nodeID,
		addr:           addr,
		heartbeatTimer: node.controlTimeWheel.NewTimer(),
	}

	events := eventhandler.NewClientEventHandler()
	events.OnClientPacket = func(net.IClient, *packet.ReadPacket) {}
	events.OnConnectionBroken = func(net.IClient) {
		c.markDisconnected()
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
	case opServiceDiscovery:
		if c.nodeID != c.node.mainNodeID || c.node.IsMainNode() {
			c.node.report(fmt.Errorf("service discovery push from node %d is not allowed", c.nodeID))
			return
		}
		var notification rpcpb.ServiceDiscovery
		if err := decodeOperationPayload(payload, &notification); err != nil {
			c.node.report(rpcInvalidMessage(err))
			return
		}
		subscriber, err := requiredRPCServiceKey(notification.Subscriber, "discovery subscriber")
		if err != nil {
			c.node.report(rpcInvalidMessage(err))
			return
		}
		serviceName, err := normalizeSubscribedServiceName(notification.ServiceName)
		if err != nil || serviceName != notification.ServiceName {
			if err == nil {
				err = fmt.Errorf("service name is not normalized")
			}
			c.node.report(rpcInvalidMessage(err))
			return
		}
		services, err := requiredRPCServiceKeys(notification.Services, "discovery services")
		if err != nil {
			c.node.report(rpcInvalidMessage(err))
			return
		}
		for _, service := range services {
			if service.Name != serviceName {
				c.node.report(rpcInvalidMessage(fmt.Errorf("discovery service %s does not match subscribed name %q", service, serviceName)))
				return
			}
		}
		if err := c.node.dispatchLocalServiceDiscovery(subscriber, serviceName, notification.EventType, services); err != nil {
			c.node.report(err)
		}
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

func (c *RPCClient) startHeartbeat() {
	c.node.controlLoop.Post(func() {
		if c.Connected() {
			c.scheduleHeartbeat()
		}
	})
}

func (c *RPCClient) scheduleHeartbeat() {
	c.heartbeatTimer.Start(c.node.heartbeatInterval, 0, c.sendHeartbeat)
}

func (c *RPCClient) sendHeartbeat() {
	if !c.Connected() || c.heartbeatPending {
		return
	}
	c.heartbeatPending = true
	err := c.requestAsync(c.node.heartbeatTimeout, opHeartbeat, &rpcpb.HeartbeatRequest{}, &rpcpb.HeartbeatResponse{}, func(err error) {
		if !c.Connected() {
			return
		}
		c.node.controlLoop.Post(func() { c.handleHeartbeatResult(err) })
	})
	if err != nil {
		c.handleHeartbeatResult(err)
	}
}

func (c *RPCClient) handleHeartbeatResult(err error) {
	if !c.heartbeatPending {
		return
	}
	c.heartbeatPending = false
	if !c.Connected() {
		return
	}
	if err != nil {
		c.Close()
		return
	}
	c.scheduleHeartbeat()
}

func (c *RPCClient) markDisconnected() {
	if !c.connected.Swap(false) {
		return
	}
	if c.heartbeatTimer != nil {
		c.heartbeatTimer.Stop()
	}
	c.node.removeRPCClient(c.nodeID, c)
	if c.nodeID == c.node.mainNodeID {
		c.node.routeCache.invalidateAll()
		c.node.startMainRecovery()
	}
}

func (c *RPCClient) Close() {
	c.markDisconnected()
	c.closeTransport()
}

func (c *RPCClient) closeTransport() {
	c.connected.Store(false)
	if c.heartbeatTimer != nil {
		c.heartbeatTimer.Stop()
	}
	c.closeOnce.Do(func() {
		if c.client != nil {
			c.client.Close(false)
		}
	})
}
