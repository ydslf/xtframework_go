package xtframework

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"xtframework/internal/rpcpb"
	xtnet "xtnet"
	"xtnet/frame"
	xtlog "xtnet/log"
	xtnetNet "xtnet/net"
	serveragent "xtnet/net/agent/server"
	"xtnet/net/eventhandler"
	"xtnet/net/packet"
	"xtnet/net/rpc"
	"xtnet/net/tcp"
)

const (
	nodeStateInitial int32 = iota
	nodeStateStarting
	nodeStateRunning
	nodeStateStopping
	nodeStateStopped
)

const defaultRouteCacheTTL = 30 * time.Second

var xtnetLoggerOnce sync.Once

func ensureXTNetLogger() {
	xtnetLoggerOnce.Do(func() {
		if xtnet.GetLogger() == nil {
			// xtnet 假定进程中始终存在全局日志记录器。这里提供一个静默的
			// 默认实现；应用程序可以在创建 Node 前安装自己的日志记录器。
			xtnet.SetLogger(xtlog.NewLogger(".", xtlog.FileSizeMin, false, false))
		}
	})
}

type nodeOptions struct {
	factories      *FactoryRegistry
	registry       ServiceRegistry
	connectTimeout time.Duration
	routeCacheTTL  time.Duration
	errorHandler   func(error)
}

type NodeOption func(*nodeOptions)

func WithFactoryRegistry(registry *FactoryRegistry) NodeOption {
	return func(options *nodeOptions) { options.factories = registry }
}

func WithServiceRegistry(registry ServiceRegistry) NodeOption {
	return func(options *nodeOptions) { options.registry = registry }
}

func WithConnectTimeout(timeout time.Duration) NodeOption {
	return func(options *nodeOptions) { options.connectTimeout = timeout }
}

// WithRouteCacheTTL 设置非主节点的 Service 路由缓存有效期。设置为 0
// 可以禁用路由缓存；默认有效期为 30 秒。
func WithRouteCacheTTL(ttl time.Duration) NodeOption {
	return func(options *nodeOptions) { options.routeCacheTTL = ttl }
}

func WithErrorHandler(handler func(error)) NodeOption {
	return func(options *nodeOptions) { options.errorHandler = handler }
}

type serviceRuntime struct {
	service    Service
	wg         sync.WaitGroup
	started    bool
	registered bool
}

// Node 表示一个框架进程。Node 的运行状态由框架内部管理，应用程序通过
// 只读方法查询节点信息和服务信息。
type Node struct {
	id            int
	mainNodeID    int
	localServices map[ServiceKey]Service
	registry      ServiceRegistry
	rpcServer     xtnetNet.IServer

	config         *Config
	nodeConfig     NodeConfig
	factories      *FactoryRegistry
	connectTimeout time.Duration
	routeCache     *serviceRouteCache
	errorHandler   func(error)

	state atomic.Int32

	rpcLoop   *frame.Loop
	rpcLoopWG sync.WaitGroup
	serverRPC rpc.IRpc

	services      map[ServiceKey]*serviceRuntime
	serviceOrder  []ServiceKey
	clientsMu     sync.Mutex
	rpcClients    map[int]*RPCClient
	sessionsMu    sync.Mutex
	sessions      map[xtnetNet.ISession]struct{}
	shutdownMutex sync.Mutex
}

func NewNode(config *Config, nodeID int, optionList ...NodeOption) (*Node, error) {
	ensureXTNetLogger()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	nodeConfig, exists := config.Node(nodeID)
	if !exists {
		return nil, fmt.Errorf("%w: id=%d", ErrNodeNotFound, nodeID)
	}

	options := nodeOptions{
		factories:      defaultFactories,
		registry:       NewMemoryRegistry(),
		connectTimeout: 5 * time.Second,
		routeCacheTTL:  defaultRouteCacheTTL,
		errorHandler: func(err error) {
			log.Printf("xtframework: %v", err)
		},
	}
	for _, apply := range optionList {
		if apply != nil {
			apply(&options)
		}
	}
	if options.factories == nil {
		return nil, fmt.Errorf("factory registry is nil")
	}
	if options.registry == nil {
		return nil, fmt.Errorf("service registry is nil")
	}
	if options.connectTimeout <= 0 {
		return nil, fmt.Errorf("connect timeout must be positive")
	}
	if options.routeCacheTTL < 0 {
		return nil, fmt.Errorf("route cache TTL must not be negative")
	}
	if options.errorHandler == nil {
		options.errorHandler = func(error) {}
	}
	node := &Node{
		id:             nodeID,
		mainNodeID:     config.MainNode,
		localServices:  make(map[ServiceKey]Service),
		registry:       options.registry,
		rpcClients:     make(map[int]*RPCClient),
		config:         config,
		nodeConfig:     nodeConfig,
		factories:      options.factories,
		connectTimeout: options.connectTimeout,
		routeCache:     newServiceRouteCache(options.routeCacheTTL),
		errorHandler:   options.errorHandler,
		rpcLoop:        frame.NewLoop(frame.LoopSizeMin, true),
		services:       make(map[ServiceKey]*serviceRuntime),
		sessions:       make(map[xtnetNet.ISession]struct{}),
	}
	node.state.Store(nodeStateInitial)

	loops := make(map[*frame.Loop]ServiceKey)
	for _, serviceConfig := range nodeConfig.Services {
		factory, found := options.factories.Get(serviceConfig.Name)
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrFactoryNotFound, serviceConfig.Name)
		}
		service, err := factory(node, serviceConfig)
		if err != nil {
			return nil, fmt.Errorf("create service %s:%d: %w", serviceConfig.Name, serviceConfig.ID, err)
		}
		if service == nil || service.Loop() == nil {
			return nil, fmt.Errorf("factory %q returned a nil service or loop", serviceConfig.Name)
		}
		key := ServiceKey{Name: serviceConfig.Name, ID: serviceConfig.ID}
		if service.Name() != key.Name || service.ID() != key.ID {
			return nil, fmt.Errorf("factory %q returned service %s:%d, want %s", serviceConfig.Name, service.Name(), service.ID(), key)
		}
		if owner, duplicate := loops[service.Loop()]; duplicate {
			return nil, fmt.Errorf("services %s and %s share one loop", owner, key)
		}
		loops[service.Loop()] = key
		node.localServices[key] = service
		node.services[key] = &serviceRuntime{service: service}
		node.serviceOrder = append(node.serviceOrder, key)
	}
	return node, nil
}

// ID 返回当前节点 ID。
func (n *Node) ID() int { return n.id }

// MainNodeID 返回主节点 ID。
func (n *Node) MainNodeID() int { return n.mainNodeID }

// ListenAddr 返回当前节点的内部 RPC 监听地址。
func (n *Node) ListenAddr() string { return n.nodeConfig.ListenAddr }

// IsMainNode 表示当前节点是否为主节点。
func (n *Node) IsMainNode() bool { return n.id == n.mainNodeID }

// LocalService 查询当前节点上的一个 Service。
func (n *Node) LocalService(key ServiceKey) (Service, bool) {
	service, exists := n.localServices[key]
	return service, exists
}

// LocalServices 返回当前节点全部 Service 的只读快照。修改返回的 map
// 不会影响 Node 内部的 Service 容器。
func (n *Node) LocalServices() map[ServiceKey]Service {
	result := make(map[ServiceKey]Service, len(n.localServices))
	for key, service := range n.localServices {
		result[key] = service
	}
	return result
}

// RegisteredService 查询注册表中的一个 Service。非主节点默认只维护空的
// 本地注册表，因此该方法主要用于观察主节点状态。
func (n *Node) RegisteredService(key ServiceKey) (ServiceLocation, bool) {
	return n.registry.Lookup(key)
}

// RegisteredServices 返回当前注册表的 Service 快照。
func (n *Node) RegisteredServices() []ServiceLocation {
	return n.registry.List()
}

// RPCClientCount 返回当前缓存的节点间 RPC 客户端数量。
func (n *Node) RPCClientCount() int {
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()
	return len(n.rpcClients)
}

func (n *Node) Start() error {
	if !n.state.CompareAndSwap(nodeStateInitial, nodeStateStarting) {
		return ErrNodeRunning
	}

	startFrameLoop(n.rpcLoop, &n.rpcLoopWG)

	if err := n.startRPCServer(); err != nil {
		n.rollbackStart()
		return err
	}

	for _, key := range n.serviceOrder {
		runtime := n.services[key]
		if err := n.startService(runtime); err != nil {
			n.rollbackStart()
			return fmt.Errorf("start service %s: %w", key, err)
		}
		if err := n.registerService(key); err != nil {
			n.rollbackStart()
			return fmt.Errorf("register service %s: %w", key, err)
		}
		runtime.registered = true
	}

	n.state.Store(nodeStateRunning)
	return nil
}

func (n *Node) startRPCServer() error {
	events := eventhandler.NewServerEventHandler()
	events.OnAccept = func(_ xtnetNet.IServer, session xtnetNet.ISession) {
		n.sessionsMu.Lock()
		n.sessions[session] = struct{}{}
		n.sessionsMu.Unlock()
	}
	events.OnSessionPacket = func(xtnetNet.IServer, xtnetNet.ISession, *packet.ReadPacket) {}
	events.OnSessionClose = func(_ xtnetNet.IServer, session xtnetNet.ISession) {
		n.sessionsMu.Lock()
		delete(n.sessions, session)
		n.sessionsMu.Unlock()
		if n.IsMainNode() {
			if nodeID, ok := session.GetUserData().(int); ok && nodeID != n.id {
				n.registry.UnregisterNode(nodeID)
			}
		}
	}

	n.serverRPC = rpc.NewNoSync(n.rpcLoop)
	n.serverRPC.SetOnRpcDirect(n.handleRPCDirect)
	n.serverRPC.SetOnRpcRequest(n.handleRPCRequest)
	agent := serveragent.NewInternal(n.rpcLoop, binary.BigEndian)
	agent.SetEventHandler(events)
	agent.SetNetRpc(n.serverRPC)
	n.rpcServer = tcp.NewServer(n.nodeConfig.ListenAddr, agent)
	if !n.rpcServer.Start() {
		return fmt.Errorf("start node %d rpc server at %s", n.id, n.nodeConfig.ListenAddr)
	}
	return nil
}

func (n *Node) startService(runtime *serviceRuntime) error {
	startFrameLoop(runtime.service.Loop(), &runtime.wg)
	if err := callOnLoop(runtime.service.Loop(), runtime.service.Start); err != nil {
		runtime.service.Loop().Close(false)
		runtime.wg.Wait()
		return err
	}
	runtime.started = true
	return nil
}

// startFrameLoop 启动 Loop，并等待 Loop 真正开始处理队列任务。
// 函数返回后，可以确认 Loop 已进入运行状态并能够处理后续的 Post。
func startFrameLoop(loop *frame.Loop, wg *sync.WaitGroup) {
	ready := make(chan struct{})
	loop.Post(func() { close(ready) })
	wg.Add(1)
	go func() {
		defer wg.Done()
		loop.Run()
	}()
	<-ready
}

func callOnLoop(loop *frame.Loop, call func() error) error {
	done := make(chan error, 1)
	loop.Post(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				done <- fmt.Errorf("panic: %v\n%s", recovered, debug.Stack())
			}
		}()
		done <- call()
	})
	return <-done
}

func (n *Node) registerService(key ServiceKey) error {
	location := ServiceLocation{
		ServiceName: key.Name,
		ServiceID:   key.ID,
		NodeID:      n.id,
		NodeAddr:    n.nodeConfig.ListenAddr,
	}
	if n.IsMainNode() {
		return n.registry.Register(location)
	}
	client, err := n.mainClient()
	if err != nil {
		return err
	}
	request := &rpcpb.RegisterRequest{
		SourceNode: int64(n.id),
		Location:   serviceLocationToProto(location),
	}
	return client.request(n.connectTimeout, opRegister, request, &rpcpb.RegisterResponse{})
}

func (n *Node) unregisterService(key ServiceKey) error {
	if n.IsMainNode() {
		return n.registry.Unregister(key, n.id)
	}
	client, err := n.mainClient()
	if err != nil {
		return err
	}
	request := &rpcpb.UnregisterRequest{
		SourceNode: int64(n.id),
		Target:     serviceKeyToProto(key),
	}
	return client.request(n.connectTimeout, opUnregister, request, &rpcpb.UnregisterResponse{})
}

func (n *Node) Send2Service(serviceName string, serviceID int, messageID uint32, payload []byte) error {
	return n.send2Service(ServiceKey{}, ServiceKey{Name: serviceName, ID: serviceID}, messageID, payload)
}

func (n *Node) send2Service(source, target ServiceKey, messageID uint32, payload []byte) error {
	if !n.operational() {
		return ErrNodeStopped
	}
	if messageID == 0 {
		return ErrInvalidMessage
	}
	if _, local := n.localServices[target]; local {
		return n.dispatchLocal(source, target, messageID, clonePayload(payload), false, nil)
	}

	wirePayload, err := encodeMessage(messageID, payload)
	if err != nil {
		return err
	}
	location, err := n.lookupService(target)
	if err != nil {
		return err
	}
	client, err := n.getRPCClient(location.NodeID, location.NodeAddr)
	if err != nil {
		n.routeCache.invalidate(target)
		return err
	}
	err = client.send(opDeliver, &rpcpb.DeliverRequest{
		SourceNode: int64(n.id),
		Source:     serviceKeyToProto(source),
		Target:     serviceKeyToProto(target),
		Payload:    wirePayload,
	})
	if err != nil {
		n.routeCache.invalidate(target)
	}
	return err
}

func (n *Node) CallService(expireMS time.Duration, serviceName string, serviceID int, messageID uint32, payload []byte) (uint32, []byte, error) {
	return n.callService(expireMS, ServiceKey{}, ServiceKey{Name: serviceName, ID: serviceID}, messageID, payload)
}

func (n *Node) callService(expireMS time.Duration, source, target ServiceKey, messageID uint32, payload []byte) (uint32, []byte, error) {
	if !n.operational() {
		return 0, nil, ErrNodeStopped
	}
	if expireMS <= 0 {
		return 0, nil, fmt.Errorf("call service expiration must be positive")
	}
	if messageID == 0 {
		return 0, nil, ErrInvalidMessage
	}
	if _, local := n.localServices[target]; local {
		type localResponse struct {
			messageID uint32
			payload   []byte
			err       error
		}
		responses := make(chan localResponse, 1)
		err := n.dispatchLocal(source, target, messageID, clonePayload(payload), true, func(responseID uint32, responsePayload []byte, responseErr error) error {
			responses <- localResponse{messageID: responseID, payload: responsePayload, err: responseErr}
			return nil
		})
		if err != nil {
			return 0, nil, err
		}
		timer := time.NewTimer(expireMS)
		defer timer.Stop()
		select {
		case response := <-responses:
			return response.messageID, response.payload, response.err
		case <-timer.C:
			return 0, nil, fmt.Errorf("call service %s: timeout after %s", target, expireMS)
		}
	}

	wirePayload, err := encodeMessage(messageID, payload)
	if err != nil {
		return 0, nil, err
	}
	location, err := n.lookupService(target)
	if err != nil {
		return 0, nil, err
	}
	client, err := n.getRPCClient(location.NodeID, location.NodeAddr)
	if err != nil {
		n.routeCache.invalidate(target)
		return 0, nil, err
	}
	responseProtocol := &rpcpb.DeliverResponse{}
	err = client.request(expireMS, opDeliver, &rpcpb.DeliverRequest{
		SourceNode: int64(n.id),
		Source:     serviceKeyToProto(source),
		Target:     serviceKeyToProto(target),
		Payload:    wirePayload,
	}, responseProtocol)
	if err != nil {
		if errors.Is(err, ErrServiceNotFound) || errors.Is(err, ErrRPCDisconnected) {
			n.routeCache.invalidate(target)
		}
		return 0, nil, err
	}
	return decodeMessage(responseProtocol.Payload)
}

func (n *Node) dispatchLocal(source, target ServiceKey, messageID uint32, payload []byte, request bool, responder func(uint32, []byte, error) error) error {
	service, exists := n.localServices[target]
	if !exists {
		return fmt.Errorf("%w: %s", ErrServiceNotFound, target)
	}
	messageContext := &MessageContext{
		source:  source,
		target:  target,
		request: request,
		respond: responder,
	}
	service.Loop().Post(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				err := fmt.Errorf("service %s panic: %v\n%s", target, recovered, debug.Stack())
				if request && !messageContext.responded.Load() {
					_ = messageContext.respondWithError(0, nil, err)
				} else {
					n.report(err)
				}
			}
		}()
		err := service.HandleMessage(messageContext, messageID, payload)
		if request && !messageContext.responded.Load() {
			if err == nil {
				err = fmt.Errorf("service %s returned without responding", target)
			}
			if responseErr := messageContext.respondWithError(0, nil, err); responseErr != nil {
				n.report(responseErr)
			}
		} else if err != nil {
			n.report(fmt.Errorf("handle message %d in service %s: %w", messageID, target, err))
		}
	})
	return nil
}

func (n *Node) lookupService(key ServiceKey) (ServiceLocation, error) {
	if n.IsMainNode() {
		if location, exists := n.registry.Lookup(key); exists {
			return location, nil
		}
		return ServiceLocation{}, fmt.Errorf("%w: %s", ErrServiceNotFound, key)
	}
	return n.routeCache.lookup(key, func() (ServiceLocation, error) {
		return n.lookupServiceFromMain(key)
	})
}

func (n *Node) lookupServiceFromMain(key ServiceKey) (ServiceLocation, error) {
	client, err := n.mainClient()
	if err != nil {
		return ServiceLocation{}, err
	}
	response := &rpcpb.LookupResponse{}
	err = client.request(n.connectTimeout, opLookup, &rpcpb.LookupRequest{
		SourceNode: int64(n.id),
		Target:     serviceKeyToProto(key),
	}, response)
	if err != nil {
		return ServiceLocation{}, err
	}
	if response.Location == nil {
		return ServiceLocation{}, fmt.Errorf("%w: %s", ErrServiceNotFound, key)
	}
	location, err := requiredRPCServiceLocation(response.Location, "lookup response location")
	if err != nil {
		return ServiceLocation{}, rpcInvalidMessage(err)
	}
	return location, nil
}

func (n *Node) mainClient() (*RPCClient, error) {
	mainConfig, exists := n.config.Node(n.mainNodeID)
	if !exists {
		return nil, fmt.Errorf("%w: main node %d", ErrNodeNotFound, n.mainNodeID)
	}
	return n.getRPCClient(mainConfig.ID, mainConfig.ListenAddr)
}

func (n *Node) getRPCClient(nodeID int, addr string) (*RPCClient, error) {
	if nodeID == n.id {
		return nil, fmt.Errorf("cannot create an rpc client to local node %d", nodeID)
	}
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()
	if current := n.rpcClients[nodeID]; current != nil {
		if current.Connected() && current.Addr() == addr {
			return current, nil
		}
		current.Close()
		delete(n.rpcClients, nodeID)
	}
	client, err := newRPCClient(n, nodeID, addr)
	if err != nil {
		return nil, err
	}
	n.rpcClients[nodeID] = client
	return client, nil
}

func (n *Node) removeRPCClient(nodeID int, client *RPCClient) {
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()
	if n.rpcClients[nodeID] == client {
		delete(n.rpcClients, nodeID)
	}
}

func (n *Node) handleRPCDirect(session xtnetNet.ISession, rpk *packet.ReadPacket) {
	envelope, err := decodeEnvelope(rpk.GetCurData())
	if err != nil {
		n.report(err)
		return
	}
	if envelope.Operation != opDeliver {
		n.report(fmt.Errorf("unsupported direct rpc operation %d", envelope.Operation))
		return
	}
	var request rpcpb.DeliverRequest
	if err := decodeOperationPayload(envelope, opDeliver, &request); err != nil {
		n.report(rpcInvalidMessage(err))
		return
	}
	sourceNode, source, target, err := decodeDeliverRequest(&request)
	if err != nil {
		n.report(rpcInvalidMessage(err))
		return
	}
	n.rememberSourceNode(session, sourceNode)
	messageID, payload, err := decodeMessage(request.Payload)
	if err != nil {
		n.report(err)
		return
	}
	if err := n.dispatchLocal(source, target, messageID, payload, false, nil); err != nil {
		n.report(err)
	}
}

func (n *Node) handleRPCRequest(session xtnetNet.ISession, contextID int32, rpk *packet.ReadPacket) {
	envelope, err := decodeEnvelope(rpk.GetCurData())
	if err != nil {
		n.respondRPCError(session, contextID, rpcInvalidMessage(err))
		return
	}
	switch envelope.Operation {
	case opRegister:
		var request rpcpb.RegisterRequest
		if decodeErr := decodeOperationPayload(envelope, opRegister, &request); decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		sourceNode, decodeErr := rpcSourceNode(request.SourceNode)
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		location, decodeErr := requiredRPCServiceLocation(request.Location, "register location")
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		n.rememberSourceNode(session, sourceNode)
		if !n.IsMainNode() {
			err = fmt.Errorf("node %d is not the main node", n.id)
		} else if location.NodeID != sourceNode {
			err = fmt.Errorf("register source node mismatch")
		} else {
			err = n.registry.Register(location)
		}
		if err != nil {
			n.respondRPCError(session, contextID, err)
			return
		}
		_ = n.respondRPC(session, contextID, opRegister, &rpcpb.RegisterResponse{})
	case opUnregister:
		var request rpcpb.UnregisterRequest
		if decodeErr := decodeOperationPayload(envelope, opUnregister, &request); decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		sourceNode, decodeErr := rpcSourceNode(request.SourceNode)
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		target, decodeErr := requiredRPCServiceKey(request.Target, "unregister target")
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		n.rememberSourceNode(session, sourceNode)
		if !n.IsMainNode() {
			err = fmt.Errorf("node %d is not the main node", n.id)
		} else {
			err = n.registry.Unregister(target, sourceNode)
		}
		if err != nil {
			n.respondRPCError(session, contextID, err)
			return
		}
		_ = n.respondRPC(session, contextID, opUnregister, &rpcpb.UnregisterResponse{})
	case opLookup:
		var request rpcpb.LookupRequest
		if decodeErr := decodeOperationPayload(envelope, opLookup, &request); decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		sourceNode, decodeErr := rpcSourceNode(request.SourceNode)
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		target, decodeErr := requiredRPCServiceKey(request.Target, "lookup target")
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		n.rememberSourceNode(session, sourceNode)
		if !n.IsMainNode() {
			err = fmt.Errorf("node %d is not the main node", n.id)
			n.respondRPCError(session, contextID, err)
			return
		}
		location, found := n.registry.Lookup(target)
		if !found {
			n.respondRPCError(session, contextID, fmt.Errorf("%w: %s", ErrServiceNotFound, target))
			return
		}
		_ = n.respondRPC(session, contextID, opLookup, &rpcpb.LookupResponse{
			Location: serviceLocationToProto(location),
		})
	case opDeliver:
		var request rpcpb.DeliverRequest
		if decodeErr := decodeOperationPayload(envelope, opDeliver, &request); decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		sourceNode, source, target, decodeErr := decodeDeliverRequest(&request)
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		n.rememberSourceNode(session, sourceNode)
		messageID, payload, decodeErr := decodeMessage(request.Payload)
		if decodeErr != nil {
			n.respondRPCError(session, contextID, decodeErr)
			return
		}
		err = n.dispatchLocal(source, target, messageID, payload, true, func(responseID uint32, responsePayload []byte, responseErr error) error {
			if responseErr != nil {
				n.respondRPCError(session, contextID, responseErr)
				return nil
			}
			wirePayload, encodeErr := encodeMessage(responseID, responsePayload)
			if encodeErr != nil {
				n.respondRPCError(session, contextID, encodeErr)
				return encodeErr
			}
			return n.respondRPC(session, contextID, opDeliver, &rpcpb.DeliverResponse{
				Payload: wirePayload,
			})
		})
		if err != nil {
			n.respondRPCError(session, contextID, err)
		}
	default:
		n.respondRPCError(session, contextID, fmt.Errorf("unsupported rpc operation %d", envelope.Operation))
	}
}

func decodeDeliverRequest(request *rpcpb.DeliverRequest) (int, ServiceKey, ServiceKey, error) {
	sourceNode, err := rpcSourceNode(request.SourceNode)
	if err != nil {
		return 0, ServiceKey{}, ServiceKey{}, err
	}
	source, err := serviceKeyFromProto(request.Source)
	if err != nil {
		return 0, ServiceKey{}, ServiceKey{}, fmt.Errorf("deliver source: %w", err)
	}
	if source != (ServiceKey{}) && (source.Name == "" || source.ID <= 0) {
		return 0, ServiceKey{}, ServiceKey{}, fmt.Errorf("deliver source is invalid: %s", source)
	}
	target, err := requiredRPCServiceKey(request.Target, "deliver target")
	if err != nil {
		return 0, ServiceKey{}, ServiceKey{}, err
	}
	if len(request.Payload) == 0 {
		return 0, ServiceKey{}, ServiceKey{}, fmt.Errorf("deliver payload is empty")
	}
	return sourceNode, source, target, nil
}

func requiredRPCServiceKey(message *rpcpb.ServiceKey, name string) (ServiceKey, error) {
	key, err := serviceKeyFromProto(message)
	if err != nil {
		return ServiceKey{}, fmt.Errorf("%s: %w", name, err)
	}
	if key.Name == "" || key.ID <= 0 {
		return ServiceKey{}, fmt.Errorf("%s is invalid: %s", name, key)
	}
	return key, nil
}

func requiredRPCServiceLocation(message *rpcpb.ServiceLocation, name string) (ServiceLocation, error) {
	location, err := serviceLocationFromProto(message)
	if err != nil {
		return ServiceLocation{}, fmt.Errorf("%s: %w", name, err)
	}
	if location.ServiceName == "" || location.ServiceID <= 0 || location.NodeID <= 0 || location.NodeAddr == "" {
		return ServiceLocation{}, fmt.Errorf("%s is invalid: %+v", name, location)
	}
	return location, nil
}

func rpcSourceNode(value int64) (int, error) {
	nodeID, err := rpcInt(value, "source node")
	if err != nil {
		return 0, err
	}
	if nodeID <= 0 {
		return 0, fmt.Errorf("source node must be positive")
	}
	return nodeID, nil
}

func rpcInvalidMessage(err error) error {
	return fmt.Errorf("%w: rpc protocol: %v", ErrInvalidMessage, err)
}

func (n *Node) rememberSourceNode(session xtnetNet.ISession, nodeID int) {
	if nodeID > 0 {
		session.SetUserData(nodeID)
	}
}

func (n *Node) respondRPCError(session xtnetNet.ISession, contextID int32, err error) {
	result := rpcResult{}
	if err != nil {
		result.Error = err.Error()
		switch {
		case errors.Is(err, ErrServiceNotFound):
			result.Code = "service_not_found"
		case errors.Is(err, ErrServiceExists):
			result.Code = "service_exists"
		case errors.Is(err, ErrNodeNotFound):
			result.Code = "node_not_found"
		case errors.Is(err, ErrInvalidMessage):
			result.Code = "invalid_message"
		}
	}
	data, encodeErr := encodeResult(result)
	if encodeErr != nil {
		n.report(encodeErr)
		return
	}
	n.serverRPC.Respond(session, contextID, writePacket(data))
}

func (n *Node) respondRPC(session xtnetNet.ISession, contextID int32, op operation, response operationProtocol) error {
	data, err := encodeOperationResult(op, response)
	if err != nil {
		n.report(err)
		return err
	}
	n.serverRPC.Respond(session, contextID, writePacket(data))
	return nil
}

func (n *Node) postRPC(call func()) {
	n.rpcLoop.Post(call)
}

func (n *Node) operational() bool {
	state := n.state.Load()
	return state == nodeStateStarting || state == nodeStateRunning
}

func (n *Node) report(err error) {
	if err != nil {
		n.errorHandler(err)
	}
}

func (n *Node) Stop() error {
	n.shutdownMutex.Lock()
	defer n.shutdownMutex.Unlock()
	if !n.state.CompareAndSwap(nodeStateRunning, nodeStateStopping) {
		if n.state.Load() == nodeStateStopped {
			return nil
		}
		return ErrNodeStopped
	}

	var errs []error
	for i := len(n.serviceOrder) - 1; i >= 0; i-- {
		runtime := n.services[n.serviceOrder[i]]
		if !runtime.registered {
			continue
		}
		if err := n.unregisterService(n.serviceOrder[i]); err != nil && !errors.Is(err, ErrServiceNotFound) {
			errs = append(errs, err)
		}
		runtime.registered = false
	}
	n.closeNetwork()
	for i := len(n.serviceOrder) - 1; i >= 0; i-- {
		if err := n.stopService(n.services[n.serviceOrder[i]]); err != nil {
			errs = append(errs, err)
		}
	}
	n.rpcLoop.Close(false)
	n.rpcLoopWG.Wait()
	n.state.Store(nodeStateStopped)
	return errors.Join(errs...)
}

func (n *Node) stopService(runtime *serviceRuntime) error {
	if runtime == nil || !runtime.started {
		return nil
	}
	err := callOnLoop(runtime.service.Loop(), runtime.service.Stop)
	runtime.service.Loop().Close(false)
	runtime.wg.Wait()
	runtime.started = false
	return err
}

func (n *Node) closeNetwork() {
	n.clientsMu.Lock()
	clients := make([]*RPCClient, 0, len(n.rpcClients))
	for _, client := range n.rpcClients {
		clients = append(clients, client)
	}
	n.rpcClients = make(map[int]*RPCClient)
	n.clientsMu.Unlock()
	for _, client := range clients {
		client.Close()
	}

	n.sessionsMu.Lock()
	sessions := make([]xtnetNet.ISession, 0, len(n.sessions))
	for session := range n.sessions {
		sessions = append(sessions, session)
	}
	n.sessions = make(map[xtnetNet.ISession]struct{})
	n.sessionsMu.Unlock()
	for _, session := range sessions {
		session.Close(false)
	}
	if n.rpcServer != nil {
		n.rpcServer.Close()
		n.rpcServer = nil
	}
}

func (n *Node) rollbackStart() {
	for i := len(n.serviceOrder) - 1; i >= 0; i-- {
		runtime := n.services[n.serviceOrder[i]]
		if runtime.registered {
			_ = n.unregisterService(n.serviceOrder[i])
			runtime.registered = false
		}
	}
	n.closeNetwork()
	for i := len(n.serviceOrder) - 1; i >= 0; i-- {
		_ = n.stopService(n.services[n.serviceOrder[i]])
	}
	n.rpcLoop.Close(false)
	n.rpcLoopWG.Wait()
	n.state.Store(nodeStateStopped)
}
