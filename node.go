package xtframework

import (
	"encoding/binary"
	"errors"
	"fmt"
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

var byteOrder binary.ByteOrder = binary.BigEndian

type nodeOptions struct {
	factories      *FactoryRegistry
	registry       ServiceRegistry
	logger         *xtlog.Logger
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

// WithLogger sets the process-wide xtnet logger shared by the Node and all of
// its Services. The caller owns an injected logger and must close it.
func WithLogger(logger *xtlog.Logger) NodeOption {
	return func(options *nodeOptions) { options.logger = logger }
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

// RemoteNode 表示一个已在当前 RPC session 上完成注册的远端节点。
type RemoteNode struct {
	id      int
	addr    string
	session xtnetNet.ISession
}

func (n *RemoteNode) ID() int      { return n.id }
func (n *RemoteNode) Addr() string { return n.addr }

// Node 表示一个框架进程。Node 的运行状态由框架内部管理，应用程序通过
// 只读方法查询节点信息和服务信息。
type Node struct {
	id         int
	mainNodeID int
	state      atomic.Int32
	config     *Config
	nodeConfig NodeConfig

	logger            Logger
	ownedLogger       *xtlog.Logger
	loggerReleaseOnce sync.Once
	errorHandler      func(error)

	factories     *FactoryRegistry
	serviceOrder  []ServiceKey
	localServices map[ServiceKey]*serviceRuntime

	rpcServer     xtnetNet.IServer
	serverRPC     rpc.IRpc
	remoteNodesMu sync.RWMutex
	remoteNodes   map[xtnetNet.ISession]*RemoteNode
	registry      ServiceRegistry

	connectTimeout time.Duration
	clientsMu      sync.Mutex
	rpcClients     map[int]*RPCClient
	routeCache     *serviceRouteCache

	shutdownMutex sync.Mutex
}

func NewNode(config *Config, nodeID int, optionList ...NodeOption) (*Node, error) {
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
	nativeLogger := options.logger
	var ownedLogger *xtlog.Logger
	if nativeLogger == nil {
		if nodeConfig.Logger == nil {
			return nil, fmt.Errorf("node %d logger config is required when WithLogger is not provided", nodeID)
		}
		var err error
		nativeLogger, err = newXTNetLogger(*nodeConfig.Logger)
		if err != nil {
			return nil, fmt.Errorf("create node %d logger: %w", nodeID, err)
		}
		ownedLogger = nativeLogger
	}
	previousLogger := xtnet.GetLogger()
	xtnet.SetLogger(nativeLogger)
	constructionSucceeded := false
	defer func() {
		if constructionSucceeded {
			return
		}
		if xtnet.GetLogger() == nativeLogger {
			xtnet.SetLogger(previousLogger)
		}
		if ownedLogger != nil {
			ownedLogger.Close()
		}
	}()

	nodeLogger := WithLogFields(nativeLogger, LogField{Key: "node", Value: nodeID})
	if options.errorHandler == nil {
		options.errorHandler = func(err error) { nodeLogger.LogError("xtframework: %v", err) }
	}
	node := &Node{
		id:             nodeID,
		mainNodeID:     config.MainNode,
		registry:       options.registry,
		logger:         nodeLogger,
		ownedLogger:    ownedLogger,
		rpcClients:     make(map[int]*RPCClient),
		config:         config,
		nodeConfig:     nodeConfig,
		factories:      options.factories,
		connectTimeout: options.connectTimeout,
		routeCache:     newServiceRouteCache(options.routeCacheTTL),
		errorHandler:   options.errorHandler,
		localServices:  make(map[ServiceKey]*serviceRuntime),
		remoteNodes:    make(map[xtnetNet.ISession]*RemoteNode),
	}
	node.state.Store(nodeStateInitial)

	loops := make(map[*frame.Loop]ServiceKey)
	for _, serviceConfig := range nodeConfig.Services {
		factory, found := node.factories.Get(serviceConfig.Name)
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
		node.localServices[key] = &serviceRuntime{service: service}
		node.serviceOrder = append(node.serviceOrder, key)
	}
	constructionSucceeded = true
	return node, nil
}

// ID 返回当前节点 ID。
func (n *Node) ID() int { return n.id }

// MainNodeID 返回主节点 ID。
func (n *Node) MainNodeID() int { return n.mainNodeID }

// Logger returns the Node logger. It shares its underlying output with all
// Service loggers created by this Node.
func (n *Node) Logger() Logger { return n.logger }

// ListenAddr 返回当前节点的内部 RPC 监听地址。
func (n *Node) ListenAddr() string { return n.nodeConfig.ListenAddr }

// IsMainNode 表示当前节点是否为主节点。
func (n *Node) IsMainNode() bool { return n.id == n.mainNodeID }

// LocalService 查询当前节点上的一个 Service。
func (n *Node) LocalService(key ServiceKey) (Service, bool) {
	runtime, exists := n.localServices[key]
	if !exists {
		return nil, false
	}
	return runtime.service, true
}

// LocalServices 返回当前节点全部 Service 的只读快照。修改返回的 map
// 不会影响 Node 内部的 Service 容器。
func (n *Node) LocalServices() map[ServiceKey]Service {
	result := make(map[ServiceKey]Service, len(n.localServices))
	for key, runtime := range n.localServices {
		result[key] = runtime.service
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

	if err := n.startRPCServer(); err != nil {
		n.rollbackStart()
		return err
	}
	if !n.IsMainNode() {
		if _, err := n.mainClient(); err != nil {
			n.rollbackStart()
			return fmt.Errorf("register node %d: %w", n.id, err)
		}
	}

	for _, key := range n.serviceOrder {
		runtime := n.localServices[key]
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
		remote := &RemoteNode{session: session}
		session.SetUserData(remote)
		n.remoteNodesMu.Lock()
		n.remoteNodes[session] = remote
		n.remoteNodesMu.Unlock()
	}
	events.OnSessionPacket = func(xtnetNet.IServer, xtnetNet.ISession, *packet.ReadPacket) {}
	events.OnSessionClose = func(_ xtnetNet.IServer, session xtnetNet.ISession) {
		n.remoteNodesMu.Lock()
		remote := n.remoteNodes[session]
		delete(n.remoteNodes, session)
		n.remoteNodesMu.Unlock()
		if remote != nil && remote.id != 0 && n.IsMainNode() {
			n.registry.UnregisterNode(remote.id)
		}
	}

	dispatcher := frame.NewDirectDispatcher()
	n.serverRPC = rpc.NewSync(dispatcher)
	n.serverRPC.SetOnRpcDirect(n.handleRPCDirect)
	n.serverRPC.SetOnRpcRequest(n.handleRPCRequest)
	agent := serveragent.NewInternal(dispatcher, byteOrder)
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
		Location: serviceLocationToProto(location),
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
		Target: serviceKeyToProto(key),
	}
	return client.request(n.connectTimeout, opUnregister, request, &rpcpb.UnregisterResponse{})
}

// Send2Service 异步发送一条业务消息。调用后，调用者不得再修改或复用 payload。
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
		return n.dispatchLocalDirect(source, target, messageID, payload)
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
		Source:    serviceKeyToProto(source),
		Target:    serviceKeyToProto(target),
		MessageId: messageID,
		Payload:   payload,
	})
	if err != nil {
		n.routeCache.invalidate(target)
	}
	return err
}

func (n *Node) CallService(expireMS time.Duration, serviceName string, serviceID int, messageID uint32, payload []byte) ([]byte, error) {
	return n.callService(expireMS, ServiceKey{}, ServiceKey{Name: serviceName, ID: serviceID}, messageID, payload)
}

func (n *Node) callService(expireMS time.Duration, source, target ServiceKey, messageID uint32, payload []byte) ([]byte, error) {
	if !n.operational() {
		return nil, ErrNodeStopped
	}
	if expireMS <= 0 {
		return nil, fmt.Errorf("call service expiration must be positive")
	}
	if messageID == 0 {
		return nil, ErrInvalidMessage
	}
	if _, local := n.localServices[target]; local {
		type localResponse struct {
			payload []byte
			err     error
		}
		responses := make(chan localResponse, 1)
		err := n.dispatchLocalRequest(source, target, messageID, payload, func(responsePayload []byte, responseErr error) error {
			responses <- localResponse{payload: responsePayload, err: responseErr}
			return nil
		})
		if err != nil {
			return nil, err
		}
		timer := time.NewTimer(expireMS)
		defer timer.Stop()
		select {
		case response := <-responses:
			return response.payload, response.err
		case <-timer.C:
			return nil, fmt.Errorf("call service %s: timeout after %s", target, expireMS)
		}
	}

	location, err := n.lookupService(target)
	if err != nil {
		return nil, err
	}
	client, err := n.getRPCClient(location.NodeID, location.NodeAddr)
	if err != nil {
		n.routeCache.invalidate(target)
		return nil, err
	}
	responseProtocol := &rpcpb.DeliverResponse{}
	err = client.request(expireMS, opDeliver, &rpcpb.DeliverRequest{
		Source:    serviceKeyToProto(source),
		Target:    serviceKeyToProto(target),
		MessageId: messageID,
		Payload:   payload,
	}, responseProtocol)
	if err != nil {
		if errors.Is(err, ErrServiceNotFound) || errors.Is(err, ErrRPCDisconnected) {
			n.routeCache.invalidate(target)
		}
		return nil, err
	}
	return responseProtocol.Payload, nil
}

func (n *Node) dispatchLocalDirect(source, target ServiceKey, messageID uint32, payload []byte) error {
	runtime, exists := n.localServices[target]
	if !exists {
		return fmt.Errorf("%w: %s", ErrServiceNotFound, target)
	}
	service := runtime.service
	messageContext := &MessageContext{
		source: source,
		target: target,
	}
	service.Loop().Post(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				err := fmt.Errorf("service %s panic: %v\n%s", target, recovered, debug.Stack())
				n.report(err)
			}
		}()
		if err := service.HandleRPCDirect(messageContext, messageID, payload); err != nil {
			n.report(fmt.Errorf("handle message %d in service %s: %w", messageID, target, err))
		}
	})
	return nil
}

type localResponder func([]byte, error) error

func (n *Node) dispatchLocalRequest(source, target ServiceKey, messageID uint32, payload []byte, responder localResponder) error {
	runtime, exists := n.localServices[target]
	if !exists {
		return fmt.Errorf("%w: %s", ErrServiceNotFound, target)
	}
	service := runtime.service
	messageContext := &MessageContext{
		source: source,
		target: target,
	}
	service.Loop().Post(func() {
		var responsePayload []byte
		var responseErr error
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					responseErr = fmt.Errorf("service %s panic: %v\n%s", target, recovered, debug.Stack())
				}
			}()
			responsePayload, responseErr = service.HandleRPCRequest(messageContext, messageID, payload)
		}()
		if err := responder(responsePayload, responseErr); err != nil {
			n.report(fmt.Errorf("respond to message %d from service %s: %w", messageID, target, err))
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
		Target: serviceKeyToProto(key),
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
	if err := client.request(n.connectTimeout, opRegisterNode, &rpcpb.RegisterNodeRequest{
		NodeId:   int64(n.id),
		NodeAddr: n.nodeConfig.ListenAddr,
	}, &rpcpb.RegisterNodeResponse{}); err != nil {
		client.Close()
		return nil, fmt.Errorf("register node %d with node %d: %w", n.id, nodeID, err)
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
	op, payload, err := decodeEnvelope(rpk)
	if err != nil {
		n.report(err)
		return
	}
	if _, err := n.registeredRemoteNode(session); err != nil {
		n.report(err)
		return
	}
	switch op {
	case opDeliver:
		var request rpcpb.DeliverRequest
		if err := decodeOperationPayload(payload, &request); err != nil {
			n.report(rpcInvalidMessage(err))
			return
		}
		source, target, messageID, err := decodeDeliverRequest(&request)
		if err != nil {
			n.report(rpcInvalidMessage(err))
			return
		}
		if err := n.dispatchLocalDirect(source, target, messageID, request.Payload); err != nil {
			n.report(err)
		}
	default:
		n.report(fmt.Errorf("unsupported direct rpc operation %d", op))
	}
}

func (n *Node) handleRPCRequest(session xtnetNet.ISession, contextID int32, rpk *packet.ReadPacket) {
	op, payload, err := decodeEnvelope(rpk)
	if err != nil {
		n.respondRPCError(session, contextID, rpcInvalidMessage(err))
		return
	}
	if op == opRegisterNode {
		n.handleRegisterNode(session, contextID, payload)
		return
	}
	remoteNode, err := n.registeredRemoteNode(session)
	if err != nil {
		n.respondRPCError(session, contextID, err)
		return
	}
	sourceNode := remoteNode.id
	switch op {
	case opRegister:
		var request rpcpb.RegisterRequest
		if decodeErr := decodeOperationPayload(payload, &request); decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		location, decodeErr := requiredRPCServiceLocation(request.Location, "register location")
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		if !n.IsMainNode() {
			err = fmt.Errorf("node %d is not the main node", n.id)
		} else if location.NodeID != sourceNode || location.NodeAddr != remoteNode.addr {
			err = fmt.Errorf("register location does not match registered node %d", sourceNode)
		} else {
			err = n.registry.Register(location)
		}
		if err != nil {
			n.respondRPCError(session, contextID, err)
			return
		}
		_ = n.respondRPC(session, contextID, &rpcpb.RegisterResponse{})
	case opUnregister:
		var request rpcpb.UnregisterRequest
		if decodeErr := decodeOperationPayload(payload, &request); decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		target, decodeErr := requiredRPCServiceKey(request.Target, "unregister target")
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		if !n.IsMainNode() {
			err = fmt.Errorf("node %d is not the main node", n.id)
		} else {
			err = n.registry.Unregister(target, sourceNode)
		}
		if err != nil {
			n.respondRPCError(session, contextID, err)
			return
		}
		_ = n.respondRPC(session, contextID, &rpcpb.UnregisterResponse{})
	case opLookup:
		var request rpcpb.LookupRequest
		if decodeErr := decodeOperationPayload(payload, &request); decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		target, decodeErr := requiredRPCServiceKey(request.Target, "lookup target")
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
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
		_ = n.respondRPC(session, contextID, &rpcpb.LookupResponse{
			Location: serviceLocationToProto(location),
		})
	case opDeliver:
		var request rpcpb.DeliverRequest
		if decodeErr := decodeOperationPayload(payload, &request); decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		source, target, messageID, decodeErr := decodeDeliverRequest(&request)
		if decodeErr != nil {
			n.respondRPCError(session, contextID, rpcInvalidMessage(decodeErr))
			return
		}
		err = n.dispatchLocalRequest(source, target, messageID, request.Payload, func(responsePayload []byte, responseErr error) error {
			if responseErr != nil {
				n.respondRPCError(session, contextID, responseErr)
				return nil
			}
			return n.respondRPC(session, contextID, &rpcpb.DeliverResponse{
				Payload: responsePayload,
			})
		})
		if err != nil {
			n.respondRPCError(session, contextID, err)
		}
	default:
		n.respondRPCError(session, contextID, fmt.Errorf("unsupported rpc operation %d", op))
	}
}

func decodeDeliverRequest(request *rpcpb.DeliverRequest) (ServiceKey, ServiceKey, uint32, error) {
	source, err := serviceKeyFromProto(request.Source)
	if err != nil {
		return ServiceKey{}, ServiceKey{}, 0, fmt.Errorf("deliver source: %w", err)
	}
	if source != (ServiceKey{}) && (source.Name == "" || source.ID <= 0) {
		return ServiceKey{}, ServiceKey{}, 0, fmt.Errorf("deliver source is invalid: %s", source)
	}
	target, err := requiredRPCServiceKey(request.Target, "deliver target")
	if err != nil {
		return ServiceKey{}, ServiceKey{}, 0, err
	}
	if request.MessageId == 0 {
		return ServiceKey{}, ServiceKey{}, 0, fmt.Errorf("deliver message id is zero")
	}
	return source, target, request.MessageId, nil
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

func rpcNodeID(value int64) (int, error) {
	nodeID, err := rpcInt(value, "node id")
	if err != nil {
		return 0, err
	}
	if nodeID <= 0 {
		return 0, fmt.Errorf("node id must be positive")
	}
	return nodeID, nil
}

func (n *Node) handleRegisterNode(session xtnetNet.ISession, contextID int32, payload []byte) {
	var request rpcpb.RegisterNodeRequest
	if err := decodeOperationPayload(payload, &request); err != nil {
		n.respondRPCError(session, contextID, rpcInvalidMessage(err))
		return
	}
	nodeID, err := rpcNodeID(request.NodeId)
	if err != nil {
		n.respondRPCError(session, contextID, rpcInvalidMessage(err))
		return
	}
	configured, exists := n.config.Node(nodeID)
	if !exists {
		n.respondRPCError(session, contextID, fmt.Errorf("%w: id=%d", ErrNodeNotFound, nodeID))
		return
	}
	if nodeID == n.id {
		n.respondRPCError(session, contextID, rpcInvalidMessage(fmt.Errorf("node %d cannot register with itself", nodeID)))
		return
	}
	if request.NodeAddr != configured.ListenAddr {
		n.respondRPCError(session, contextID, rpcInvalidMessage(fmt.Errorf(
			"node %d address %q does not match configured address %q",
			nodeID, request.NodeAddr, configured.ListenAddr,
		)))
		return
	}
	n.remoteNodesMu.Lock()
	remote := n.remoteNodes[session]
	if remote == nil {
		n.remoteNodesMu.Unlock()
		n.respondRPCError(session, contextID, rpcInvalidMessage(fmt.Errorf("rpc session is not tracked")))
		return
	}
	if remote.id != 0 {
		n.remoteNodesMu.Unlock()
		if remote.id == nodeID && remote.addr == request.NodeAddr {
			_ = n.respondRPC(session, contextID, &rpcpb.RegisterNodeResponse{})
			return
		}
		n.respondRPCError(session, contextID, rpcInvalidMessage(fmt.Errorf("rpc session is already registered as node %d", remote.id)))
		return
	}
	var previous *RemoteNode
	for _, candidate := range n.remoteNodes {
		if candidate != remote && candidate.id == nodeID {
			previous = candidate
			delete(n.remoteNodes, candidate.session)
			break
		}
	}
	remote.id = nodeID
	remote.addr = request.NodeAddr
	n.remoteNodesMu.Unlock()
	if previous != nil {
		previous.session.Close(false)
	}
	_ = n.respondRPC(session, contextID, &rpcpb.RegisterNodeResponse{})
}

func (n *Node) registeredRemoteNode(session xtnetNet.ISession) (*RemoteNode, error) {
	n.remoteNodesMu.RLock()
	remote := n.remoteNodes[session]
	n.remoteNodesMu.RUnlock()
	if remote == nil || remote.id == 0 {
		return nil, fmt.Errorf("%w: rpc session is not registered", ErrNodeNotFound)
	}
	return remote, nil
}

func rpcInvalidMessage(err error) error {
	return fmt.Errorf("%w: rpc protocol: %v", ErrInvalidMessage, err)
}

func (n *Node) respondRPCError(session xtnetNet.ISession, contextID int32, err error) {
	var code, errMessage string
	if err != nil {
		code = "rpc_error"
		errMessage = err.Error()
		switch {
		case errors.Is(err, ErrServiceNotFound):
			code = "service_not_found"
		case errors.Is(err, ErrServiceExists):
			code = "service_exists"
		case errors.Is(err, ErrNodeNotFound):
			code = "node_not_found"
		case errors.Is(err, ErrInvalidMessage):
			code = "invalid_message"
		}
	}
	wpk, encodeErr := encodeResult(code, errMessage, nil)
	if encodeErr != nil {
		n.report(encodeErr)
		return
	}
	n.serverRPC.Respond(session, contextID, wpk)
}

func (n *Node) respondRPC(session xtnetNet.ISession, contextID int32, response operationProtocol) error {
	wpk, err := encodeResult("", "", response)
	if err != nil {
		n.report(err)
		return err
	}
	n.serverRPC.Respond(session, contextID, wpk)
	return nil
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
	if n.state.CompareAndSwap(nodeStateInitial, nodeStateStopped) {
		n.releaseLogger()
		return nil
	}
	if !n.state.CompareAndSwap(nodeStateRunning, nodeStateStopping) {
		if n.state.Load() == nodeStateStopped {
			n.releaseLogger()
			return nil
		}
		return ErrNodeStopped
	}

	var errs []error
	for i := len(n.serviceOrder) - 1; i >= 0; i-- {
		runtime := n.localServices[n.serviceOrder[i]]
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
		if err := n.stopService(n.localServices[n.serviceOrder[i]]); err != nil {
			errs = append(errs, err)
		}
	}
	n.state.Store(nodeStateStopped)
	n.releaseLogger()
	return errors.Join(errs...)
}

func (n *Node) releaseLogger() {
	n.loggerReleaseOnce.Do(func() {
		if n.ownedLogger != nil {
			n.ownedLogger.Close()
		}
	})
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

	n.remoteNodesMu.Lock()
	remoteNodes := make([]*RemoteNode, 0, len(n.remoteNodes))
	for _, remote := range n.remoteNodes {
		remoteNodes = append(remoteNodes, remote)
	}
	n.remoteNodes = make(map[xtnetNet.ISession]*RemoteNode)
	n.remoteNodesMu.Unlock()
	for _, remote := range remoteNodes {
		remote.session.Close(false)
	}
	if n.rpcServer != nil {
		n.rpcServer.Close()
		n.rpcServer = nil
	}
}

func (n *Node) rollbackStart() {
	for i := len(n.serviceOrder) - 1; i >= 0; i-- {
		runtime := n.localServices[n.serviceOrder[i]]
		if runtime.registered {
			_ = n.unregisterService(n.serviceOrder[i])
			runtime.registered = false
		}
	}
	n.closeNetwork()
	for i := len(n.serviceOrder) - 1; i >= 0; i-- {
		_ = n.stopService(n.localServices[n.serviceOrder[i]])
	}
	n.state.Store(nodeStateStopped)
	n.releaseLogger()
}
