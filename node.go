package xtframework

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

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
	messages       *MessageRegistry
	codec          Codec
	registry       ServiceRegistry
	connectTimeout time.Duration
	errorHandler   func(error)
}

type NodeOption func(*nodeOptions)

func WithFactoryRegistry(registry *FactoryRegistry) NodeOption {
	return func(options *nodeOptions) { options.factories = registry }
}

func WithMessageRegistry(registry *MessageRegistry) NodeOption {
	return func(options *nodeOptions) { options.messages = registry }
}

func WithCodec(codec Codec) NodeOption {
	return func(options *nodeOptions) { options.codec = codec }
}

func WithServiceRegistry(registry ServiceRegistry) NodeOption {
	return func(options *nodeOptions) { options.registry = registry }
}

func WithConnectTimeout(timeout time.Duration) NodeOption {
	return func(options *nodeOptions) { options.connectTimeout = timeout }
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

// Node 表示一个框架进程。RPCClients 和 LocalService 用于观察运行状态，
// 应用程序应将它们视为只读成员。
type Node struct {
	ID           int
	MainNodeID   int
	LocalService map[ServiceKey]Service
	Registry     ServiceRegistry
	RPCServer    xtnetNet.IServer
	RPCClients   map[int]*RPCClient
	Messages     *MessageRegistry
	Codec        Codec

	config         *Config
	nodeConfig     NodeConfig
	factories      *FactoryRegistry
	connectTimeout time.Duration
	errorHandler   func(error)

	state atomic.Int32

	rpcLoop   *frame.Loop
	rpcLoopWG sync.WaitGroup
	serverRPC rpc.IRpc

	services      map[ServiceKey]*serviceRuntime
	serviceOrder  []ServiceKey
	clientsMu     sync.Mutex
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
		messages:       NewMessageRegistry(),
		registry:       NewMemoryRegistry(),
		connectTimeout: 5 * time.Second,
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
	if options.messages == nil {
		return nil, fmt.Errorf("message registry is nil")
	}
	if options.registry == nil {
		return nil, fmt.Errorf("service registry is nil")
	}
	if options.connectTimeout <= 0 {
		return nil, fmt.Errorf("connect timeout must be positive")
	}
	if options.errorHandler == nil {
		options.errorHandler = func(error) {}
	}
	if options.codec == nil {
		switch config.Codec {
		case "", "xtnet":
			options.codec = NewXTNetCodec(options.messages)
		case "protobuf", "proto":
			options.codec = NewProtoCodec(options.messages)
		default:
			return nil, fmt.Errorf("unsupported codec %q", config.Codec)
		}
	}

	node := &Node{
		ID:             nodeID,
		MainNodeID:     config.MainNode,
		LocalService:   make(map[ServiceKey]Service),
		Registry:       options.registry,
		RPCClients:     make(map[int]*RPCClient),
		Messages:       options.messages,
		Codec:          options.codec,
		config:         config,
		nodeConfig:     nodeConfig,
		factories:      options.factories,
		connectTimeout: options.connectTimeout,
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
		node.LocalService[key] = service
		node.services[key] = &serviceRuntime{service: service}
		node.serviceOrder = append(node.serviceOrder, key)
	}
	return node, nil
}

func (n *Node) IsMainNode() bool { return n.ID == n.MainNodeID }

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
			if nodeID, ok := session.GetUserData().(int); ok && nodeID != n.ID {
				n.Registry.UnregisterNode(nodeID)
			}
		}
	}

	n.serverRPC = rpc.NewNoSync(n.rpcLoop)
	n.serverRPC.SetOnRpcDirect(n.handleRPCDirect)
	n.serverRPC.SetOnRpcRequest(n.handleRPCRequest)
	agent := serveragent.NewInternal(n.rpcLoop, binary.BigEndian)
	agent.SetEventHandler(events)
	agent.SetNetRpc(n.serverRPC)
	n.RPCServer = tcp.NewServer(n.nodeConfig.ListenAddr, agent)
	if !n.RPCServer.Start() {
		return fmt.Errorf("start node %d rpc server at %s", n.ID, n.nodeConfig.ListenAddr)
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

// startFrameLoop 在 Run 启动前向队列放入一个屏障任务。该任务既用于确认
// Loop 已就绪，也会围绕 xtnet 非原子的 Loop 状态字段建立 happens-before
// 关系，避免第一次网络 Post 与 Run 发生数据竞争。
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
		NodeID:      n.ID,
		NodeAddr:    n.nodeConfig.ListenAddr,
	}
	if n.IsMainNode() {
		return n.Registry.Register(location)
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.connectTimeout)
	defer cancel()
	client, err := n.mainClient()
	if err != nil {
		return err
	}
	_, err = client.request(ctx, rpcEnvelope{Operation: opRegister, SourceNode: n.ID, Location: &location})
	return err
}

func (n *Node) unregisterService(key ServiceKey) error {
	if n.IsMainNode() {
		return n.Registry.Unregister(key, n.ID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.connectTimeout)
	defer cancel()
	client, err := n.mainClient()
	if err != nil {
		return err
	}
	_, err = client.request(ctx, rpcEnvelope{Operation: opUnregister, SourceNode: n.ID, Target: key})
	return err
}

func (n *Node) Send2Service(serviceName string, serviceID int, msg *Message) error {
	return n.send2Service(ServiceKey{}, ServiceKey{Name: serviceName, ID: serviceID}, msg)
}

func (n *Node) send2Service(source, target ServiceKey, msg *Message) error {
	if !n.operational() {
		return ErrNodeStopped
	}
	if msg == nil || msg.ID == 0 || msg.Payload == nil {
		return ErrInvalidMessage
	}
	if _, local := n.LocalService[target]; local {
		return n.dispatchLocal(context.Background(), source, target, msg, false, nil)
	}

	payload, err := n.Codec.Encode(msg)
	if err != nil {
		return err
	}
	location, err := n.lookupService(target)
	if err != nil {
		return err
	}
	client, err := n.getRPCClient(location.NodeID, location.NodeAddr)
	if err != nil {
		return err
	}
	return client.send(rpcEnvelope{
		Operation: opDeliver, SourceNode: n.ID, Source: source, Target: target, Payload: payload,
	})
}

func (n *Node) CallService(ctx context.Context, serviceName string, serviceID int, req *Message) (*Message, error) {
	return n.callService(ctx, ServiceKey{}, ServiceKey{Name: serviceName, ID: serviceID}, req)
}

func (n *Node) callService(ctx context.Context, source, target ServiceKey, req *Message) (*Message, error) {
	if !n.operational() {
		return nil, ErrNodeStopped
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if req == nil || req.ID == 0 || req.Payload == nil {
		return nil, ErrInvalidMessage
	}
	if _, local := n.LocalService[target]; local {
		type localResponse struct {
			message *Message
			err     error
		}
		responses := make(chan localResponse, 1)
		err := n.dispatchLocal(ctx, source, target, req, true, func(message *Message, responseErr error) error {
			responses <- localResponse{message: message, err: responseErr}
			return nil
		})
		if err != nil {
			return nil, err
		}
		select {
		case response := <-responses:
			return response.message, response.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	payload, err := n.Codec.Encode(req)
	if err != nil {
		return nil, err
	}
	location, err := n.lookupService(target)
	if err != nil {
		return nil, err
	}
	client, err := n.getRPCClient(location.NodeID, location.NodeAddr)
	if err != nil {
		return nil, err
	}
	result, err := client.request(ctx, rpcEnvelope{
		Operation: opDeliver, SourceNode: n.ID, Source: source, Target: target, Payload: payload,
	})
	if err != nil {
		return nil, err
	}
	var response Message
	if err := n.Codec.Decode(result.Payload, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (n *Node) dispatchLocal(ctx context.Context, source, target ServiceKey, msg *Message, request bool, responder func(*Message, error) error) error {
	service, exists := n.LocalService[target]
	if !exists {
		return fmt.Errorf("%w: %s", ErrServiceNotFound, target)
	}
	messageContext := &MessageContext{
		Context: ctx,
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
					_ = messageContext.respondWithError(nil, err)
				} else {
					n.report(err)
				}
			}
		}()
		err := service.HandleMessage(messageContext, msg)
		if request && !messageContext.responded.Load() {
			if err == nil {
				err = fmt.Errorf("service %s returned without responding", target)
			}
			if responseErr := messageContext.respondWithError(nil, err); responseErr != nil {
				n.report(responseErr)
			}
		} else if err != nil {
			n.report(fmt.Errorf("handle message %d in service %s: %w", msg.ID, target, err))
		}
	})
	return nil
}

func (n *Node) lookupService(key ServiceKey) (ServiceLocation, error) {
	if n.IsMainNode() {
		if location, exists := n.Registry.Lookup(key); exists {
			return location, nil
		}
		return ServiceLocation{}, fmt.Errorf("%w: %s", ErrServiceNotFound, key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.connectTimeout)
	defer cancel()
	client, err := n.mainClient()
	if err != nil {
		return ServiceLocation{}, err
	}
	result, err := client.request(ctx, rpcEnvelope{Operation: opLookup, SourceNode: n.ID, Target: key})
	if err != nil {
		return ServiceLocation{}, err
	}
	if result.Location == nil {
		return ServiceLocation{}, fmt.Errorf("%w: %s", ErrServiceNotFound, key)
	}
	return *result.Location, nil
}

func (n *Node) mainClient() (*RPCClient, error) {
	mainConfig, exists := n.config.Node(n.MainNodeID)
	if !exists {
		return nil, fmt.Errorf("%w: main node %d", ErrNodeNotFound, n.MainNodeID)
	}
	return n.getRPCClient(mainConfig.ID, mainConfig.ListenAddr)
}

func (n *Node) getRPCClient(nodeID int, addr string) (*RPCClient, error) {
	if nodeID == n.ID {
		return nil, fmt.Errorf("cannot create an rpc client to local node %d", nodeID)
	}
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()
	if current := n.RPCClients[nodeID]; current != nil {
		if current.Connected() && current.Addr() == addr {
			return current, nil
		}
		current.Close()
		delete(n.RPCClients, nodeID)
	}
	client, err := newRPCClient(n, nodeID, addr)
	if err != nil {
		return nil, err
	}
	n.RPCClients[nodeID] = client
	return client, nil
}

func (n *Node) removeRPCClient(nodeID int, client *RPCClient) {
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()
	if n.RPCClients[nodeID] == client {
		delete(n.RPCClients, nodeID)
	}
}

func (n *Node) handleRPCDirect(session xtnetNet.ISession, rpk *packet.ReadPacket) {
	envelope, err := decodeEnvelope(rpk.GetCurData())
	if err != nil {
		n.report(err)
		return
	}
	n.rememberSourceNode(session, envelope.SourceNode)
	if envelope.Operation != opDeliver {
		n.report(fmt.Errorf("unsupported direct rpc operation %d", envelope.Operation))
		return
	}
	var message Message
	if err := n.Codec.Decode(envelope.Payload, &message); err != nil {
		n.report(err)
		return
	}
	if err := n.dispatchLocal(context.Background(), envelope.Source, envelope.Target, &message, false, nil); err != nil {
		n.report(err)
	}
}

func (n *Node) handleRPCRequest(session xtnetNet.ISession, contextID int32, rpk *packet.ReadPacket) {
	envelope, err := decodeEnvelope(rpk.GetCurData())
	if err != nil {
		n.respondRPC(session, contextID, rpcResult{Error: err.Error()})
		return
	}
	n.rememberSourceNode(session, envelope.SourceNode)
	switch envelope.Operation {
	case opRegister:
		if !n.IsMainNode() {
			err = fmt.Errorf("node %d is not the main node", n.ID)
		} else if envelope.Location == nil {
			err = fmt.Errorf("register request has no location")
		} else if envelope.Location.NodeID != envelope.SourceNode {
			err = fmt.Errorf("register source node mismatch")
		} else {
			err = n.Registry.Register(*envelope.Location)
		}
		n.respondRPCError(session, contextID, err)
	case opUnregister:
		if !n.IsMainNode() {
			err = fmt.Errorf("node %d is not the main node", n.ID)
		} else {
			err = n.Registry.Unregister(envelope.Target, envelope.SourceNode)
		}
		n.respondRPCError(session, contextID, err)
	case opLookup:
		if !n.IsMainNode() {
			err = fmt.Errorf("node %d is not the main node", n.ID)
			n.respondRPCError(session, contextID, err)
			return
		}
		location, found := n.Registry.Lookup(envelope.Target)
		if !found {
			n.respondRPCError(session, contextID, fmt.Errorf("%w: %s", ErrServiceNotFound, envelope.Target))
			return
		}
		n.respondRPC(session, contextID, rpcResult{Location: &location})
	case opDeliver:
		var message Message
		if err := n.Codec.Decode(envelope.Payload, &message); err != nil {
			n.respondRPCError(session, contextID, err)
			return
		}
		err = n.dispatchLocal(context.Background(), envelope.Source, envelope.Target, &message, true, func(response *Message, responseErr error) error {
			if responseErr != nil {
				n.respondRPCError(session, contextID, responseErr)
				return nil
			}
			payload, encodeErr := n.Codec.Encode(response)
			if encodeErr != nil {
				n.respondRPCError(session, contextID, encodeErr)
				return encodeErr
			}
			n.respondRPC(session, contextID, rpcResult{Payload: payload})
			return nil
		})
		if err != nil {
			n.respondRPCError(session, contextID, err)
		}
	default:
		n.respondRPCError(session, contextID, fmt.Errorf("unsupported rpc operation %d", envelope.Operation))
	}
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
	n.respondRPC(session, contextID, result)
}

func (n *Node) respondRPC(session xtnetNet.ISession, contextID int32, result rpcResult) {
	n.serverRPC.Respond(session, contextID, writePacket(encodeResult(result)))
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
	clients := make([]*RPCClient, 0, len(n.RPCClients))
	for _, client := range n.RPCClients {
		clients = append(clients, client)
	}
	n.RPCClients = make(map[int]*RPCClient)
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
	if n.RPCServer != nil {
		n.RPCServer.Close()
		n.RPCServer = nil
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
