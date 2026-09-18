package xtframework

import (
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"unicode/utf8"

	"xtframework/internal/rpcpb"
	xtnetNet "xtnet/net"
)

const maxSubscribedServiceNameSize = 256

// ServiceSubscriptionHandler 由需要接收 Service 发现事件的业务 Service 实现。
// 所有回调都会在订阅者自己的 Service Loop 中按接收顺序执行。
type ServiceSubscriptionHandler interface {
	HandleServiceSnapshot(serviceName string, services []ServiceKey)
	HandleServiceOnline(service ServiceKey)
	HandleServiceOffline(service ServiceKey)
}

// serviceSubscriptionIndex 只维护订阅关系，不负责注册表查询或消息发送。
// 它由主节点的 control Loop 独占访问，因此内部不需要额外加锁。
type serviceSubscriptionIndex struct {
	byServiceName map[string]map[ServiceKey]struct{}
	bySubscriber  map[ServiceKey]map[string]struct{}
}

func newServiceSubscriptionIndex() *serviceSubscriptionIndex {
	return &serviceSubscriptionIndex{
		byServiceName: make(map[string]map[ServiceKey]struct{}),
		bySubscriber:  make(map[ServiceKey]map[string]struct{}),
	}
}

// subscribe 返回是否新建了订阅关系。重复订阅保持幂等。
func (s *serviceSubscriptionIndex) subscribe(subscriber ServiceKey, serviceName string) bool {
	subscribers := s.byServiceName[serviceName]
	if subscribers == nil {
		subscribers = make(map[ServiceKey]struct{})
		s.byServiceName[serviceName] = subscribers
	}
	if _, exists := subscribers[subscriber]; exists {
		return false
	}
	subscribers[subscriber] = struct{}{}

	serviceNames := s.bySubscriber[subscriber]
	if serviceNames == nil {
		serviceNames = make(map[string]struct{})
		s.bySubscriber[subscriber] = serviceNames
	}
	serviceNames[serviceName] = struct{}{}
	return true
}

func (s *serviceSubscriptionIndex) removeSubscriber(subscriber ServiceKey) {
	serviceNames := s.bySubscriber[subscriber]
	for serviceName := range serviceNames {
		subscribers := s.byServiceName[serviceName]
		delete(subscribers, subscriber)
		if len(subscribers) == 0 {
			delete(s.byServiceName, serviceName)
		}
	}
	delete(s.bySubscriber, subscriber)
}

func (s *serviceSubscriptionIndex) subscribers(serviceName string) []ServiceKey {
	indexed := s.byServiceName[serviceName]
	result := make([]ServiceKey, 0, len(indexed))
	for subscriber := range indexed {
		result = append(result, subscriber)
	}
	sortServiceKeys(result)
	return result
}

func sortServiceKeys(keys []ServiceKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Name == keys[j].Name {
			return keys[i].ID < keys[j].ID
		}
		return keys[i].Name < keys[j].Name
	})
}

func normalizeSubscribedServiceName(serviceName string) (string, error) {
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return "", fmt.Errorf("subscribed service name is empty")
	}
	if len(serviceName) > maxSubscribedServiceNameSize {
		return "", fmt.Errorf("subscribed service name is too long: %d bytes", len(serviceName))
	}
	if !utf8.ValidString(serviceName) {
		return "", fmt.Errorf("subscribed service name is not valid UTF-8")
	}
	return serviceName, nil
}

func (n *Node) subscribeService(subscriber ServiceKey, serviceName string) error {
	if !n.operational() {
		return ErrNodeStopped
	}
	serviceName, err := normalizeSubscribedServiceName(serviceName)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	runtime, exists := n.localServices[subscriber]
	if !exists {
		return fmt.Errorf("%w: subscriber %s", ErrServiceNotFound, subscriber)
	}

	runtime.subscriptionsMu.Lock()
	runtime.subscriptions[serviceName] = struct{}{}
	registered := runtime.registered.Load()
	runtime.subscriptionsMu.Unlock()
	if !registered {
		return nil
	}

	return n.requestServiceSubscription(subscriber, serviceName)
}

func (n *Node) subscribeRuntime(subscriber ServiceKey, runtime *serviceRuntime) error {
	runtime.subscriptionsMu.Lock()
	serviceNames := make([]string, 0, len(runtime.subscriptions))
	for serviceName := range runtime.subscriptions {
		serviceNames = append(serviceNames, serviceName)
	}
	runtime.subscriptionsMu.Unlock()
	sort.Strings(serviceNames)
	for _, serviceName := range serviceNames {
		if err := n.requestServiceSubscription(subscriber, serviceName); err != nil {
			return fmt.Errorf("%s: %w", serviceName, err)
		}
	}
	return nil
}

func (n *Node) requestServiceSubscription(subscriber ServiceKey, serviceName string) error {
	if n.IsMainNode() {
		return callOnLoop(n.controlLoop, func() error {
			if err := n.addServiceSubscription(subscriber, n.id, serviceName); err != nil {
				return err
			}
			services := n.serviceSnapshot(serviceName)
			n.sendServiceDiscovery(subscriber, serviceName, rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_SNAPSHOT, services)
			return nil
		})
	}
	client, err := n.mainClient()
	if err != nil {
		return err
	}
	return client.requestSync(n.connectTimeout, opSubscribe, &rpcpb.SubscribeRequest{
		Subscriber:  serviceKeyToProto(subscriber),
		ServiceName: serviceName,
	}, &rpcpb.SubscribeResponse{})
}

func (n *Node) dispatchLocalServiceDiscovery(subscriber ServiceKey, serviceName string, eventType rpcpb.ServiceDiscoveryEventType, services []ServiceKey) error {
	runtime, exists := n.localServices[subscriber]
	if !exists {
		return fmt.Errorf("%w: subscriber %s", ErrServiceNotFound, subscriber)
	}
	handler, exists := runtime.service.(ServiceSubscriptionHandler)
	if !exists {
		return fmt.Errorf("service %s does not implement ServiceSubscriptionHandler", subscriber)
	}
	switch eventType {
	case rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_SNAPSHOT:
	case rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_ONLINE,
		rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_OFFLINE:
		if len(services) != 1 {
			return fmt.Errorf("discovery event %s contains %d services, want 1", eventType, len(services))
		}
	default:
		return fmt.Errorf("unsupported discovery event type %s", eventType)
	}
	runtime.service.Loop().Post(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				n.report(fmt.Errorf("service %s discovery handler panic: %v\n%s", subscriber, recovered, debug.Stack()))
			}
		}()
		switch eventType {
		case rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_SNAPSHOT:
			handler.HandleServiceSnapshot(serviceName, services)
		case rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_ONLINE:
			handler.HandleServiceOnline(services[0])
		case rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_OFFLINE:
			handler.HandleServiceOffline(services[0])
		}
	})
	return nil
}

// addServiceSubscription 必须在主节点的 control Loop 中调用。
func (n *Node) addServiceSubscription(subscriber ServiceKey, sourceNode int, serviceName string) error {
	location, exists := n.registry.Lookup(subscriber)
	if !exists {
		return fmt.Errorf("%w: subscriber %s", ErrServiceNotFound, subscriber)
	}
	if location.NodeID != sourceNode {
		return fmt.Errorf("subscriber %s belongs to node %d, not node %d", subscriber, location.NodeID, sourceNode)
	}
	n.subscriptions.subscribe(subscriber, serviceName)
	return nil
}

// serviceSnapshot 必须在主节点的 control Loop 中调用。
func (n *Node) serviceSnapshot(serviceName string) []ServiceKey {
	locations := n.registry.List()
	services := make([]ServiceKey, 0)
	for _, location := range locations {
		if location.ServiceName == serviceName {
			services = append(services, location.Key())
		}
	}
	sortServiceKeys(services)
	return services
}

// serviceRegistered 必须在主节点的 control Loop 中调用。
func (n *Node) serviceRegistered(service ServiceKey) {
	n.broadcastRouteInvalidation(service)
	for _, subscriber := range n.subscriptions.subscribers(service.Name) {
		n.sendServiceDiscovery(subscriber, service.Name, rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_ONLINE, []ServiceKey{service})
	}
}

// serviceUnregistered 必须在主节点的 control Loop 中调用。
func (n *Node) serviceUnregistered(service ServiceKey) {
	n.servicesUnregistered([]ServiceKey{service})
}

// servicesUnregistered 必须在主节点的 control Loop 中调用。
func (n *Node) servicesUnregistered(services []ServiceKey) {
	// 下线的 Service 不再作为订阅者接收后续事件；它作为被订阅对象的
	// 订阅关系仍然保留，以便同名的其他实例继续产生通知。
	for _, service := range services {
		n.subscriptions.removeSubscriber(service)
	}
	n.broadcastRouteInvalidation(services...)
	for _, service := range services {
		for _, subscriber := range n.subscriptions.subscribers(service.Name) {
			n.sendServiceDiscovery(subscriber, service.Name, rpcpb.ServiceDiscoveryEventType_SERVICE_DISCOVERY_EVENT_TYPE_OFFLINE, []ServiceKey{service})
		}
	}
}

// sendServiceDiscovery 必须在主节点的 control Loop 中调用。
func (n *Node) sendServiceDiscovery(subscriber ServiceKey, serviceName string, eventType rpcpb.ServiceDiscoveryEventType, services []ServiceKey) {
	location, exists := n.registry.Lookup(subscriber)
	if !exists {
		return
	}
	if location.NodeID == n.id {
		if err := n.dispatchLocalServiceDiscovery(subscriber, serviceName, eventType, services); err != nil {
			n.report(err)
		}
		return
	}

	n.remoteNodesMu.RLock()
	var session xtnetNet.ISession
	for candidate, remote := range n.remoteNodes {
		if remote.id == location.NodeID {
			session = candidate
			break
		}
	}
	n.remoteNodesMu.RUnlock()
	if session == nil {
		return
	}

	message := &rpcpb.ServiceDiscovery{
		Subscriber:  serviceKeyToProto(subscriber),
		ServiceName: serviceName,
		EventType:   eventType,
		Services:    make([]*rpcpb.ServiceKey, 0, len(services)),
	}
	for _, service := range services {
		message.Services = append(message.Services, serviceKeyToProto(service))
	}
	wpk, err := encodeEnvelope(opServiceDiscovery, message)
	if err != nil {
		n.report(err)
		return
	}
	n.serverRPC.SendDirect(session, wpk)
}
