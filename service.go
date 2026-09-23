package xtframework

import (
	"fmt"
	"sync"
	"unicode/utf8"

	"xtnet/frame"
)

type Factory func(node *Node, config ServiceConfig) (Service, error)

// ServiceCallCallback 用于接收异步 Service 调用的结果。
type ServiceCallCallback func(payload []byte, err error)

type Service interface {
	Name() string
	ID() int
	Loop() *frame.Loop
	Start() error
	Stop() error
	HandleRPCDirect(MessageContext, uint32, []byte) error
	HandleRPCRequest(MessageContext, uint32, []byte) ([]byte, error)
	HandleRPCDirectString(MessageContext, string, []byte) error
	HandleRPCRequestString(MessageContext, string, []byte) ([]byte, error)
}

type serviceMessageIDKind uint8

const (
	serviceMessageIDNumeric serviceMessageIDKind = iota + 1
	serviceMessageIDString
	maxStringMessageIDSize = 256
)

type serviceMessageID struct {
	kind   serviceMessageIDKind
	number uint32
	text   string
}

func numericServiceMessageID(value uint32) serviceMessageID {
	return serviceMessageID{kind: serviceMessageIDNumeric, number: value}
}

func stringServiceMessageID(value string) serviceMessageID {
	return serviceMessageID{kind: serviceMessageIDString, text: value}
}

func (id serviceMessageID) validate() error {
	switch id.kind {
	case serviceMessageIDNumeric:
		if id.number == 0 {
			return fmt.Errorf("%w: numeric message id is zero", ErrInvalidMessage)
		}
	case serviceMessageIDString:
		if id.text == "" {
			return fmt.Errorf("%w: string message id is empty", ErrInvalidMessage)
		}
		if len(id.text) > maxStringMessageIDSize {
			return fmt.Errorf("%w: string message id is too long: %d bytes", ErrInvalidMessage, len(id.text))
		}
		if !utf8.ValidString(id.text) {
			return fmt.Errorf("%w: string message id is not valid UTF-8", ErrInvalidMessage)
		}
	default:
		return fmt.Errorf("%w: message id kind is invalid", ErrInvalidMessage)
	}
	return nil
}

func (id serviceMessageID) String() string {
	if id.kind == serviceMessageIDString {
		return fmt.Sprintf("%q", id.text)
	}
	return fmt.Sprintf("%d", id.number)
}

type FactoryRegistry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

func NewFactoryRegistry() *FactoryRegistry {
	return &FactoryRegistry{factories: make(map[string]Factory)}
}

func (r *FactoryRegistry) Register(name string, factory Factory) error {
	if name == "" || factory == nil {
		return fmt.Errorf("service name and factory are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.factories[name]; exists {
		return fmt.Errorf("service factory %q already registered", name)
	}
	r.factories[name] = factory
	return nil
}

func (r *FactoryRegistry) Get(name string) (Factory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, exists := r.factories[name]
	return factory, exists
}

var defaultFactories = NewFactoryRegistry()

func RegisterServiceFactory(name string, factory Factory) error {
	return defaultFactories.Register(name, factory)
}

type BaseService struct {
	node   *Node
	config ServiceConfig
	loop   *frame.Loop
	logger Logger
}

func NewBaseService(node *Node, config ServiceConfig) BaseService {
	var logger Logger
	if node != nil {
		logger = WithLogFields(node.Logger(),
			LogField{Key: "service", Value: config.Name},
			LogField{Key: "service_id", Value: config.ID},
		)
	}
	return BaseService{node: node, config: config, loop: frame.NewLoop(frame.LoopSizeMin, true), logger: logger}
}

func (s *BaseService) Name() string                               { return s.config.Name }
func (s *BaseService) ID() int                                    { return s.config.ID }
func (s *BaseService) Loop() *frame.Loop                          { return s.loop }
func (s *BaseService) Config() ServiceConfig                      { return s.config }
func (s *BaseService) Logger() Logger                             { return s.logger }
func (s *BaseService) Start() error                               { return nil }
func (s *BaseService) Stop() error                                { return nil }
func (s *BaseService) HandleServiceSnapshot(string, []ServiceKey) {}
func (s *BaseService) HandleServiceOnline(ServiceKey)             {}
func (s *BaseService) HandleServiceOffline(ServiceKey)            {}
func (s *BaseService) HandleRPCDirect(MessageContext, uint32, []byte) error {
	return fmt.Errorf("service %s:%d does not handle messages", s.Name(), s.ID())
}
func (s *BaseService) HandleRPCRequest(MessageContext, uint32, []byte) ([]byte, error) {
	return nil, fmt.Errorf("service %s:%d does not handle requests", s.Name(), s.ID())
}
func (s *BaseService) HandleRPCDirectString(MessageContext, string, []byte) error {
	return fmt.Errorf("service %s:%d does not handle string messages", s.Name(), s.ID())
}
func (s *BaseService) HandleRPCRequestString(MessageContext, string, []byte) ([]byte, error) {
	return nil, fmt.Errorf("service %s:%d does not handle string requests", s.Name(), s.ID())
}

// Subscribe 订阅指定名字的全部 Service 实例。可以在 Start 中调用；此时
// Node 会在当前 Service 注册成功后向主节点提交订阅。重复订阅会重新获取快照。
func (s *BaseService) Subscribe(serviceName string) error {
	if s.node == nil {
		return ErrNodeStopped
	}
	return s.node.subscribeService(ServiceKey{Name: s.Name(), ID: s.ID()}, serviceName)
}

// Send2Service 异步发送一条业务消息。调用后，调用者不得再修改或复用 payload。
func (s *BaseService) Send2Service(serviceName string, serviceID int, messageID uint32, payload []byte) error {
	if s.node == nil {
		return ErrNodeStopped
	}
	return s.node.send2Service(ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, numericServiceMessageID(messageID), payload)
}

// Send2ServiceString 异步发送一条使用字符串消息 ID 的业务消息。
// 调用后，调用者不得再修改或复用 payload。
func (s *BaseService) Send2ServiceString(serviceName string, serviceID int, messageID string, payload []byte) error {
	if s.node == nil {
		return ErrNodeStopped
	}
	return s.node.send2Service(ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, stringServiceMessageID(messageID), payload)
}

// CallService 异步调用另一个 Service。返回 nil 表示请求已被接受；调用完成后，
// callback 会被投递到当前 Service 的 Loop 中执行。返回非 nil 错误时不会调用 callback。
// Node 配置的 Service 调用超时只约束远端 RPC，本地 Service 调用不计算超时。
func (s *BaseService) CallService(serviceName string, serviceID int, messageID uint32, payload []byte, callback ServiceCallCallback) error {
	if callback == nil {
		return fmt.Errorf("service call callback is nil")
	}
	if s.node == nil {
		return ErrNodeStopped
	}
	return s.node.callService(ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, numericServiceMessageID(messageID), payload, func(responsePayload []byte, responseErr error) {
		s.loop.Post(func() {
			callback(responsePayload, responseErr)
		})
	})
}

// CallServiceString 异步调用另一个 Service，并使用字符串消息 ID。
// callback 会被投递到当前 Service 的 Loop 中执行。
func (s *BaseService) CallServiceString(serviceName string, serviceID int, messageID string, payload []byte, callback ServiceCallCallback) error {
	if callback == nil {
		return fmt.Errorf("service call callback is nil")
	}
	if s.node == nil {
		return ErrNodeStopped
	}
	return s.node.callService(ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, stringServiceMessageID(messageID), payload, func(responsePayload []byte, responseErr error) {
		s.loop.Post(func() {
			callback(responsePayload, responseErr)
		})
	})
}

// CallServiceSync 同步调用另一个 Service，并阻塞等待调用结果或超时。
// 需要保持响应的 Service Loop 不应使用此方法。
func (s *BaseService) CallServiceSync(serviceName string, serviceID int, messageID uint32, payload []byte) ([]byte, error) {
	if s.node == nil {
		return nil, ErrNodeStopped
	}
	return s.node.callServiceSync(ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, numericServiceMessageID(messageID), payload)
}

// CallServiceSyncString 同步调用另一个 Service，并使用字符串消息 ID。
// 需要保持响应的 Service Loop 不应使用此方法。
func (s *BaseService) CallServiceSyncString(serviceName string, serviceID int, messageID string, payload []byte) ([]byte, error) {
	if s.node == nil {
		return nil, ErrNodeStopped
	}
	return s.node.callServiceSync(ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, stringServiceMessageID(messageID), payload)
}

type MessageContext struct {
	source ServiceKey
	target ServiceKey
}

func (c MessageContext) Source() ServiceKey { return c.source }
func (c MessageContext) Target() ServiceKey { return c.target }
