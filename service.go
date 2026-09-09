package xtframework

import (
	"fmt"
	"sync"
	"time"

	"xtnet/frame"
)

type Factory func(node *Node, config ServiceConfig) (Service, error)

type Service interface {
	Name() string
	ID() int
	Loop() *frame.Loop
	Start() error
	Stop() error
	HandleRPCDirect(*MessageContext, uint32, []byte) error
	HandleRPCRequest(*MessageContext, uint32, []byte) ([]byte, error)
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

func (s *BaseService) Name() string          { return s.config.Name }
func (s *BaseService) ID() int               { return s.config.ID }
func (s *BaseService) Loop() *frame.Loop     { return s.loop }
func (s *BaseService) Config() ServiceConfig { return s.config }
func (s *BaseService) Logger() Logger        { return s.logger }
func (s *BaseService) Start() error          { return nil }
func (s *BaseService) Stop() error           { return nil }
func (s *BaseService) HandleRPCDirect(*MessageContext, uint32, []byte) error {
	return fmt.Errorf("service %s:%d does not handle messages", s.Name(), s.ID())
}
func (s *BaseService) HandleRPCRequest(*MessageContext, uint32, []byte) ([]byte, error) {
	return nil, fmt.Errorf("service %s:%d does not handle requests", s.Name(), s.ID())
}

// Send2Service 异步发送一条业务消息。调用后，调用者不得再修改或复用 payload。
func (s *BaseService) Send2Service(serviceName string, serviceID int, messageID uint32, payload []byte) error {
	if s.node == nil {
		return ErrNodeStopped
	}
	return s.node.send2Service(ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, messageID, payload)
}

func (s *BaseService) CallService(expireMS time.Duration, serviceName string, serviceID int, messageID uint32, payload []byte) ([]byte, error) {
	if s.node == nil {
		return nil, ErrNodeStopped
	}
	return s.node.callService(expireMS, ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, messageID, payload)
}

type MessageContext struct {
	source ServiceKey
	target ServiceKey
}

func (c *MessageContext) Source() ServiceKey { return c.source }
func (c *MessageContext) Target() ServiceKey { return c.target }
