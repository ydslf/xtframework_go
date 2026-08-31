package xtframework

import (
	"fmt"
	"sync"
	"sync/atomic"
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
	HandleMessage(*MessageContext, *Message) error
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
}

func NewBaseService(node *Node, config ServiceConfig) BaseService {
	return BaseService{node: node, config: config, loop: frame.NewLoop(frame.LoopSizeMin, true)}
}

func (s *BaseService) Name() string          { return s.config.Name }
func (s *BaseService) ID() int               { return s.config.ID }
func (s *BaseService) Loop() *frame.Loop     { return s.loop }
func (s *BaseService) Config() ServiceConfig { return s.config }
func (s *BaseService) Start() error          { return nil }
func (s *BaseService) Stop() error           { return nil }
func (s *BaseService) HandleMessage(*MessageContext, *Message) error {
	return fmt.Errorf("service %s:%d does not handle messages", s.Name(), s.ID())
}

func (s *BaseService) Send2Service(serviceName string, serviceID int, msg *Message) error {
	if s.node == nil {
		return ErrNodeStopped
	}
	return s.node.send2Service(ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, msg)
}

func (s *BaseService) CallService(expireMS time.Duration, serviceName string, serviceID int, req *Message) (*Message, error) {
	if s.node == nil {
		return nil, ErrNodeStopped
	}
	return s.node.callService(expireMS, ServiceKey{Name: s.Name(), ID: s.ID()}, ServiceKey{Name: serviceName, ID: serviceID}, req)
}

type MessageContext struct {
	source    ServiceKey
	target    ServiceKey
	request   bool
	responded atomic.Bool
	respond   func(*Message, error) error
}

func (c *MessageContext) Source() ServiceKey { return c.source }
func (c *MessageContext) Target() ServiceKey { return c.target }
func (c *MessageContext) IsRequest() bool    { return c.request }

func (c *MessageContext) Respond(msg *Message) error {
	return c.respondWithError(msg, nil)
}

func (c *MessageContext) respondWithError(msg *Message, responseErr error) error {
	if !c.request || c.respond == nil {
		return ErrNotRequest
	}
	if responseErr == nil && (msg == nil || msg.ID == 0 || msg.Payload == nil) {
		return ErrInvalidMessage
	}
	if !c.responded.CompareAndSwap(false, true) {
		return ErrAlreadyResponded
	}
	return c.respond(msg, responseErr)
}
