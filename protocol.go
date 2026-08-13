package xtframework

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
	xtencoding "xtnet/encoding"
)

// Message is the unit delivered between services. Payload must match the type
// registered for ID in the selected codec's MessageRegistry.
type Message struct {
	ID      uint32
	Payload any
}

type MessageFactory func() any

type MessageRegistry struct {
	mu        sync.RWMutex
	factories map[uint32]MessageFactory
}

func NewMessageRegistry() *MessageRegistry {
	return &MessageRegistry{factories: make(map[uint32]MessageFactory)}
}

func (r *MessageRegistry) Register(id uint32, factory MessageFactory) error {
	if id == 0 || factory == nil {
		return fmt.Errorf("%w: id and factory are required", ErrInvalidMessage)
	}
	value := factory()
	if value == nil || reflect.ValueOf(value).Kind() != reflect.Ptr {
		return fmt.Errorf("%w: message factory %d must return a non-nil pointer", ErrInvalidMessage, id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.factories[id]; exists {
		return fmt.Errorf("%w: id=%d", ErrMessageExists, id)
	}
	r.factories[id] = factory
	return nil
}

func (r *MessageRegistry) New(id uint32) (any, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: registry is nil", ErrMessageNotFound)
	}
	r.mu.RLock()
	factory := r.factories[id]
	r.mu.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("%w: id=%d", ErrMessageNotFound, id)
	}
	return factory(), nil
}

type Codec interface {
	Name() string
	Encode(*Message) ([]byte, error)
	Decode([]byte, *Message) error
}

type XTNetCodec struct{ registry *MessageRegistry }

func NewXTNetCodec(registry *MessageRegistry) *XTNetCodec {
	return &XTNetCodec{registry: registry}
}

func (*XTNetCodec) Name() string { return "xtnet" }

func (c *XTNetCodec) Encode(msg *Message) ([]byte, error) {
	if msg == nil || msg.ID == 0 || msg.Payload == nil {
		return nil, ErrInvalidMessage
	}
	body, err := xtencoding.Encode(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("xtnet encode message %d: %w", msg.ID, err)
	}
	return joinMessage(msg.ID, body), nil
}

func (c *XTNetCodec) Decode(data []byte, msg *Message) error {
	id, body, err := splitMessage(data)
	if err != nil {
		return err
	}
	payload, err := c.registry.New(id)
	if err != nil {
		return err
	}
	if err := xtencoding.Decode(body, payload); err != nil {
		return fmt.Errorf("xtnet decode message %d: %w", id, err)
	}
	msg.ID, msg.Payload = id, payload
	return nil
}

type ProtoCodec struct{ registry *MessageRegistry }

func NewProtoCodec(registry *MessageRegistry) *ProtoCodec {
	return &ProtoCodec{registry: registry}
}

func (*ProtoCodec) Name() string { return "protobuf" }

func (c *ProtoCodec) Encode(msg *Message) ([]byte, error) {
	if msg == nil || msg.ID == 0 || msg.Payload == nil {
		return nil, ErrInvalidMessage
	}
	payload, ok := msg.Payload.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%w: message %d payload does not implement proto.Message", ErrUnsupportedPayload, msg.ID)
	}
	body, err := proto.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("protobuf encode message %d: %w", msg.ID, err)
	}
	return joinMessage(msg.ID, body), nil
}

func (c *ProtoCodec) Decode(data []byte, msg *Message) error {
	id, body, err := splitMessage(data)
	if err != nil {
		return err
	}
	value, err := c.registry.New(id)
	if err != nil {
		return err
	}
	payload, ok := value.(proto.Message)
	if !ok {
		return fmt.Errorf("%w: registered message %d does not implement proto.Message", ErrUnsupportedPayload, id)
	}
	if err := proto.Unmarshal(body, payload); err != nil {
		return fmt.Errorf("protobuf decode message %d: %w", id, err)
	}
	msg.ID, msg.Payload = id, payload
	return nil
}

func joinMessage(id uint32, body []byte) []byte {
	data := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(data, id)
	copy(data[4:], body)
	return data
}

func splitMessage(data []byte) (uint32, []byte, error) {
	if len(data) < 4 {
		return 0, nil, fmt.Errorf("%w: encoded message is shorter than 4 bytes", ErrInvalidMessage)
	}
	id := binary.BigEndian.Uint32(data)
	if id == 0 {
		return 0, nil, fmt.Errorf("%w: message id is zero", ErrInvalidMessage)
	}
	return id, data[4:], nil
}
