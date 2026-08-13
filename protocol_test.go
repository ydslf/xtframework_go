package xtframework

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

type codecTestMessage struct {
	Text  string
	Count int32
}

func TestXTNetCodecRoundTrip(t *testing.T) {
	registry := NewMessageRegistry()
	if err := registry.Register(1, func() any { return &codecTestMessage{} }); err != nil {
		t.Fatal(err)
	}
	codec := NewXTNetCodec(registry)
	encoded, err := codec.Encode(&Message{ID: 1, Payload: &codecTestMessage{Text: "hello", Count: 7}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded Message
	if err := codec.Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	payload := decoded.Payload.(*codecTestMessage)
	if payload.Text != "hello" || payload.Count != 7 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestProtoCodecRoundTrip(t *testing.T) {
	registry := NewMessageRegistry()
	if err := registry.Register(2, func() any { return &wrapperspb.StringValue{} }); err != nil {
		t.Fatal(err)
	}
	codec := NewProtoCodec(registry)
	encoded, err := codec.Encode(&Message{ID: 2, Payload: wrapperspb.String("hello")})
	if err != nil {
		t.Fatal(err)
	}
	var decoded Message
	if err := codec.Decode(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := decoded.Payload.(*wrapperspb.StringValue).Value; got != "hello" {
		t.Fatalf("decoded value = %q", got)
	}
}

func TestCodecUnknownMessage(t *testing.T) {
	codec := NewXTNetCodec(NewMessageRegistry())
	var decoded Message
	err := codec.Decode(joinMessage(99, nil), &decoded)
	if !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("Decode() error = %v", err)
	}
}
