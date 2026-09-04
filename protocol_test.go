package xtframework

import (
	"bytes"
	"errors"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	wantPayload := []byte("application encoded payload")
	encoded, err := encodeMessage(42, wantPayload)
	if err != nil {
		t.Fatal(err)
	}
	id, payload, err := decodeMessage(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 || !bytes.Equal(payload, wantPayload) {
		t.Fatalf("decoded message = (%d, %q), want (42, %q)", id, payload, wantPayload)
	}
}

func TestMessageAllowsEmptyPayload(t *testing.T) {
	encoded, err := encodeMessage(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	id, payload, err := decodeMessage(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 || len(payload) != 0 {
		t.Fatalf("decoded message = (%d, %q), want (1, empty)", id, payload)
	}
}

func TestMessageRejectsInvalidID(t *testing.T) {
	if _, err := encodeMessage(0, nil); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("encodeMessage() error = %v", err)
	}
	if _, _, err := decodeMessage([]byte{0, 0, 0, 0}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("decodeMessage() error = %v", err)
	}
}
