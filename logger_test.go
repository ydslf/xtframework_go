package xtframework

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

type recordingLogger struct {
	mu      sync.Mutex
	entries []string
}

func (l *recordingLogger) LogDebug(format string, v ...interface{}) {
	l.record("debug", format, v...)
}

func (l *recordingLogger) LogWarn(format string, v ...interface{}) {
	l.record("warn", format, v...)
}

func (l *recordingLogger) LogError(format string, v ...interface{}) {
	l.record("error", format, v...)
}

func (l *recordingLogger) record(level, format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, level+": "+fmt.Sprintf(format, v...))
}

func (l *recordingLogger) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.entries...)
}

func TestNodeAndServiceShareLoggerWithContext(t *testing.T) {
	recorder := &recordingLogger{}
	collector := &serviceCollector{services: make(map[string]*frameworkTestService)}
	factories := NewFactoryRegistry()
	if err := factories.Register("room", collector.factory); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{{
			ID:         1,
			ListenAddr: freeAddress(t),
			Services:   []ServiceConfig{{Name: "room", ID: 7}},
		}},
	}

	node, err := NewNode(cfg, 1, WithFactoryRegistry(factories), WithLogger(recorder))
	if err != nil {
		t.Fatal(err)
	}
	service := collector.get(1, "room", 7)

	node.Logger().LogWarn("node message")
	service.Logger().LogDebug("service message %d", 42)

	entries := recorder.snapshot()
	if len(entries) != 2 {
		t.Fatalf("log entries = %v, want 2 entries", entries)
	}
	if !strings.Contains(entries[0], "[node=1] node message") {
		t.Fatalf("node log = %q", entries[0])
	}
	if !strings.Contains(entries[1], "[node=1 service=room service_id=7] service message 42") {
		t.Fatalf("service log = %q", entries[1])
	}
}

func TestWithLogFieldsDoesNotMutateParentLogger(t *testing.T) {
	recorder := &recordingLogger{}
	parent := WithLogFields(recorder, LogField{Key: "node", Value: 1})
	child := WithLogFields(parent, LogField{Key: "service", Value: "room"})

	parent.LogDebug("parent")
	child.LogDebug("child")

	entries := recorder.snapshot()
	if !strings.Contains(entries[0], "[node=1] parent") {
		t.Fatalf("parent log = %q", entries[0])
	}
	if !strings.Contains(entries[1], "[node=1 service=room] child") {
		t.Fatalf("child log = %q", entries[1])
	}
}
