package xtframework

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	xtnet "xtnet"
	xtlog "xtnet/log"
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
	logDir := t.TempDir()
	nativeLogger := xtlog.NewLogger(logDir, xtlog.FileSizeMin, false, false)
	nativeLogger.SetLogLevel(xtlog.LevelNone)
	previousLogger := xtnet.GetLogger()
	defer func() {
		xtnet.SetLogger(previousLogger)
		nativeLogger.Close()
	}()

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

	node, err := NewNode(cfg, 1, WithFactoryRegistry(factories), WithLogger(nativeLogger))
	if err != nil {
		t.Fatal(err)
	}
	service := collector.get(1, "room", 7)

	nodeContext, ok := node.Logger().(*contextLogger)
	if !ok || nodeContext.base != nativeLogger {
		t.Fatal("Node logger does not wrap the injected xtnet logger")
	}
	serviceContext, ok := service.Logger().(*contextLogger)
	if !ok || serviceContext.base != nativeLogger {
		t.Fatal("Service logger does not share the injected xtnet logger")
	}
	if nodeContext.prefix != "[node=1] " {
		t.Fatalf("Node logger prefix = %q", nodeContext.prefix)
	}
	if serviceContext.prefix != "[node=1 service=room service_id=7] " {
		t.Fatalf("Service logger prefix = %q", serviceContext.prefix)
	}
	if err := node.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestNewNodeCreatesAndOwnsConfiguredLogger(t *testing.T) {
	logDir := t.TempDir()
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{{
			ID:         1,
			ListenAddr: freeAddress(t),
			Logger: &LoggerConfig{
				Dir:      logDir,
				Level:    "debug",
				FileSize: xtlog.FileSizeMin,
				Async:    true,
			},
		}},
	}
	previousLogger := xtnet.GetLogger()
	defer xtnet.SetLogger(previousLogger)
	node, err := NewNode(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	if node.ownedLogger == nil || xtnet.GetLogger() != node.ownedLogger {
		t.Fatal("configured logger is not owned by the Node or installed in xtnet")
	}
	if err := node.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestNewNodeRequiresLoggerConfigWithoutOption(t *testing.T) {
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{{
			ID:         1,
			ListenAddr: freeAddress(t),
		}},
	}
	_, err := NewNode(cfg, 1)
	if err == nil || !strings.Contains(err.Error(), "logger config is required") {
		t.Fatalf("NewNode() error = %v", err)
	}
}

func TestNewNodeRestoresLoggerWhenConstructionFails(t *testing.T) {
	cfg := &Config{
		MainNode: 1,
		Nodes: []NodeConfig{{
			ID:         1,
			ListenAddr: freeAddress(t),
			Logger: &LoggerConfig{
				Dir:      t.TempDir(),
				Level:    "debug",
				FileSize: xtlog.FileSizeMin,
				Async:    true,
			},
			Services: []ServiceConfig{{Name: "missing", ID: 1}},
		}},
	}
	previousLogger := xtnet.GetLogger()
	_, err := NewNode(cfg, 1, WithFactoryRegistry(NewFactoryRegistry()))
	if err == nil || !strings.Contains(err.Error(), ErrFactoryNotFound.Error()) {
		t.Fatalf("NewNode() error = %v", err)
	}
	if xtnet.GetLogger() != previousLogger {
		t.Fatal("NewNode did not restore the previous xtnet logger after construction failure")
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
