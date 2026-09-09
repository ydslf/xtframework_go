package xtframework

import (
	"fmt"
	"strings"

	xtlog "xtnet/log"
)

// Logger is the logging contract used by the framework, Node, and Service.
// *xtnet/log.Logger implements this interface directly.
type Logger interface {
	LogDebug(format string, v ...interface{})
	LogWarn(format string, v ...interface{})
	LogError(format string, v ...interface{})
}

// LogField is a piece of immutable context attached to a Logger.
type LogField struct {
	Key   string
	Value interface{}
}

// WithLogFields returns a Logger that prefixes every message with fields.
// The returned Logger shares the same underlying output with logger.
func WithLogFields(logger Logger, fields ...LogField) Logger {
	if logger == nil || len(fields) == 0 {
		return logger
	}

	base := logger
	allFields := append([]LogField(nil), fields...)
	if contextual, ok := logger.(*contextLogger); ok {
		base = contextual.base
		allFields = append(append([]LogField(nil), contextual.fields...), fields...)
	}
	return &contextLogger{
		base:   base,
		fields: allFields,
		prefix: formatLogFields(allFields),
	}
}

type contextLogger struct {
	base   Logger
	fields []LogField
	prefix string
}

func (l *contextLogger) LogDebug(format string, v ...interface{}) {
	l.base.LogDebug(l.prefix+format, v...)
}

func (l *contextLogger) LogWarn(format string, v ...interface{}) {
	l.base.LogWarn(l.prefix+format, v...)
}

func (l *contextLogger) LogError(format string, v ...interface{}) {
	l.base.LogError(l.prefix+format, v...)
}

func formatLogFields(fields []LogField) string {
	var result strings.Builder
	result.WriteByte('[')
	for i, field := range fields {
		if i > 0 {
			result.WriteByte(' ')
		}
		result.WriteString(field.Key)
		result.WriteByte('=')
		result.WriteString(fmt.Sprint(field.Value))
	}
	result.WriteString("] ")
	return result.String()
}

var _ Logger = (*xtlog.Logger)(nil)

func newXTNetLogger(config LoggerConfig) (*xtlog.Logger, error) {
	level, err := parseLogLevel(config.Level)
	if err != nil {
		return nil, err
	}
	logger := xtlog.NewLogger(config.Dir, config.FileSize, config.Screen, config.Async)
	logger.SetLogLevel(level)
	return logger, nil
}

func parseLogLevel(level string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "none":
		return xtlog.LevelNone, nil
	case "error":
		return xtlog.LevelError, nil
	case "warn":
		return xtlog.LevelWarn, nil
	case "debug":
		return xtlog.LevelDebug, nil
	default:
		return 0, fmt.Errorf("level %q must be one of none, error, warn, debug", level)
	}
}
