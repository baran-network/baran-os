package runtime

import (
	"fmt"
	"log/slog"
)

// natsLogger bridges the nats-server Logger interface to a *slog.Logger.
// All NATS server log output flows through the runtime's structured logger
// with component="nats".
type natsLogger struct {
	log *slog.Logger
}

func newNATSLogger(log *slog.Logger) *natsLogger {
	return &natsLogger{log: log.With("component", "nats")}
}

func (l *natsLogger) Noticef(format string, v ...interface{}) {
	l.log.Info(fmt.Sprintf(format, v...))
}

func (l *natsLogger) Warnf(format string, v ...interface{}) {
	l.log.Warn(fmt.Sprintf(format, v...))
}

func (l *natsLogger) Errorf(format string, v ...interface{}) {
	l.log.Error(fmt.Sprintf(format, v...))
}

func (l *natsLogger) Fatalf(format string, v ...interface{}) {
	l.log.Error(fmt.Sprintf(format, v...))
}

func (l *natsLogger) Debugf(format string, v ...interface{}) {
	l.log.Debug(fmt.Sprintf(format, v...))
}

func (l *natsLogger) Tracef(format string, v ...interface{}) {
	l.log.Debug(fmt.Sprintf(format, v...))
}
