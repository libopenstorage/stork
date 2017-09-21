/*
Package dlog_zap provides zap functionality for dlog.

https://go.uber.org/zap
*/
package dlog_zap

import (
	"errors"

	"go.pedge.io/dlog"
	"go.uber.org/zap"
)

var (
	// ErrCannotSetZapLevel is the error which is used to panic if AtLevel is called.
	ErrCannotSetZapLevel = errors.New("cannot set zap level")
)

// Register registers the default zap Logger as the dlog Logger.
func Register() {
	dlog.SetLogger(NewLogger(zap.New(nil).Sugar()))
}

// NewLogger returns a new dlog.Logger for the given zap.SugaredLogger.
func NewLogger(zapSugaredLogger *zap.SugaredLogger) dlog.Logger {
	return newLogger(zapSugaredLogger)
}

type logger struct {
	*zap.SugaredLogger
}

func newLogger(l *zap.SugaredLogger) *logger {
	return &logger{l}
}

func (l *logger) AtLevel(level dlog.Level) dlog.Logger {
	// TODO(pedge): something better
	panic(ErrCannotSetZapLevel.Error())
}

func (l *logger) WithField(key string, value interface{}) dlog.Logger {
	return newLogger(l.SugaredLogger.With(key, value))
}

func (l *logger) WithFields(fields map[string]interface{}) dlog.Logger {
	args := make([]interface{}, len(fields)*2)
	i := 0
	for key, value := range fields {
		args[i] = key
		i++
		args[i] = value
		i++
	}
	return newLogger(l.SugaredLogger.With(args...))
}

func (l *logger) Debugln(args ...interface{}) {
	l.SugaredLogger.Debug(args...)
}

func (l *logger) Infoln(args ...interface{}) {
	l.SugaredLogger.Info(args...)
}

func (l *logger) Warnln(args ...interface{}) {
	l.SugaredLogger.Warn(args...)
}

func (l *logger) Errorln(args ...interface{}) {
	l.SugaredLogger.Error(args...)
}

func (l *logger) Fatalln(args ...interface{}) {
	l.SugaredLogger.Fatal(args...)
}

func (l *logger) Panicln(args ...interface{}) {
	l.SugaredLogger.Panic(args...)
}

func (l *logger) Printf(format string, args ...interface{}) {
	l.SugaredLogger.Infof(format, args...)
}

func (l *logger) Println(args ...interface{}) {
	l.SugaredLogger.Info(args...)
}
