/*
Package dlog_lion provides lion functionality for dlog.

https://go.pedge.io/lion
*/
package dlog_lion // import "go.pedge.io/dlog/lion"

import (
	"go.pedge.io/dlog"
	"go.pedge.io/lion"
)

// Register registers the default lion Logger as the dlog Logger.
func Register() {
	lion.AddGlobalHook(
		func(lionLogger lion.Logger) {
			dlog.SetLogger(NewLogger(lionLogger))
		},
	)
}

// NewLogger returns a new dlog.Logger for the given lion.Logger.
func NewLogger(lionLogger lion.Logger) dlog.Logger {
	return newLogger(lionLogger)
}

type logger struct {
	dlog.BaseLogger
	l lion.Logger
}

func newLogger(l lion.Logger) *logger {
	return &logger{l, l}
}

func (l *logger) AtLevel(level dlog.Level) dlog.Logger {
	// TODO(pedge): don't be lazy, make an actual map between dlog.Level and lion.Level
	// you just copy/pasted lion_level.go to dlog_level.go
	return newLogger(l.l.AtLevel(lion.Level(level)))
}

func (l *logger) WithField(key string, value interface{}) dlog.Logger {
	return newLogger(l.l.WithField(key, value))
}

func (l *logger) WithFields(fields map[string]interface{}) dlog.Logger {
	return newLogger(l.l.WithFields(fields))
}
