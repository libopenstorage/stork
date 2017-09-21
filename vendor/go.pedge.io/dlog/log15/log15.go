/*
Package dlog_log15 provides log15 functionality for dlog.

https://github.com/inconshreveable/log15
*/
package dlog_log15 // import "go.pedge.io/dlog/log15"

import (
	"fmt"
	"os"

	"go.pedge.io/dlog"
	"gopkg.in/inconshreveable/log15.v2"
)

var (
	levelToLog15Level = map[dlog.Level]log15.Lvl{
		dlog.LevelNone:  log15.LvlInfo,
		dlog.LevelDebug: log15.LvlDebug,
		dlog.LevelInfo:  log15.LvlInfo,
		dlog.LevelWarn:  log15.LvlWarn,
		dlog.LevelError: log15.LvlError,
		dlog.LevelFatal: log15.LvlCrit,
		dlog.LevelPanic: log15.LvlCrit,
	}
)

// Register registers the default log15 Logger as the dlog Logger.
func Register() {
	dlog.SetLogger(NewLogger(log15.Root()))
}

// NewLogger returns a new dlog.Logger that uses the log15.Logger.
func NewLogger(log15Logger log15.Logger) dlog.Logger {
	return newLogger(log15Logger)
}

type logger struct {
	l log15.Logger
}

func newLogger(l log15.Logger) *logger {
	return &logger{l}
}

func (l *logger) AtLevel(level dlog.Level) dlog.Logger {
	// TODO(pedge): not thread safe, tradeoff here
	// TODO(pedge): overall hacky, overwriting other handlers
	// TODO(pedge): does not check map, even though we expect coverage
	l.l.SetHandler(
		log15.LvlFilterHandler(
			levelToLog15Level[level],
			log15.StdoutHandler,
		),
	)
	return l
}

func (l *logger) WithField(key string, value interface{}) dlog.Logger {
	return newLogger(l.l.New(key, value))
}

func (l *logger) WithFields(fields map[string]interface{}) dlog.Logger {
	fieldsSlice := make([]interface{}, len(fields)*2)
	i := 0
	for key, value := range fields {
		fieldsSlice[i] = key
		fieldsSlice[i+1] = value
		i += 2
	}
	return newLogger(l.l.New(fieldsSlice...))
}

func (l *logger) Debugf(format string, args ...interface{}) {
	l.l.Debug(fmt.Sprintf(format, args...))
}

func (l *logger) Debugln(args ...interface{}) {
	l.l.Debug(fmt.Sprint(args...))
}

func (l *logger) Infof(format string, args ...interface{}) {
	l.l.Info(fmt.Sprintf(format, args...))
}

func (l *logger) Infoln(args ...interface{}) {
	l.l.Info(fmt.Sprint(args...))
}

func (l *logger) Warnf(format string, args ...interface{}) {
	l.l.Warn(fmt.Sprintf(format, args...))
}

func (l *logger) Warnln(args ...interface{}) {
	l.l.Warn(fmt.Sprint(args...))
}

func (l *logger) Errorf(format string, args ...interface{}) {
	l.l.Error(fmt.Sprintf(format, args...))
}

func (l *logger) Errorln(args ...interface{}) {
	l.l.Error(fmt.Sprint(args...))
}

func (l *logger) Fatalf(format string, args ...interface{}) {
	l.l.Crit(fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (l *logger) Fatalln(args ...interface{}) {
	l.l.Crit(fmt.Sprint(args...))
	os.Exit(1)
}

func (l *logger) Panicf(format string, args ...interface{}) {
	l.l.Crit(fmt.Sprintf(format, args...))
	panic(fmt.Sprintf(format, args...))
}

func (l *logger) Panicln(args ...interface{}) {
	l.l.Crit(fmt.Sprint(args...))
	panic(fmt.Sprint(args...))
}

func (l *logger) Printf(format string, args ...interface{}) {
	l.l.Info(fmt.Sprintf(format, args...))
}

func (l *logger) Println(args ...interface{}) {
	l.l.Info(fmt.Sprint(args...))
}
