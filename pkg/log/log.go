package log

import (
	"bytes"
	"fmt"
	"github.com/portworx/torpedo/pkg/aetosutil"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

type colorizer func(...interface{}) string

//type Logger struct {
//	LogrusLogger *logrus.Logger
//}

var (
	green    colorizer
	yellow   colorizer
	hiyellow colorizer
	red      colorizer
	white    colorizer
	heading  colorizer
	plain    colorizer
)

var (
	dash  *aetosutil.Dashboard
	lock  = &sync.Mutex{}
	tpLog *logrus.Logger
)

// We are logging to file, strip colors to make the output more readable
var txtFormatter = &logrus.TextFormatter{DisableColors: true}

// Hook to handle writing to local tpLog files.
type Hook struct {
	formatter logrus.Formatter
}

// NewHook returns a torpedo color formatting hook.
func NewHook() *Hook {
	hook := &Hook{
		formatter: txtFormatter,
	}
	return hook
}

// SetFormatter sets the tpLog formatter.
func (hook *Hook) SetFormatter(formatter logrus.Formatter) {
	hook.formatter = formatter

	switch hook.formatter.(type) {
	case *logrus.TextFormatter:
		textFormatter := hook.formatter.(*logrus.TextFormatter)
		textFormatter.DisableColors = true
	}
}

func successMessage(msg string) bool {
	successStrings := []string{
		"pass",
		"validated",
		"successfully",
	}

	for _, s := range successStrings {
		if strings.Contains(strings.ToLower(msg), s) {
			return true
		}
	}

	return false
}

func errorMessage(msg string) bool {
	errorStrings := []string{
		"failed",
		"error",
	}

	for _, s := range errorStrings {
		if strings.Contains(strings.ToLower(msg), s) {
			return true
		}
	}

	return false
}

// Fire color codes the output.
func (hook *Hook) Fire(entry *logrus.Entry) error {
	if entry.Level < logrus.WarnLevel {
		entry.Message = red(entry.Message)
	} else if entry.Level == logrus.WarnLevel {
		entry.Message = yellow(entry.Message)
	} else {
		if successMessage(entry.Message) {
			entry.Message = green(entry.Message)
		} else if errorMessage(entry.Message) {
			entry.Message = red(entry.Message)
		}
	}

	return nil
}

// Levels returns the various logrus levels this hooks into.
func (hook *Hook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.DebugLevel,
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
	}
}

func init() {
	green = color.New(color.FgGreen).SprintFunc()
	yellow = color.New(color.FgYellow).SprintFunc()
	hiyellow = color.New(color.FgHiYellow).SprintFunc()
	red = color.New(color.FgRed).SprintFunc()
	white = color.New(color.FgWhite).SprintFunc()
	heading = color.New(color.Underline, color.Bold).SprintFunc()
	plain = color.New(color.Reset).SprintFunc()

	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	logrus.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true
}

func New() *logrus.Logger {
	logursLog := logrus.New()
	logursLog.SetFormatter(&MyFormatter{})
	logursLog.ReportCaller = true
	logursLog.Out = io.MultiWriter(os.Stdout)
	return logursLog
}

// GetLogInstance returns the logrus instance
func GetLogInstance() *logrus.Logger {
	if tpLog == nil {
		lock.Lock()
		defer lock.Unlock()
		if tpLog == nil {
			tpLog = New()
		}
	}
	return tpLog
}

func SetLoglevel(logLevel string) {
	switch logLevel {
	case "debug":
		tpLog.Level = logrus.DebugLevel
	case "info":
		tpLog.Level = logrus.InfoLevel
	case "error":
		tpLog.Level = logrus.ErrorLevel
	case "warn":
		tpLog.Level = logrus.WarnLevel
	case "trace":
		tpLog.Level = logrus.TraceLevel
	default:
		tpLog.Level = logrus.DebugLevel

	}
}

// SetTorpedoFileOutput adds output destination for logging
func SetTorpedoFileOutput(logger *lumberjack.Logger) {
	if logger != nil {
		tpLog.Out = io.MultiWriter(tpLog.Out, logger)
		tpLog.Infof("Log Dir: %s", logger.Filename)
	}
}

// SetDefaultOutput  sets default output
func SetDefaultOutput(logger *lumberjack.Logger) {
	if logger != nil {
		tpLog.Out = io.MultiWriter(os.Stdout, logger)
	}

}

type MyFormatter struct{}

func (mf *MyFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}
	level := strings.ToUpper(entry.Level.String())
	strList := strings.Split(entry.Caller.File, "/")
	fileName := strList[len(strList)-1]
	funcList := strings.Split(entry.Caller.Function, "/")
	funcName := funcList[len(funcList)-1]
	subIndex := strings.Index(funcName, ".")
	if subIndex != -1 {
		funcName = funcName[subIndex+1:]
	}

	b.WriteString(fmt.Sprintf("%s:[%s %s:#%d]  %s\n",
		entry.Time.Format("2006-01-02 15:04:05 -0700"), level, fileName, entry.Caller.Line,
		entry.Message))
	return b.Bytes(), nil
}

func Fatalf(format string, args ...interface{}) {
	dash.Fatal(format, args...)
	tpLog.Fatalf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	dash.Errorf(format, args...)
	tpLog.Errorf(format, args...)
}

func Warnf(format string, args ...interface{}) {
	dash.Warnf(format, args...)
	tpLog.Warningf(format, args...)
}

func Infof(format string, args ...interface{}) {
	tpLog.Infof(format, args...)
}

func InfoD(format string, args ...interface{}) {
	dash.Infof(format, args...)
	tpLog.Infof(format, args...)
}

func Debugf(format string, args ...interface{}) {
	tpLog.Debugf(format, args...)
}

func Error(args ...interface{}) {
	dash.Error(fmt.Sprint(args...))
	tpLog.Error(args...)
}

func Warn(args ...interface{}) {
	dash.Warn(fmt.Sprint(args...))
	tpLog.Warn(args...)
}

func Info(args ...interface{}) {
	tpLog.Info(args...)
}

func Debug(args ...interface{}) {
	tpLog.Debug(args...)
}

func Panicf(format string, args ...interface{}) {
	tpLog.Panicf(format, args...)
}

func FailOnError(err error, description string, args ...interface{}) {
	if err != nil {
		Fatalf("%v. Err: %v", fmt.Sprintf(description, args...), err)
	}
}

func init() {
	tpLog = GetLogInstance()
	dash = aetosutil.Get()
}
