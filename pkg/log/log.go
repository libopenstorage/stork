package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

type colorizer func(...interface{}) string

var (
	green    colorizer
	yellow   colorizer
	hiyellow colorizer
	red      colorizer
	white    colorizer
	heading  colorizer
	plain    colorizer
)

var log *logrus.Logger
var lock = &sync.Mutex{}

// We are logging to file, strip colors to make the output more readable
var txtFormatter = &logrus.TextFormatter{DisableColors: true}

// Hook to handle writing to local log files.
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

// SetFormatter sets the log formatter.
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

//GetLogInstance returns the logrus instance
func GetLogInstance() *logrus.Logger {
	if log == nil {
		lock.Lock()
		defer lock.Unlock()
		if log == nil {
			log = logrus.New()
			log.SetFormatter(&MyFormatter{})
			log.ReportCaller = true
			log.Out = io.MultiWriter(os.Stdout)
		}
	}
	return log
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

	b.WriteString(fmt.Sprintf("%s:[%s %s::%s:%d]  %s\n",
		entry.Time.Format("2006-01-02 15:04:05 -0700"), level, fileName, funcName, entry.Caller.Line,
		entry.Message))
	return b.Bytes(), nil
}
