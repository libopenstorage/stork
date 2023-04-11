package log

import (
	"bytes"
	"fmt"
	"github.com/google/gnostic/compiler"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/portworx/torpedo/pkg/aetosutil"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"runtime"
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
	var writeString string
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}
	level := strings.ToUpper(entry.Level.String())
	funcList := strings.Split(entry.Caller.Function, "/")
	funcName := funcList[len(funcList)-1]
	subIndex := strings.Index(funcName, ".")
	if subIndex != -1 {
		funcName = funcName[subIndex+1:]
	}

	// TODO: Once we migrate to ginkgo v2, we can extract test name as follows:
	// report := CurrentSpecReport()
	// testName := report.ContainerHierarchyTexts[0]
	report := CurrentGinkgoTestDescription()
	testName := strings.Split(report.FullTestText, " ")[0]
	if testName != "" {
		writeString = fmt.Sprintf("%s:[%s] [%s] %s\n",
			entry.Time.Format("2006-01-02 15:04:05 -0700"), level, testName,
			entry.Message)
	} else {
		writeString = fmt.Sprintf("%s:[%s] %s\n",
			entry.Time.Format("2006-01-02 15:04:05 -0700"), level,
			entry.Message)
	}

	b.WriteString(writeString)
	return b.Bytes(), nil
}

func Fatalf(format string, args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, format)
	dash.Fatal(extendedFormat, args...)
	tpLog.Errorf(extendedFormat, args...)
}

func Errorf(format string, args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, format)
	dash.Errorf(extendedFormat, args...)
	tpLog.Errorf(extendedFormat, args...)
}

func Warnf(format string, args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, format)
	dash.Warnf(extendedFormat, args...)
	tpLog.Warningf(extendedFormat, args...)
}

func Infof(format string, args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, format)
	tpLog.Infof(extendedFormat, args...)
}

func InfoD(format string, args ...interface{}) {
	dash.Infof(format, args...)
	tpLog.Infof(format, args...)
}

func Debugf(format string, args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, format)
	tpLog.Debugf(extendedFormat, args...)
}

func Error(args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, strings.Join(compiler.ConvertInterfaceArrayToStringArray(args), " "))
	dash.Error(fmt.Sprint(args...))
	tpLog.Error(extendedFormat)
}

func Warn(args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, strings.Join(compiler.ConvertInterfaceArrayToStringArray(args), " "))
	dash.Warn(fmt.Sprint(args...))
	tpLog.Warn(extendedFormat)
}

func Info(args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, strings.Join(compiler.ConvertInterfaceArrayToStringArray(args), " "))
	tpLog.Info(extendedFormat)
}

func Debug(args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, strings.Join(compiler.ConvertInterfaceArrayToStringArray(args), " "))
	tpLog.Debugf(extendedFormat, args...)
}

func Panicf(format string, args ...interface{}) {
	pc, _, line, _ := runtime.Caller(1)
	callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
	callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
	extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, format)
	tpLog.Panicf(extendedFormat, args...)
}

func FailOnError(err error, description string, args ...interface{}) {
	if err != nil {
		errorString := fmt.Sprintf("%v. Err: %v", fmt.Sprintf(description, args...), err)
		pc, _, line, _ := runtime.Caller(1)
		callerFuncSlice := strings.Split(runtime.FuncForPC(pc).Name(), "/")
		callerFunc := fmt.Sprintf("%s:#%d", callerFuncSlice[len(callerFuncSlice)-1], line)
		extendedFormat := fmt.Sprintf("[%s] - %s", callerFunc, errorString)
		dash.Fatal(extendedFormat)
		tpLog.Errorf(extendedFormat)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
}

func init() {
	tpLog = GetLogInstance()
	dash = aetosutil.Get()
}
