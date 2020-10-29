package dbg

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	fnameFmt       = "2006-01-02T15:04:05.999999-0700MST"
	stackTraceSize = 5120 * 1024 // 5MB
	logLevelFile   = "/tmp/loglevel"
)

// Init Initializes the debug handlers
func Init(name string, dir string) {
	installSignalHandlers(name, dir)
}

func installSignalHandlers(name string, dir string) {
	dumpChannel := make(chan os.Signal, 1)
	cpuProfChannel := make(chan os.Signal, 1)
	signal.Notify(dumpChannel, syscall.SIGUSR1)
	go func() {
		for {
			<-dumpChannel
			dumpGoMemoryTrace()
			dumpHeap(generateFilename(name, dir, ".heap"))
			dumpGoProfile(generateFilename(name, dir, ".stack"))
			changeLogLevel()
		}
	}()
	signal.Notify(cpuProfChannel, syscall.SIGUSR2)
	go func() {
		started := false
		for {
			<-cpuProfChannel
			if !started {
				started = startCPUProfileDump(generateFilename(name, dir, ".cpuprof"))
			} else {
				pprof.StopCPUProfile()
				started = false
			}
		}
	}()

}

func generateFilename(name string, dir string, suffix string) string {
	return path.Join(dir, name+"."+time.Now().Format(fnameFmt)+suffix)
}

func changeLogLevel() {
	var level string
	data, err := ioutil.ReadFile(logLevelFile)
	if err != nil {
		level = "info"
	} else {
		level = strings.ToLower(string(data))
	}
	switch {
	case strings.Contains(level, "debug"):
		logrus.SetLevel(logrus.DebugLevel)
	case strings.Contains(level, "trace"):
		logrus.SetLevel(logrus.TraceLevel)
	case strings.Contains(level, "info"):
		logrus.SetLevel(logrus.InfoLevel)
	case strings.Contains(level, "warn"):
		logrus.SetLevel(logrus.WarnLevel)
	case strings.Contains(level, "error"):
		logrus.SetLevel(logrus.ErrorLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}
}

func startCPUProfileDump(filename string) bool {
	f, err := os.Create(filename)
	if err != nil {
		logrus.Errorf("could not create cpu profile: %v", err)
		return false
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		logrus.Errorf("Error starting cpu profiling: %v", err)
		return false
	}
	return true
}

// dumpGoMemoryTrace output memory profile to logs.
func dumpGoMemoryTrace() {
	m := &runtime.MemStats{}
	runtime.ReadMemStats(m)
	res := fmt.Sprintf("%#v", m)
	logrus.Infof("==== Dumping Memory Profile ===")
	logrus.Infof(res)
}

// dumpGoProfile output goroutines to file.
func dumpGoProfile(filename string) {
	trace := make([]byte, stackTraceSize)
	len := runtime.Stack(trace, true)
	if err := ioutil.WriteFile(filename, trace[:len], 0644); err != nil {
		logrus.Errorf("Error dumping goroutines: %v", err)
	}
}

// dumpHeap dumps the memory heap to a file
func dumpHeap(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		logrus.Errorf("could not create memory profile: %v", err)
		return
	}
	defer func() {
		if err := f.Close(); err != nil {
			logrus.Errorf("Error closing heap file: %v", err)
		}
	}()

	if err := pprof.WriteHeapProfile(f); err != nil {
		logrus.Errorf("could not write memory profile: %v", err)
	}
}
