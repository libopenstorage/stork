package util

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo"
)

func nowStamp() string {
	return time.Now().Format(time.StampMilli)
}

func log(level string, format string, args ...interface{}) {
	fmt.Fprintf(ginkgo.GinkgoWriter, nowStamp()+": "+level+": "+format+"\n", args...)
}

// Infof logs info log to ginkgo writer
func Infof(format string, args ...interface{}) {
	log("INFO", format, args...)
}

// Warnf logs warn log to ginkgo writer
func Warnf(format string, args ...interface{}) {
	log("WARN", format, args...)
}

// Errorf logs error log to ginkgo writer
func Errorf(format string, args ...interface{}) {
	log("ERROR", format, args...)
}

// Fatalf logs fatal log to ginkgo writer
func Fatalf(format string, args ...interface{}) {
	log("FATAL", format, args...)
}
