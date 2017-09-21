package dlog_testing

import (
	"flag"
	"testing"

	"go.pedge.io/dlog"
	"go.pedge.io/dlog/glog"
	"go.pedge.io/dlog/lion"
	"go.pedge.io/dlog/log15"
	"go.pedge.io/dlog/logrus"
	"go.pedge.io/dlog/zap"
)

func TestPrint(t *testing.T) {
	dlog.Register()
	testPrint(t)
}

func TestPrintGlog(t *testing.T) {
	_ = flag.Set("alsologtostderr", "true")
	dlog_glog.Register()
	testPrint(t)
}

func TestPrintLion(t *testing.T) {
	dlog_lion.Register()
	testPrint(t)
}

func TestPrintLog15(t *testing.T) {
	dlog_log15.Register()
	testPrint(t)
}

func TestPrintLogrus(t *testing.T) {
	dlog_logrus.Register()
	testPrint(t)
}

func TestPrintZap(t *testing.T) {
	dlog_zap.Register()
	testPrint(t)
}

func testPrint(t *testing.T) {
	dlog.WithField("key", "value").WithField("int", 1).Infof("number %d", 2)
	dlog.Warnln("warning line")
}
