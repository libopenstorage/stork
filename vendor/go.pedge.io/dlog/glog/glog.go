/*
Package dlog_glog provides glog functionality for dlog.

https://github.com/golang/glog
*/
package dlog_glog // import "go.pedge.io/dlog/glog"

import (
	"go.pedge.io/dlog"

	"github.com/golang/glog"
)

// Register registers the default glog Logger as the dlog Logger.
func Register() {
	dlog.SetLogger(NewLogger())
}

// NewLogger returns a new dlog.Logger for glog.
func NewLogger() dlog.Logger {
	return dlog.NewLogger(
		glog.Infoln,
		map[dlog.Level]func(...interface{}){
			dlog.LevelDebug: glog.Infoln,
			dlog.LevelInfo:  glog.Infoln,
			dlog.LevelWarn:  glog.Warningln,
			dlog.LevelError: glog.Errorln,
			// this relies on implementation-specific details of the generic dlog Logger implementation, not good
			dlog.LevelFatal: glog.Fatalln,
			dlog.LevelPanic: glog.Errorln,
		},
	)
}
