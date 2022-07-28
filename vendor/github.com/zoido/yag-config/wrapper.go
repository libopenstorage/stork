package yag

import (
	"flag"

	"github.com/zoido/yag-config/value"
)

type wrapper struct {
	dest flag.Value
	set  bool
}

func (w *wrapper) Set(val string) error {
	err := w.dest.Set(val)
	if err != nil {
		return err
	}

	w.set = true
	return nil
}

func (w *wrapper) String() string {
	if w.isSet() {
		return w.dest.String()
	}
	return ""
}

func (w *wrapper) IsBoolFlag() bool {
	return value.IsBoolFlag(w.dest)
}

func (w *wrapper) isSet() bool {
	return w.set
}
