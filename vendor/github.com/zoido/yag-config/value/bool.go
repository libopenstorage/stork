package value

import (
	"flag"
	"strconv"
)

// Bool returns a new flag.Value for the bool type.
func Bool(dest *bool) flag.Value {
	return &boolValue{dest}
}

// IsBoolFlag returns true if the flag.Value implementation has an IsBoolFlag() bool
// method returning true.
// This is used by the flag package commandline-parser for flags that can be supplied
//  without "=value" text
func IsBoolFlag(v flag.Value) bool {
	type boolFlag interface {
		flag.Value
		IsBoolFlag() bool
	}
	if bv, ok := v.(boolFlag); ok {
		return bv.IsBoolFlag()
	}
	return false
}

type boolValue struct {
	dest *bool
}

func (*boolValue) IsBoolFlag() bool {
	return true
}

func (bv *boolValue) Set(val string) error {
	b, err := strconv.ParseBool(val)
	if err != nil {
		return err
	}

	*bv.dest = b
	return nil
}

func (bv *boolValue) String() string {
	return strconv.FormatBool(*bv.dest)
}
