package value

import (
	"flag"
	"time"
)

// Duration returns a new flag.Value for the time.Duration type.
func Duration(dest *time.Duration) flag.Value {
	return &durationValue{dest}
}

type durationValue struct {
	dest *time.Duration
}

func (dv *durationValue) Set(val string) error {
	duration, err := time.ParseDuration(val)
	if err != nil {
		return err
	}

	*dv.dest = duration
	return nil
}

func (dv *durationValue) String() string {
	return dv.dest.String()
}
