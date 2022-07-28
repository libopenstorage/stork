package value

import "flag"

// String returns a new flag.Value for the string type.
func String(dest *string) flag.Value {
	return &stringValue{dest}
}

type stringValue struct {
	dest *string
}

func (sv *stringValue) Set(val string) error {
	*sv.dest = val
	return nil
}

func (sv *stringValue) String() string {
	return *sv.dest
}
