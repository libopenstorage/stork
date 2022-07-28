package value

import (
	"flag"
	"strconv"
)

type float32Value struct {
	dest *float32
}

// Float32 returns a new flag.Value for the float32 type.
func Float32(dest *float32) flag.Value {
	return &float32Value{dest}
}

// Float64 returns a new flag.Value for the float64 type.
func Float64(dest *float64) flag.Value {
	return &float64Value{dest}
}

func (iv *float32Value) Set(val string) error {
	num, err := strconv.ParseFloat(val, 32)
	if err != nil {
		return err
	}

	*iv.dest = float32(num)
	return nil
}

func (iv *float32Value) String() string {
	return strconv.FormatFloat(float64(*iv.dest), 'G', -1, 32)
}

type float64Value struct {
	dest *float64
}

func (iv *float64Value) Set(val string) error {
	num, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return err
	}

	*iv.dest = float64(num)
	return nil
}

func (iv *float64Value) String() string {
	return strconv.FormatFloat(float64(*iv.dest), 'G', -1, 64)
}
