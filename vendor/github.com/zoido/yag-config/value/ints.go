package value

import (
	"flag"
	"strconv"
)

// Int returns a new flag.Value for the int type.
func Int(dest *int) flag.Value {
	return &intValue{dest}
}

// Int8 returns a new flag.Value for the int8 type.
func Int8(dest *int8) flag.Value {
	return &int8Value{dest}
}

// Int16 returns a new flag.Value for the int16 type.
func Int16(dest *int16) flag.Value {
	return &int16Value{dest}
}

// Int32 returns a new flag.Value for the int32 type.
func Int32(dest *int32) flag.Value {
	return &int32Value{dest}
}

// Int64 returns a new flag.Value for the int64 type.
func Int64(dest *int64) flag.Value {
	return &int64Value{dest}
}

type intValue struct {
	dest *int
}

func (iv *intValue) Set(val string) error {
	num, err := strconv.Atoi(val)
	if err != nil {
		return err
	}

	*iv.dest = num
	return nil
}

func (iv *intValue) String() string {
	return strconv.Itoa(*iv.dest)
}

type int8Value struct {
	dest *int8
}

func (iv *int8Value) Set(val string) error {
	num, err := strconv.ParseInt(val, 10, 8)
	if err != nil {
		return err
	}

	*iv.dest = int8(num)
	return nil
}

func (iv *int8Value) String() string {
	return strconv.FormatInt(int64(*iv.dest), 10)
}

type int16Value struct {
	dest *int16
}

func (iv *int16Value) Set(val string) error {
	num, err := strconv.ParseInt(val, 10, 16)
	if err != nil {
		return err
	}

	*iv.dest = int16(num)
	return nil
}

func (iv *int16Value) String() string {
	return strconv.FormatInt(int64(*iv.dest), 10)
}

type int32Value struct {
	dest *int32
}

func (iv *int32Value) Set(val string) error {
	num, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return err
	}

	*iv.dest = int32(num)
	return nil
}

func (iv *int32Value) String() string {
	return strconv.FormatInt(int64(*iv.dest), 10)
}

type int64Value struct {
	dest *int64
}

func (iv *int64Value) Set(val string) error {
	num, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}

	*iv.dest = num
	return nil
}

func (iv *int64Value) String() string {
	return strconv.FormatInt(*iv.dest, 10)
}
