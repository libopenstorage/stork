package yag

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/zoido/yag-config/value"
)

// ErrHelp is the error returned if the -help or -h flag is invoked but no such flag is defined.
// Alias for flag.ErrHelp
var ErrHelp = flag.ErrHelp

// Parser registers and parses configuration values.
type Parser struct {
	envPrefix string
	flagSet   *flag.FlagSet

	vars []*variable
}

// New returns new instance of the Yag.
func New(options ...ParserOption) *Parser {
	vars := make([]*variable, 0, 10)

	y := &Parser{
		vars: vars,
	}
	for _, opt := range options {
		opt(y)
	}
	y.flagSet = &flag.FlagSet{Usage: func() {}}
	return y
}

// Value registers new generic flag.Value implementation for parsing.
func (y *Parser) Value(v flag.Value, name, help string, options ...VarOption) {
	y.addVar(&wrapper{dest: v}, name, help, options...)
}

// String registers new string variable for parsing.
func (y *Parser) String(s *string, name, help string, options ...VarOption) {
	y.Value(value.String(s), name, help, options...)
}

// Int registers new int variable for parsing.
func (y *Parser) Int(i *int, name, help string, options ...VarOption) {
	y.Value(value.Int(i), name, help, options...)
}

// Int8 registers new int8 variable for parsing.
func (y *Parser) Int8(i8 *int8, name, help string, options ...VarOption) {
	y.Value(value.Int8(i8), name, help, options...)
}

// Int16 registers new int16 variable for parsing.
func (y *Parser) Int16(i16 *int16, name, help string, options ...VarOption) {
	y.Value(value.Int16(i16), name, help, options...)
}

// Int32 registers new int32 variable for parsing.
func (y *Parser) Int32(i32 *int32, name, help string, options ...VarOption) {
	y.Value(value.Int32(i32), name, help, options...)
}

// Int64 registers new int64 variable for parsing.
func (y *Parser) Int64(i64 *int64, name, help string, options ...VarOption) {
	y.Value(value.Int64(i64), name, help, options...)
}

// Uint registers new uint variable for parsing.
func (y *Parser) Uint(ui *uint, name, help string, options ...VarOption) {
	y.Value(value.Uint(ui), name, help, options...)
}

// Uint8 registers new uint8 variable for parsing.
func (y *Parser) Uint8(ui8 *uint8, name, help string, options ...VarOption) {
	y.Value(value.Uint8(ui8), name, help, options...)
}

// Uint16 registers new uint16 variable for parsing.
func (y *Parser) Uint16(ui16 *uint16, name, help string, options ...VarOption) {
	y.Value(value.Uint16(ui16), name, help, options...)
}

// Uint32 registers new uint32 variable for parsing.
func (y *Parser) Uint32(ui32 *uint32, name, help string, options ...VarOption) {
	y.Value(value.Uint32(ui32), name, help, options...)
}

// Uint64 registers new uint64 variable for parsing.
func (y *Parser) Uint64(ui64 *uint64, name, help string, options ...VarOption) {
	y.Value(value.Uint64(ui64), name, help, options...)
}

// Float32 registers new float32 variable for parsing.
func (y *Parser) Float32(f32 *float32, name, help string, options ...VarOption) {
	y.Value(value.Float32(f32), name, help, options...)
}

// Float64 registers new float64 variable for parsing.
func (y *Parser) Float64(f32 *float64, name, help string, options ...VarOption) {
	y.Value(value.Float64(f32), name, help, options...)
}

// Bool registers new bool variable for parsing.
func (y *Parser) Bool(b *bool, name, help string, options ...VarOption) {
	y.Value(value.Bool(b), name, help, options...)
}

// Duration registers new time.Duration variable for parsing.
func (y *Parser) Duration(d *time.Duration, name, help string, options ...VarOption) {
	y.Value(value.Duration(d), name, help, options...)
}

func (y *Parser) addVar(w *wrapper, name, help string, options ...VarOption) {
	v := &variable{
		flag:      w,
		envName:   strings.ToUpper(fmt.Sprintf("%s%s", y.envPrefix, name)),
		name:      name,
		help:      help,
		parseEnv:  true,
		parseFlag: true,
	}
	for _, opt := range options {
		opt(v)
	}
	y.vars = append(y.vars, v)
	if v.parseFlag {
		y.flagSet.Var(w, name, help)
	}
}

func (y *Parser) validate() error {
	for _, v := range y.vars {
		if v.required && !v.flag.isSet() {
			return fmt.Errorf("config option '%s' is required", v.name)
		}
	}
	return nil
}

// ParseFlags parses configuration values from commandline flags.
func (y *Parser) ParseFlags(args []string) error {
	if err := y.doParseFlags(args); err != nil {
		return err
	}
	return y.validate()
}

func (y *Parser) doParseFlags(args []string) error {
	return y.flagSet.Parse(args)
}

// ParseEnv parses configuration values from environment variables.
func (y *Parser) ParseEnv() error {
	if err := y.doParseEnv(); err != nil {
		return err
	}
	return y.validate()
}

func (y *Parser) doParseEnv() error {
	for _, v := range y.vars {
		if !v.parseEnv {
			continue
		}

		envValue, envIsSet := os.LookupEnv(v.envName)
		if envIsSet && !v.flag.isSet() {
			if err := v.flag.Set(envValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// Parse parses configuration values from flags and environment variables.
// Flags values always override environment variable value.
func (y *Parser) Parse(args []string) error {
	if err := y.doParseFlags(args); err != nil {
		return err
	}
	if err := y.doParseEnv(); err != nil {
		return err
	}
	return y.validate()
}

// Usage returns formatted string with usage help.
func (y *Parser) Usage() string {
	u := make([]string, len(y.vars))
	for i, v := range y.vars {
		u[i] = v.usage()
	}
	return strings.Join(u, "\n")
}
