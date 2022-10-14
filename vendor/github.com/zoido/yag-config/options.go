package yag

// ParserOption configures the Parser.
type ParserOption func(c *Parser)

// WithEnvPrefix configures the prefix of the environment variable names that will be used for the
// lookup of the values. Default without prefix.
func WithEnvPrefix(prefix string) ParserOption {
	return func(c *Parser) {
		c.envPrefix = prefix
	}
}

// VarOption configures handling of registered variables.
type VarOption func(v *variable)

// FromEnv overrides the environment variable name thay will be used to obtain the set value of the
// registered variable.
func FromEnv(envName string) VarOption {
	return func(v *variable) {
		v.envName = envName
	}
}

// Required sets the variable as required. Parsing will fail when the variable is not set via flags
// nor environment.
func Required() VarOption {
	return func(v *variable) {
		v.required = true
	}
}

// NoEnv disables looking up of the variable value in the environment variables.
func NoEnv() VarOption {
	return func(v *variable) {
		v.parseEnv = false
	}
}

// NoFlag disables the flag for the variable. Useful for the options that should not appear on
// command line.
func NoFlag() VarOption {
	return func(v *variable) {
		v.parseFlag = false
	}
}
