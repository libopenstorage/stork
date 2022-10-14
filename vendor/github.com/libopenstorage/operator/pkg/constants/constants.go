package constants

import "time"

const (
	// DefaultCordonedRestartDelay initial duration for which the operator should not try
	// to restart pods after the node is cordoned
	DefaultCordonedRestartDelay = 5 * time.Second
	// MaxCordonedRestartDelay maximum duration for which the operator should not try
	// to restart pods after the node is cordoned
	MaxCordonedRestartDelay = 15 * time.Minute
)
