package constants

import "time"

const (
	// DefaultCordonedRestartDelay duration for which the operator should not try
	// to restart pods after the node is cordoned
	DefaultCordonedRestartDelay = 15 * time.Minute
)
