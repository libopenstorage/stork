package common

import (
	"fmt"
	"os"
	"strconv"

	"k8s.io/client-go/rest"
)

const (
	qPSRate   = "KUBERNETES_OPS_QPS_RATE"
	burstRate = "KUBERNETES_OPS_BURST_RATE"
)

// SetRateLimiter sets rate limiter
func SetRateLimiter(config *rest.Config) error {
	if val := os.Getenv(qPSRate); val != "" {
		qps, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid qps count specified %v: %v", val, err)
		}
		config.QPS = float32(qps)
	}
	if val := os.Getenv(burstRate); val != "" {
		burst, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid burst count specified %v: %v", val, err)
		}
		config.Burst = int(burst)
	}
	return nil
}
