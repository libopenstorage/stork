package common

import (
	"fmt"
	"os"
	"strconv"

	"k8s.io/client-go/rest"
)

const (
	QPSRate   = "KUBERNETES_OPS_QPS_RATE"
	BurstRate = "KUBERNETES_OPS_BURST_RATE"
)

func SetRateLimiter(config *rest.Config) error {
	if val := os.Getenv(QPSRate); val != "" {
		qps, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid qps count specified %v: %v", val, err)
		}
		config.QPS = float32(qps)
	}
	if val := os.Getenv(BurstRate); val != "" {
		burst, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid burst count specified %v: %v", val, err)
		}
		config.Burst = int(burst)
	}
	return nil
}
