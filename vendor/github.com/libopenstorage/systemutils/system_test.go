package systemutils

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestSystemUtils(t *testing.T) {
	s := New()
	for i := 0; i < 10; i++ {
		cpuUsage, totalTicks, ticks := s.CpuUsage()
		if math.IsNaN(cpuUsage) {
			t.Errorf("cpu usage is NaN")
		}
		memUsage, _, _ := s.MemUsage()
		fmt.Printf("CpuUsage: %f, totalTicks: %f, ticks: %f\n",
			cpuUsage, totalTicks, ticks)
		fmt.Printf("MemUsage: %v\n", memUsage)
		time.Sleep(2 * time.Second)
	}
}
