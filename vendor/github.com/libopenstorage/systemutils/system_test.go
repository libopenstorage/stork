package systemutils

import (
	"fmt"
	"testing"
	"time"
)

func TestSystemUtils(t *testing.T) {
	s := New()
	for i := 0; i < 10; i++ {
		cpuUsage, totalTicks, ticks := s.CpuUsage()
		memUsage := s.MemUsage()
		lunsMap := s.Luns()
		fmt.Printf("CpuUsage: %f, totalTicks: %f, ticks: %f\n",
			cpuUsage, totalTicks, ticks)
		fmt.Printf("MemUsage: %f\n", memUsage)
		fmt.Println("Luns Map: ", lunsMap)
		time.Sleep(2 * time.Second)
	}
}
