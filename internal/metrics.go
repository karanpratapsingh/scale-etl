package internal

import (
	"fmt"
	"runtime"
	"time"
)

func MeasureExecTime(id string, function func()) {
	start := time.Now()
	function()
	// PrintMemoryUsage() // TODO: Enable with debug flag?
	duration := time.Since(start)
	fmt.Printf("%s (%s)\n", id, duration)
}

func PrintMemoryUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Alloc = %v MiB", m.Alloc/1024/1024)
	fmt.Printf("\tTotalAlloc = %v MiB", m.TotalAlloc/1024/1024)
	fmt.Printf("\tSys = %v MiB", m.Sys/1024/1024)
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}
