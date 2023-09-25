package internal

import (
	"fmt"
	"time"
)

func MeasureExecTime(id string, function func()) {
	start := time.Now()
	function()
	duration := time.Since(start)
	fmt.Printf("%s (%s)\n", id, duration)
}
