package internal

import (
	"fmt"
	"strconv"
	"sync/atomic"
)

type Counter struct {
	val *int32
}

func NewCounter(val int32) Counter {
	return Counter{&val}
}

func (c Counter) get() int32 {
	atomic.AddInt32(c.val, 1)
	return atomic.LoadInt32(c.val)
}

func copySlice[T any](input [][]T) [][]T {
	copiedSlice := make([][]T, len(input))
	copy(copiedSlice, input)

	return copiedSlice
}

func parseValue(columnValue string, columnType string) any {
	switch columnType {
	case "string", "number": // DynamoDB auto parses number type from string
		return columnValue
	case "bool":
		val, err := strconv.ParseBool(columnValue)
		if err != nil {
			panic(err)
		}
		return val
	default:
		panic(fmt.Sprintf("column type %s is not supported", columnType))
	}
}

// TODO: Add github docs link
func HandlePanic() {
	if r := recover(); r != nil {
		fmt.Println("Error:", r)
	}
}
