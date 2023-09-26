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

func parseValue(fieldValue string, fieldType string) any {
	switch fieldType {
	case "string", "number": // DynamoDB auto parses number type from string
		return fieldValue
	case "bool":
		val, err := strconv.ParseBool(fieldValue)
		if err != nil {
			panic(err)
		}
		return val
	default:
		panic(fmt.Sprintf("field type %s is not supported", fieldType))
	}
}
