package internal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

func MeasureExecTime(id string, function func()) {
	start := time.Now()
	function()
	duration := time.Since(start)
	fmt.Printf("%s [%s]\n", id, duration)
}

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

func generateHash(input string) string {
	hash := sha256.Sum256([]byte(input))
	hashString := hex.EncodeToString(hash[:])

	return hashString[:16]
}

func copySlice[T any](input [][]T) [][]T {
	copiedSlice := make([][]T, len(input))
	copy(copiedSlice, input)

	return copiedSlice
}

func parseValue(columnValue string, columnType string) any {
	// Fun fact: DynamoDB auto parses number type from string
	switch columnType {
	case "string":
		return columnValue
	case "number":
		val, err := strconv.Atoi(columnValue)
		if err != nil {
			panic(err)
		}
		return val
	case "bool":
		val, err := strconv.ParseBool(columnValue)
		if err != nil {
			panic(err)
		}
		return val
	default:
		panic(ErrUnsupportedColumnType(columnType))
	}
}

func HandlePanic() {
	if r := recover(); r != nil {
		fmt.Println("Error:", r)
		fmt.Println("For more info try --help")
	}
}

func ConcatenateArrays[T any](arrays ...[]T) []T {
	var concatenated []T

	for _, arr := range arrays {
		concatenated = append(concatenated, arr...)
	}

	return concatenated
}

func chunk[T any](items []T, size int) [][]T {
	var chunks [][]T
	N := len(items)

	for i := 0; i < N; i += size {
		end := i + size
		if end > N {
			end = N
		}
		chunks = append(chunks, items[i:end])
	}

	return chunks
}

func ParseRune(str string) rune {
	return []rune(str)[0]
}
