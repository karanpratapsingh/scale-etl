package internal

import (
	"encoding/csv"
	"io"
	"os"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
)

func ParseChunk(chunk *os.File, transformer Transformer, schema Schema, batchSize int, delimiter rune) {
	defer chunk.Close()

	var wg sync.WaitGroup
	var records [][]string

	processBatch := func(wg *sync.WaitGroup, records [][]string) {
		defer wg.Done()
		transformer.Transform(records) // Layer 3
	}

	reader := csv.NewReader(chunk)
	reader.Comma = delimiter

	record, err := reader.Read()
	if err != nil {
		panic(err)
	}

	// Skip csv header (if present)
	recordSet := mapset.NewSet(record...)
	if !recordSet.Equal(schema.Header) {
		records = append(records, record)
	}

	for {
		record, err := reader.Read()

		if err == io.EOF {
			// Remaining records when window size is less than batch size
			if len(records) != 0 {
				wg.Add(1)
				go processBatch(&wg, copySlice(records))
			}
			break
		} else if err != nil {
			panic(err)
		}

		records = append(records, record)

		if len(records) == batchSize {
			wg.Add(1)

			go processBatch(&wg, copySlice(records)) // Copy slice for goroutine
			records = records[:0]                    // Reset batch window
		}
	}

	wg.Wait()
}

func copySlice(input [][]string) [][]string {
	copiedSlice := make([][]string, len(input))
	copy(copiedSlice, input)

	return copiedSlice
}
