package internal

import (
	"encoding/csv"
	"io"
	"os"

	mapset "github.com/deckarep/golang-set/v2"
)

func ParseChunk(chunk *os.File, batches chan [][]string, schema Schema, batchSize int, delimiter rune) {
	defer chunk.Close()

	var records [][]string

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
				batches <- copySlice(records)
			}
			break
		} else if err != nil {
			panic(err)
		}

		records = append(records, record)

		if len(records) == batchSize {
			batches <- copySlice(records) // Copy slice for goroutine
			records = records[:0]         // Reset batch window
		}
	}
}
