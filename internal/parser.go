package internal

import (
	"encoding/csv"
	"io"
	"os"
	"sync"
)

func ParseChunk(chunk *os.File, transformer Transformer, batchSize int, delimiter rune) {
	defer chunk.Close()

	var wg sync.WaitGroup
	var records [][]string

	processBatch := func(wg *sync.WaitGroup, records [][]string) {
		defer wg.Done()
		transformer.Transform(records) // Layer 3
	}

	reader := csv.NewReader(chunk)
	reader.Comma = delimiter

	for {
		record, err := reader.Read()

		if err == io.EOF {
			// Remaining records when window size is less than batch size
			if len(records) != 0 {
				wg.Add(1)
				go processBatch(&wg, records)
			}
			break
		} else if err != nil {
			panic(err)
		}

		records = append(records, record)

		if len(records) == batchSize {
			wg.Add(1)
			go processBatch(&wg, records)
			records = records[:0] // Reset
		}
	}

	wg.Wait()
}
