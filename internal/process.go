package internal

import (
	"encoding/csv"
	"io"
	"os"
	"sync"
)

func ProcessChunk(chunk *os.File, batchSize int, transformer Transformer) {
	defer chunk.Close()

	var wg sync.WaitGroup
	var records [][]string

	processBatch := func(wg *sync.WaitGroup, records [][]string) {
		defer wg.Done()
		wg.Add(1)

		transformer.Transform(records)
	}

	reader := csv.NewReader(chunk)

	for {
		record, err := reader.Read()

		if err == io.EOF {
			// Remaining records when window size is less than batch size
			if len(records) != 0 {
				go processBatch(&wg, records)
			}
			break
		} else if err != nil {
			panic(err)
		}

		records = append(records, record)

		if len(records) == batchSize {
			go processBatch(&wg, records)
			records = records[:0] // Reset
		}
	}

	wg.Wait()
}
