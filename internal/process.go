package internal

import (
	"encoding/csv"
	"io"
	"os"
	"sync"
)

func ProcessChunk(chunk *os.File, batchSize int, processBatch func([][]string)) {
	defer chunk.Close()

	var wg sync.WaitGroup
	var records [][]string

	process := func(wg *sync.WaitGroup, records [][]string) {
		defer wg.Done()
		wg.Add(1)

		processBatch(records)
	}

	reader := csv.NewReader(chunk)

	for {
		record, err := reader.Read()

		if err == io.EOF {
			// Remaining records when window size is less than batch size
			if len(records) != 0 {
				go process(&wg, records)
			}
			break
		} else if err != nil {
			panic(err)
		}

		records = append(records, record)

		if len(records) == batchSize {
			go process(&wg, records)
			records = records[:0] // Reset
		}
	}

	wg.Wait()
}
