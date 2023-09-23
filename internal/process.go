package internal

import (
	"encoding/csv"
	"os"
	"sync"
)

func ProcessChunk(chunk *os.File, batchSize int, processBatch func([][]string)) {
	defer chunk.Close()

	var wg sync.WaitGroup
	reader := csv.NewReader(chunk)

	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	if batchSize > len(records) {
		processBatch(records)
		return
	}

	batches := len(records) / batchSize

	for i := 0; i < batches; i += 1 {
		start := i * batchSize
		end := start + batchSize

		if end > len(records) {
			end = len(records)
		}

		wg.Add(1)
		go func(i int, j int, records [][]string, wg *sync.WaitGroup) {
			defer wg.Done()
			processBatch(records[start:end]) // Layer 3
		}(start, end, records, &wg)
	}

	wg.Wait()
}
