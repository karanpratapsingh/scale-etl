package internal

import (
	"encoding/csv"
	"io"
	"os"
	"strings"
	"sync"
)

func ProcessChunk(chunk *os.File, batchSize int, transformer Transformer) {
	defer chunk.Close()

	var wg sync.WaitGroup
	var records [][]string

	processBatch := func(wg *sync.WaitGroup, records [][]string) {
		defer wg.Done()
		wg.Add(1)

		id := getChunkId(chunk.Name())
		transformer.Transform(id, records)
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

func getChunkId(name string) string {
	parts := strings.Split(name, "/")
	return parts[len(parts)-1]
}
