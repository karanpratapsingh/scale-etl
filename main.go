package main

import (
	"csv-ingest/internal"
	"fmt"
	"os"
	"sync"
	"time"
)

var chunks = make(chan *os.File)
// var batches = make(chan [][]string)

var chunkSize int
var batchSize int
var file string

func init() {
	internal.Args(chunkSize, batchSize, file)
}

func processBatch(records [][]string) { // Layer 3
	fmt.Println("processed", len(records))
}

func main() {
	start := time.Now()
	filename := "test"

	var wg sync.WaitGroup

	dirPath := internal.SplitFile(filename, chunkSize)
	go internal.ReadChunks(dirPath, chunks) // Layer 1

	for c := range chunks {
		wg.Add(1)
		go func(chunk *os.File, wg *sync.WaitGroup) {
			defer wg.Done()
			internal.ProcessChunk(chunk, batchSize, processBatch) // Layer 2
		}(c, &wg)
	}

	wg.Wait()

	duration := time.Since(start)
	fmt.Printf("Execution took %s\n", duration)
}
