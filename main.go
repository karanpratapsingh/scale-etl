package main

import (
	"csv-ingest/internal"
	"fmt"
	"os"
	"sync"
	"time"
)

var chunks = make(chan *os.File)
var args internal.Args = internal.ParseArgs()

func processBatch(records [][]string) { // Layer 3
	// fmt.Println("processed", len(records))
}

func main() {
	start := time.Now()
	var wg sync.WaitGroup

	dirPath := internal.SplitFile(args.FilePath, args.ChunkSize)
	go internal.ReadChunks(dirPath, chunks) // Layer 1

	for c := range chunks {
		wg.Add(1)
		go func(chunk *os.File, wg *sync.WaitGroup) {
			defer wg.Done()
			internal.ProcessChunk(chunk, args.BatchSize, processBatch) // Layer 2
		}(c, &wg)
	}

	wg.Wait()

	duration := time.Since(start)
	fmt.Printf("Execution completed in %s\n", duration)
}
