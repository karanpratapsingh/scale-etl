package main

import (
	"csv-ingest/internal"
	"fmt"
	"os"
	"sync"
	"time"
)

var args internal.Args = internal.ParseArgs()
var config internal.Config = internal.NewConfig(args.ConfigPath)

var chunks = make(chan *os.File)

func main() {
	start := time.Now()
	var wg sync.WaitGroup

	dirPath := internal.SplitFile(config.FilePath, config.ChunkSize)
	go internal.ReadChunks(dirPath, chunks) // Layer 1

	var transformer = internal.NewTransformer(config.TransformType)

	for chunk := range chunks {
		go func(chunk *os.File, wg *sync.WaitGroup) {
			defer wg.Done()
			wg.Add(1)

			internal.ProcessChunk(chunk, config.BatchSize, transformer) // Layer 2
		}(chunk, &wg)
	}

	wg.Wait()

	duration := time.Since(start)
	fmt.Printf("execution completed in %s\n", duration)

	transformer.Save()
}
