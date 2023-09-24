package main

import (
	"csv-ingest/internal"
	"os"
	"sync"
)

var args internal.Args = internal.ParseArgs()
var config internal.Config = internal.NewConfig(args.ConfigPath)

var chunks = make(chan *os.File)

func main() {
	var wg sync.WaitGroup

	dirPath := internal.SplitFile(config.FilePath, config.ChunkSize)
	go internal.ReadChunks(dirPath, chunks) // Layer 1

	var transformer = internal.NewTransformer(config.TransformType)

	internal.MeasureExecTime("processing", func() {
		for chunk := range chunks {
			wg.Add(1)

			go func(chunk *os.File, wg *sync.WaitGroup) {
				defer wg.Done()
				internal.ProcessChunk(chunk, config.BatchSize, transformer) // Layer 2
			}(chunk, &wg)
		}

		wg.Wait()
	})

	internal.MeasureExecTime("saving", func() {
		transformer.Save()
	})
}
