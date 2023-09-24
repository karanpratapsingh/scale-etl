package main

import (
	"csv-ingest/internal"
	"fmt"
	"os"
	"sync"
)

var args internal.Args = internal.ParseArgs()
var config internal.Config = internal.NewConfig(args.ConfigPath)

var chunks = make(chan *os.File, config.BufferSize)
var batches = make(chan [][]string, config.BufferSize)

func main() {
	var transformer = internal.NewTransformer(config.TransformType, config.FilePath, config.OutputDir)

	dirPath := internal.SplitFile(config.FilePath, config.ProcessDir, config.ChunkSize)

	go internal.MeasureExecTime("reading chunks", func() {
		internal.ReadChunks(dirPath, chunks) // Layer 1
	})

	internal.MeasureExecTime(
		fmt.Sprintf("processing with batch size %d", config.BatchSize),
		func() {
			var wgChunks sync.WaitGroup
			var wgBatches sync.WaitGroup

			wgChunks.Add(1)
			go processChunks(&wgChunks) // Layer 2

			wgBatches.Add(1)
			go processBatches(&wgBatches, transformer) // Layer 3

			wgChunks.Wait()
			close(batches)
			wgBatches.Wait()
		})
}

func processChunks(wg *sync.WaitGroup) {
	defer wg.Done()

	for chunk := range chunks {
		wg.Add(1)

		go func(chunk *os.File, wg *sync.WaitGroup) {
			defer wg.Done()
			internal.ParseChunk(
				chunk,
				batches,
				config.Schema,
				config.BatchSize,
				config.Delimiter,
			)
		}(chunk, wg)
	}
}

func processBatches(wg *sync.WaitGroup, transformer internal.Transformer) {
	defer wg.Done()

	for records := range batches {
		wg.Add(1)

		go func(records [][]string, wg *sync.WaitGroup) {
			defer wg.Done()
			transformer.Transform(records)
		}(records, wg)
	}
}
