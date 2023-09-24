package main

import (
	"csv-ingest/internal"
	"fmt"
	"io/fs"
	"os"
	"sync"
)

var args internal.Args = internal.ParseArgs()
var config internal.Config = internal.NewConfig(args.ConfigPath)

var chunks = make(chan *os.File) // TODO: rename to queue_size
var batches = make(chan [][]string)
var processed = make(chan struct{})

func main() {
	var transformer = internal.NewTransformer(config.TransformType, config.FilePath, config.OutputDir)

	dirPath, chunksBuffers := internal.SplitInputFile(config.FilePath, config.ProcessDir, config.ChunkSize, config.BufferSize)

	go internal.MeasureExecTime("reading chunks", func() {
		internal.ReadChunksBuffers(dirPath, chunksBuffers, chunks, processed) // Layer 1
	})

	internal.MeasureExecTime(
		fmt.Sprintf("processing with batch size %d", config.BatchSize),
		func() {
			var wgChunks sync.WaitGroup
			var wgBatches sync.WaitGroup

			wgChunks.Add(1)
			go processChunks(&wgChunks, chunksBuffers) // Layer 2

			wgBatches.Add(1)
			go processBatches(&wgBatches, transformer) // Layer 3

			wgChunks.Wait()
			close(batches)
			wgBatches.Wait()
		})
}

func processChunks(wg *sync.WaitGroup, chunksBuffers [][]fs.DirEntry) {
	defer wg.Done()

	for i, chunksBuffer := range chunksBuffers {
		for range chunksBuffer {
			chunk := <-chunks
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
		fmt.Println("completed", i+1)
		processed <- struct{}{}
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
