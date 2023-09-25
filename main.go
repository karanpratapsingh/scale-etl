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

var chunks = make(chan *os.File)
var processed = make(chan struct{}) // Signal

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
			processChunks(&wgChunks, chunksBuffers, transformer) // Layer 2
		})
}

func processChunks(wg *sync.WaitGroup, chunksBuffers [][]fs.DirEntry, transformer internal.Transformer) {
	for i, chunksBuffer := range chunksBuffers {
		for range chunksBuffer {
			chunk := <-chunks
			wg.Add(1)

			go func(chunk *os.File, wg *sync.WaitGroup) {
				defer wg.Done()

				internal.ParseChunk(
					wg,
					chunk,
					config.Schema,
					config.BatchSize,
					config.Delimiter,
					transformer,
				)
			}(chunk, wg)
		}

		wg.Wait()
		fmt.Println("processed batch", i+1)
		processed <- struct{}{}
	}
}
