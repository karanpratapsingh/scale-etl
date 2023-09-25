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

var partitions = make(chan *os.File)
var processed = make(chan struct{}) // TODO: do we need this?

func main() {
	var transformer = internal.NewTransformer(config.TransformType, config.FilePath, config.OutputDir)

	dirPath, partitionBatches := internal.PartitionFile(config.FilePath, config.PartitionDir, config.PartitionSize, config.BatchSize)

	go internal.MeasureExecTime("reading partitions", func() {
		internal.ReadPartitionBatches(dirPath, partitionBatches, partitions, processed) // Layer 1
	})

	internal.MeasureExecTime(
		fmt.Sprintf("processing with batch size %d", config.BatchSize),
		func() {
			var wgPartition sync.WaitGroup
			processPartitions(&wgPartition, partitionBatches, transformer) // Layer 2
		})
}

func processPartitions(wg *sync.WaitGroup, partitionBatches [][]fs.DirEntry, transformer internal.Transformer) {
	for i, batch := range partitionBatches {
		for range batch {
			partition := <-partitions
			wg.Add(1)

			go func(wg *sync.WaitGroup, partition *os.File) {
				defer wg.Done()

				internal.ProcessPartition(
					wg,
					partition,
					config.Schema,
					config.SegmentSize,
					config.Delimiter,
					transformer,
				)
			}(wg, partition)
		}

		fmt.Println(batch)
		wg.Wait()
		fmt.Println("processed batch", i+1)
		processed <- struct{}{}
	}
}
