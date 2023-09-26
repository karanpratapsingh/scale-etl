package main

import (
	"csv-ingest/internal"
	"fmt"
	"sync"
)

var args internal.Args = internal.ParseArgs()
var config internal.Config = internal.NewConfig(args.ConfigPath)

func main() {
	// TODO: global recover
	var fs = internal.NewFS(config.FilePath, config.PartitionDir, config.OutputDir)
	totalPartitions, totalBatches, partitions := fs.PartitionFile(config.PartitionSize, config.BatchSize)

	var transformer = internal.NewTransformer(fs, config.TransformType, config.Schema, totalBatches)
	var processor = internal.NewProcessor(fs, transformer)

	internal.MeasureExecTime("completed", func() {
		processPartitions(totalPartitions, partitions, config.BatchSize, processor) // Layer 2
	})
}

func processPartitions(totalPartitions int, partitions chan string, batchSize int, processor internal.Processor) {
	var wg sync.WaitGroup

	for i := 0; i < totalPartitions; i += batchSize {
		batchNo := internal.CountBatches(i, batchSize) + 1

		end := min(totalPartitions, i+batchSize) // Last batch can be less than batchSize

		internal.MeasureExecTime(fmt.Sprintf("processed batch %d", batchNo), func() {
			for j := i; j < end; j += 1 {
				partition := <-partitions

				wg.Add(1)
				go func(wg *sync.WaitGroup, partition string) {
					defer wg.Done()

					processor.ProcessPartition(
						wg,
						batchNo,
						partition,
						config.Schema,
						config.SegmentSize,
						config.Delimiter,
					)
				}(&wg, partition)
			}
			wg.Wait()
		})
	}
}
