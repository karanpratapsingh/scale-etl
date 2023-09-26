package main

import (
	"csv-ingest/internal"
	"fmt"
	"sync"
)

func main() {
	defer internal.HandlePanic()

	var args internal.Args = internal.ParseArgs()
	var config internal.Config = internal.NewConfig(args.ConfigPath)

	var fs = internal.NewFS(config.FilePath, config.PartitionDir, config.OutputDir)
	totalPartitions, totalBatches, partitions := fs.PartitionFile(config.PartitionSize, config.BatchSize)

	var transformer = internal.NewTransformer(fs, config.TransformType, config.Schema, totalBatches)
	var processor = internal.NewProcessor(fs, transformer)

	internal.MeasureExecTime("Processing complete", func() {
		processPartitions(totalPartitions, partitions, config, processor) // Layer 2
	})
}

func processPartitions(totalPartitions int, partitions chan string, config internal.Config, processor internal.Processor) {
	var wg sync.WaitGroup
	batchSize := config.BatchSize

	for i := 0; i < totalPartitions; i += batchSize {
		batchNo := internal.CountBatches(i, batchSize) + 1
		end := min(totalPartitions, i+batchSize) // Last batch can be less than batchSize
		batchItemCount := end - i

		internal.MeasureExecTime(fmt.Sprintf("Processed batch %d with %d partition(s)", batchNo, batchItemCount), func() {
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
