package main

import (
	"csv-ingest/internal"
	"fmt"
	"sync"
)

var args internal.Args = internal.ParseArgs()
var config internal.Config = internal.NewConfig(args.ConfigPath)

func main() {
	var fs = internal.NewFS(config.FilePath, config.PartitionDir, config.OutputDir)
	var transformer = internal.NewTransformer(config.TransformType, config.FilePath, config.OutputDir)
	var processor = internal.NewProcessor(fs, transformer)

	total, partitions := fs.PartitionFile(config.PartitionSize, config.BatchSize)

	internal.MeasureExecTime("finished", func() {
		processPartitions(total, partitions, config.BatchSize, processor) // Layer 2
	})
}

func processPartitions(n int, partitions chan string, batchSize int, processor internal.Processor) {
	var wg sync.WaitGroup

	for i := 0; i < n; i += batchSize {
		batchNo := i / batchSize
		end := min(n, i+batchSize)

		internal.MeasureExecTime(fmt.Sprintf("processed batch %d", batchNo), func() {
			for j := i; j < end; j += 1 {
				partition := <-partitions

				wg.Add(1)
				go func(wg *sync.WaitGroup, partition string) {
					defer wg.Done()

					processor.ProcessPartition(
						wg,
						partition,
						config.Schema,
						config.SegmentSize,
						config.Delimiter,
					)
				}(&wg, partition)
			}
		})

		wg.Wait()
	}
}
