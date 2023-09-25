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

	partitions, totalPartitions := fs.PartitionFile(config.PartitionSize, config.BatchSize)

	internal.MeasureExecTime(
		"finished",
		func() {
			processPartitions(partitions, totalPartitions, config.BatchSize, processor) // Layer 2
		})
}

func processPartitions(partitions chan string, totalPartitions, batchSize int, processor internal.Processor) {
	var wg sync.WaitGroup

	N := totalPartitions

	total := 0
	for i := 0; i < N; i += batchSize {
		end := i + batchSize
		if end > N {
			end = N
		}

		batchNo := i / batchSize

		fmt.Println("processing batch", batchNo)
		for j := i; j < end; j += 1 {
			partition := <-partitions

			wg.Add(1)
			go func(wg *sync.WaitGroup, partition string) {
				defer wg.Done()
				total += 1
				processor.ProcessPartition(
					wg,
					partition,
					config.Schema,
					config.SegmentSize,
					config.Delimiter,
				)
			}(&wg, partition)
		}

		wg.Wait()
		fmt.Println("completed batch", batchNo)
	}
}
