package main

import (
	"csv-ingest/internal"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	defer internal.HandlePanic()

	app := &cli.App{
		Name:        "scale-etl",
		Usage:       "Partition, Transform, Load, and Search large CSV files",
		Description: internal.Description,
		Commands: []*cli.Command{
			{
				Name:  "partition",
				Usage: "Partition input CSV file into multiple smaller files",
				Flags: internal.PartitionCommandFlags,
				Action: func(ctx *cli.Context) error {
					filePath := ctx.String("file-path")
					partitionDir := ctx.String("partition-dir")
					partitionSize := ctx.Int("partition-size")

					var partitioner = internal.NewPartitioner(filePath, partitionDir)

					return partitioner.PartitionFile(partitionSize)
				},
			},
			{
				Name:  "transform",
				Usage: "Transform partitions into a particular format",
				Flags: internal.TransformCommandFlags,
				Action: func(ctx *cli.Context) error {
					filePath := ctx.String("file-path")
					partitionDir := ctx.String("partition-dir")
					batchSize := ctx.Int("batch-size")
					segmentSize := ctx.Int("segment-size")
					transformType := internal.TransformType(ctx.String("transform-type"))
					outputDir := ctx.String("output-dir")
					schemaPath := ctx.String("schema-path")
					delimiter := internal.ParseRune(ctx.String("delimiter"))
					noHeader := ctx.Bool("no-header")

					var partitioner = internal.NewPartitioner(filePath, partitionDir)

					partitionManifest := partitioner.GetPartitionManifest()
					internal.PrintInputFileInfo(filePath, partitionManifest.TotalRows)

					var schema = internal.NewSchema(schemaPath)

					internal.PrintSchemaInfo(schema)
					internal.PrintTransformInfo(schema, transformType, delimiter)

					if err := internal.CheckTransformType(transformType, schema); err != nil {
						return err
					}

					var output = internal.NewOutput(filePath, outputDir)

					partitions, totalPartitions := partitioner.StreamPartitions()
					if err := internal.CheckBatchSize(batchSize, totalPartitions); err != nil {
						return err
					}

					var processor = internal.NewProcessor(partitioner, schema, batchSize, segmentSize, delimiter, noHeader)

					totalBatches := processor.CountBatches(totalPartitions)
					internal.PrintBatchInfo(totalBatches, batchSize)
					output.PrepareOutputDirs(totalBatches)

					internal.PrintSegmentInfo(segmentSize)

					var transformer = internal.NewTransformer(output, internal.TransformType(transformType), schema)
					processor.ProcessPartitions(partitions, totalPartitions, transformer)

					return nil
				},
			},
			{
				Name:  "search",
				Usage: "Searches partitions for a specific pattern",
				Flags: internal.SearchCommandFlags,
				Action: func(ctx *cli.Context) error {
					pattern := ctx.String("pattern")
					outputPath := ctx.String("output")
					filePath := ctx.String("file-path")
					partitionDir := ctx.String("partition-dir")
					batchSize := ctx.Int("batch-size")
					segmentSize := ctx.Int("segment-size")
					schemaPath := ctx.String("schema-path")
					delimiter := internal.ParseRune(ctx.String("delimiter"))
					noHeader := ctx.Bool("no-header")

					var schema = internal.NewSchema(schemaPath)

					var partitioner = internal.NewPartitioner(filePath, partitionDir)
					partitions, totalPartitions := partitioner.StreamPartitions()

					var processor = internal.NewProcessor(partitioner, schema, batchSize, segmentSize, delimiter, noHeader)

					totalBatches := processor.CountBatches(totalPartitions)
					internal.PrintBatchInfo(totalBatches, batchSize)
					internal.PrintSegmentInfo(segmentSize)

					var searcher = internal.NewSearcher(schema, pattern, outputPath)

					processor.ProcessPartitions(partitions, totalPartitions, searcher)
					return searcher.Cleanup()
				},
			},
			{
				Name:  "load",
				Usage: "Load transformed segments concurrently",
				Flags: internal.LoaderCommandFlags,
				Action: func(ctx *cli.Context) error {
					filePath := ctx.String("file-path")
					outputDir := ctx.String("output-dir")
					poolSize := ctx.Int("pool-size")
					scriptPath := ctx.String("script-path")

					var loader = internal.NewLoader(filePath, scriptPath, outputDir)

					return loader.LoadSegments(poolSize)
				},
			},
			{
				Name:  "clean",
				Usage: "Clean partitions file info",
				Flags: internal.CleanCommandFlags,
				Action: func(ctx *cli.Context) error {
					filePath := ctx.String("file-path")
					partitionDir := ctx.String("partition-dir")

					var partitioner = internal.NewPartitioner(filePath, partitionDir)

					return partitioner.CleanPartitions()
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
