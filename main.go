package main

import (
	"csv-ingest/internal"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	defer internal.HandlePanic()

	app := &cli.App{
		Name:        "csv-etl",
		Usage:       "partition, transform, and search large CSV files",
		Description: internal.Description,
		Commands: []*cli.Command{
			{
				Name:  "partition",
				Usage: "partition into multiple smaller files",
				Flags: append(internal.PartitionerFlags(), internal.PartitionSizeFlag()),
				Action: func(ctx *cli.Context) error {
					filePath := ctx.String("file-path")
					partitionDir := ctx.String("partition-dir")
					partitionSize := ctx.Int("partition-size")

					var partitioner = internal.NewPartitioner(filePath, partitionDir)

					totalRows := internal.CountFileRows(filePath)
					if err := internal.CheckPartitionSize(partitionSize, totalRows); err != nil {
						return err
					}

					internal.PrintInputFileInfo(filePath, totalRows)

					return partitioner.PartitionFile(partitionSize)
				},
			},
			{
				Name:  "transform",
				Usage: "transform partitions to a particular format",
				Flags: append(
					internal.ConcatenateArrays(
						internal.PartitionerFlags(),
						internal.BatchAndSegmentSizeFlags(),
					),
					internal.TransformTypeFlag(),
					internal.OutputDirFlag(),
					internal.SchemaPathFlag(),
					internal.DelimiterFlag(),
				),
				Action: func(ctx *cli.Context) error {
					filePath := ctx.String("file-path")
					partitionDir := ctx.String("partition-dir")
					batchSize := ctx.Int("batch-size")
					segmentSize := ctx.Int("segment-size")
					transformType := internal.TransformType(ctx.String("transform-type"))
					outputDir := ctx.String("output-dir")
					schemaPath := ctx.String("schema-path")
					delimiter := []rune(ctx.String("delimiter"))[0]

					var partitioner = internal.NewPartitioner(filePath, partitionDir)

					totalRows := internal.CountFileRows(filePath)
					internal.PrintInputFileInfo(filePath, totalRows)

					var schema = internal.NewSchema(schemaPath)

					internal.PrintSchemaInfo(schema)
					internal.PrintTransformInfo(schema, transformType, delimiter)

					if err := internal.CheckTransformType(transformType, schema); err != nil {
						return err
					}

					var output = internal.NewOutput(filePath, outputDir)

					partitions, totalPartitions := partitioner.LoadPartitions()
					if err := internal.CheckBatchSize(batchSize, totalPartitions); err != nil {
						return err
					}

					var processor = internal.NewProcessor(partitioner, schema, batchSize, segmentSize, delimiter)

					totalBatches := processor.CountBatches(totalPartitions)
					internal.PrintBatchInfo(totalBatches, batchSize)
					output.PrepareOutputDirs(totalBatches)

					var transformer = internal.NewTransformer(output, internal.TransformType(transformType), schema)
					processor.ProcessPartitions(partitions, totalPartitions, transformer)

					return nil
				},
			},
			{
				Name:  "search",
				Usage: "searches partitions for a pattern and outputs results",
				Flags: append(
					internal.ConcatenateArrays(
						internal.SearchFlags(),
						internal.PartitionerFlags(),
						internal.BatchAndSegmentSizeFlags(),
					),
					internal.SchemaPathFlag(),
					internal.DelimiterFlag(),
				),
				Action: func(ctx *cli.Context) error {
					pattern := ctx.String("pattern")
					outputPath := ctx.String("output")
					filePath := ctx.String("file-path")
					partitionDir := ctx.String("partition-dir")
					batchSize := ctx.Int("batch-size")
					segmentSize := ctx.Int("segment-size")
					schemaPath := ctx.String("schema-path")
					delimiter := []rune(ctx.String("delimiter"))[0]

					var schema = internal.NewSchema(schemaPath)

					var partitioner = internal.NewPartitioner(filePath, partitionDir)
					partitions, totalPartitions := partitioner.LoadPartitions()

					var processor = internal.NewProcessor(partitioner, schema, batchSize, segmentSize, delimiter)
					var searcher = internal.NewSearcher(schema, pattern, outputPath)

					processor.ProcessPartitions(partitions, totalPartitions, searcher)
					return searcher.Cleanup()
				},
			},
			{
				Name:  "clean",
				Usage: "clean partitions directory",
				Flags: internal.PartitionerFlags(),
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
		fmt.Println(err)
	}
}
