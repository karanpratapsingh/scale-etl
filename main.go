package main

import (
	"csv-ingest/internal"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	defer internal.HandlePanic()

	var args internal.Args = internal.ParseArgs()
	var config internal.Config = internal.NewConfig(args.ConfigPath)

	var fs = internal.NewFS(config.FilePath, config.PartitionDir, config.OutputDir)

	app := &cli.App{
		Name:  "csv-etl",
		Usage: "csv etl",
		Commands: []*cli.Command{
			{
				Name:  "partitions",
				Usage: "partition a csv file",
				Action: func(*cli.Context) error {
					fs.PartitionFile(config.PartitionSize)

					return nil
				},
			},
			{
				Name:  "transform",
				Usage: "transform a csv file",
				Action: func(*cli.Context) error {
					totalPartitions, totalBatches, partitions := fs.LoadPartitions(config.PartitionSize, config.BatchSize)
					var processor = internal.NewProcessor(fs, config.Schema, config.BatchSize, config.SegmentSize, config.Delimiter)
					var transformer = internal.NewTransformer(fs, config.TransformType, config.Schema, totalBatches)

					processor.ProcessPartitions(totalPartitions, partitions, transformer.Transform)
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}
