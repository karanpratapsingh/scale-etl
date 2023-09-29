package main

import (
	"csv-ingest/internal"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	defer internal.HandlePanic()

	var args internal.Args = internal.ParseArgs() // TODO: move to cli framework
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
					return fs.PartitionFile(config.PartitionSize)
				},
			},
			{
				Name:  "transform",
				Usage: "transform a csv file",
				Action: func(*cli.Context) error {
					partitions, totalPartitions, totalBatches := fs.LoadPartitions(config.PartitionSize, config.BatchSize)
					var processor = internal.NewProcessor(fs, config.Schema, config.BatchSize, config.SegmentSize, config.Delimiter)
					var transformer = internal.NewTransformer(fs, config.TransformType, config.Schema, totalBatches)

					processor.ProcessPartitions(totalPartitions, partitions, transformer)
					return nil
				},
			},
			{
				Name:  "search",
				Usage: "search a csv file",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "pattern",
						Required: true,
						Aliases:  []string{"p"},
						Usage:    "Search pattern",
					},
					&cli.StringFlag{
						Name:    "output",
						Value:   "matches.csv",
						Aliases: []string{"o"},
						Usage:   "Output file path",
					},
				},
				Action: func(c *cli.Context) error {
					outputPath := c.String("output")
					pattern := c.String("pattern")

					partitions, totalPartitions, _ := fs.LoadPartitions(config.PartitionSize, config.BatchSize)
					var processor = internal.NewProcessor(fs, config.Schema, config.BatchSize, config.SegmentSize, config.Delimiter)
					var searcher = internal.NewSearcher(fs, config.Schema, pattern, outputPath)

					processor.ProcessPartitions(totalPartitions, partitions, searcher)
					return searcher.Cleanup()
				},
			},
			{
				Name:  "clean",
				Usage: "clean partitions directory",
				Action: func(*cli.Context) error {
					return fs.CleanPartitions()
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}
