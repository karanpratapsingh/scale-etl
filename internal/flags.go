package internal

import "github.com/urfave/cli/v2"

func PartitionerFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:     "file-path",
			Required: true,
			Usage:    "File path",
		},
		&cli.StringFlag{
			Name:  "partition-dir",
			Value: "partitions",
			Usage: "Partition output dir",
		},
	}
}

func PartitionSizeFlag() *cli.IntFlag {
	return &cli.IntFlag{
		Name:     "partition-size",
		Required: true,
		Usage:    "Partition size",
	}
}

func BatchAndSegmentSizeFlags() []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{
			Name:  "batch-size",
			Value: 5,
			Usage: "Batch size",
			Action: func(_ *cli.Context, batchSize int) error {
				if batchSize < 1 {
					return ErrInsufficientBatchSize
				}

				return nil
			},
		},
		&cli.IntFlag{
			Name:     "segment-size",
			Required: true,
			Usage:    "Segment size",
		},
	}
}

func SchemaPathFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  "schema-path",
		Value: "schema.yaml",
		Usage: "Schema file path",
	}
}

func SearchFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:     "pattern",
			Required: true,
			Usage:    "Search pattern",
		},
		&cli.StringFlag{
			Name:  "output",
			Value: "matches.csv",
			Usage: "Output file path",
		},
	}
}

func TransformTypeFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  "transform-type",
		Value: "csv",
		Usage: "Transform type",
	}
}

func DelimiterFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  "delimiter",
		Value: ",",
		Usage: "Delimiter",
	}
}

func OutputDirFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  "output-dir",
		Value: "output",
		Usage: "Output dir",
	}
}
