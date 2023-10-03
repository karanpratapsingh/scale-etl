package internal

import "github.com/urfave/cli/v2"

var PartitionCommandFlags = append(partitionerFlags, partitionSizeFlag)

var TransformCommandFlags = append(
	transformSearchCommonFlags,
	transformTypeFlag,
	outputDirFlag,
)

var SearchCommandFlags = ConcatenateArrays(
	searchFlags,
	transformSearchCommonFlags,
)

var LoaderCommandFlags = []cli.Flag{
	filePathFlag,
	outputDirFlag,
	poolSizeFlag,
	commandFlag,
}

var CleanCommandFlags = partitionerFlags

var transformSearchCommonFlags = append(
	ConcatenateArrays(
		partitionerFlags,
		batchAndSegmentSizeFlags,
	),
	schemaPathFlag,
	delimiterFlag,
)

var partitionerFlags = []cli.Flag{
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

var filePathFlag = &cli.StringFlag{
	Name:     "file-path",
	Required: true,
	Usage:    "File path",
}

var partitionSizeFlag = &cli.IntFlag{
	Name:     "partition-size",
	Required: true,
	Usage:    "Partition size",
	Action: func(_ *cli.Context, partitionSize int) error {
		if partitionSize < 1 {
			return ErrInsufficientPartitionSize
		}

		return nil
	},
}

var batchAndSegmentSizeFlags = []cli.Flag{
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
		Action: func(_ *cli.Context, segmentSize int) error {
			if segmentSize < 1 {
				return ErrInsufficientSegmentSize
			}

			return nil
		},
	},
}

var schemaPathFlag = &cli.StringFlag{
	Name:  "schema-path",
	Value: "schema.yaml",
	Usage: "Schema file path",
}

var searchFlags = []cli.Flag{
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

var transformTypeFlag = &cli.StringFlag{
	Name:  "transform-type",
	Value: "csv",
	Usage: "Transform type",
}

var delimiterFlag = &cli.StringFlag{
	Name:  "delimiter",
	Value: ",",
	Usage: "Delimiter",
}

var poolSizeFlag = &cli.IntFlag{
	Name:     "pool-size",
	Required: true,
	Usage:    "Request pool size",
}

var commandFlag = &cli.StringFlag{
	Name:     "command",
	Required: true,
	Usage:    "command executed for each segment",
}

var outputDirFlag = &cli.StringFlag{
	Name:  "output-dir",
	Value: "output",
	Usage: "Output dir",
}
