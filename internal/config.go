package internal

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	FilePath      string        `yaml:"file_path"`
	TransformType TransformType `yaml:"transform_type"`
	BatchSize     int           `yaml:"batch_size,omitempty"`
	PartitionSize int           `yaml:"partition_size"`
	SegmentSize   int           `yaml:"segment_size"`
	Schema        Schema        `yaml:"schema"`
	PartitionDir  string        `yaml:"partition_dir,omitempty"`
	OutputDir     string        `yaml:"output_dir,omitempty"`
	Delimiter     rune          `yaml:"delimiter,omitempty"`
}

func NewConfig(path string) Config {
	var config Config

	// Set defaults
	config.Delimiter = ','
	config.BatchSize = 5
	config.PartitionDir = "partitions"
	config.OutputDir = "output"

	file, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("error reading config file: %v\n", err))
	}

	if err = yaml.Unmarshal(file, &config); err != nil {
		panic(fmt.Errorf("error unmarshalling yaml: %v", err))
	}

	if !pathExists(config.FilePath) {
		panic(fmt.Sprintf("file %s doesn't exist", config.FilePath))
	}

	if config.BatchSize < 1 {
		panic("batch size cannot be less than 1")
	}

	if config.SegmentSize > config.PartitionSize {
		panic(fmt.Sprintf("segment size (%d) should be less than or equal to partition size (%d)", config.SegmentSize, config.PartitionSize))
	}

	if len(config.Schema.Columns) == 0 {
		panic("schema definition is required")
	}

	if config.TransformType == TransformTypeDynamoDB {
		if config.Schema.TableName == "" {
			panic("table name is required for transform type dynamodb")
		}

		if config.Schema.Key == "" {
			panic("key is required for transform type dynamodb")
		}
	}

	totalRows := countFileRows(config.FilePath)

	if config.PartitionSize > totalRows {
		panic(fmt.Sprintf("partition size (%d) should be less than or equal to total number of rows (%d)", config.PartitionSize, totalRows))
	}

	printInputFileInfo(config.FilePath, totalRows, config.Delimiter)
	return config
}
