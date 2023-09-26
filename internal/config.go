package internal

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type TransformType string

const (
	TransformTypeDynamoDB TransformType = "dynamodb"
	TransformTypeParquet  TransformType = "parquet"
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
	file, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("error reading yaml file: %v\n", err))
	}

	var config Config

	// Set defaults
	config.Delimiter = ','
	config.BatchSize = 5
	config.PartitionDir = "partitions"
	config.OutputDir = "output"

	if err = yaml.Unmarshal(file, &config); err != nil {
		panic(fmt.Sprintf("error unmarshalling yaml: %v\n", err))
	}

	if config.BatchSize < 1 {
		panic("batch size cannot be less than 1")
	}

	if config.SegmentSize > config.PartitionSize {
		panic("segment size should be less than partition size")
	}

	if len(config.Schema.Fields) == 0 {
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

	fmt.Println("loaded config from", path)
	return config
}
