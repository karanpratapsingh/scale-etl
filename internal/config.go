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
	Delimiter     rune          `yaml:"delimiter,omitempty"`
	ChunkSize     int           `yaml:"chunk_size"`
	BatchSize     int           `yaml:"batch_size"`
	BufferSize    int           `yaml:"buffer_size,omitempty"`
	ProcessDir    string        `yaml:"process_dir,omitempty"`
	OutputDir     string        `yaml:"output_dir,omitempty"`
}

// TODO: schema
func NewConfig(path string) Config {
	file, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("error reading yaml file: %v\n", err))
	}

	var config Config

	// Set defaults
	config.Delimiter = ','
	config.BufferSize = 20
	config.ProcessDir = "chunks"
	config.OutputDir = "output"

	err = yaml.Unmarshal(file, &config)
	if err != nil {
		panic(fmt.Sprintf("Error unmarshalling YAML: %v\n", err))
	}

	fmt.Println("loaded config from", path)
	return config
}
