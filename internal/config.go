package internal

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	FilePath      string `yaml:"file_path"`
	TransformType string `yaml:"transform_type"`
	Delimiter     string `yaml:"delimiter,omitempty"`
	ChunkSize     int    `yaml:"chunk_size"`
	BatchSize     int    `yaml:"batch_size"`
}

func NewConfig(path string) Config {
	file, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("error reading yaml file: %v\n", err))
	}

	var config Config
	config.Delimiter = "," // Default delimiter is comma

	err = yaml.Unmarshal(file, &config)
	if err != nil {
		panic(fmt.Sprintf("Error unmarshalling YAML: %v\n", err))
	}

	fmt.Println("loaded config from", path)
	return config
}
