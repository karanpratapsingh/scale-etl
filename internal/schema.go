package internal

import (
	"fmt"
	"os"

	mapset "github.com/deckarep/golang-set/v2"
	"gopkg.in/yaml.v2"
)

type Schema struct {
	TableName string
	Key       string
	Header    mapset.Set[string]
	Columns   []string
	Types     map[string]string
}

func NewSchema(schemaPath string) Schema {
	if !pathExists(schemaPath) {
		panic(fmt.Sprintf("schema file %s doesn't exist", schemaPath))
	}

	var schema Schema

	var yamlSchema struct { // YAML schema
		TableName string              `yaml:"table_name,omitempty"`
		Key       string              `yaml:"key,omitempty"`
		Columns   []map[string]string `yaml:"columns"`
	}

	file, err := os.ReadFile(schemaPath)
	if err != nil {
		panic(fmt.Sprintf("error reading config file: %v\n", err))
	}

	if err := yaml.Unmarshal(file, &yamlSchema); err != nil {
		panic(fmt.Errorf("error unmarshalling yaml: %v", err))
	}

	if len(yamlSchema.Columns) == 0 {
		panic("schema definition is required")
	}

	schema.TableName = yamlSchema.TableName
	schema.Key = yamlSchema.Key
	schema.Types = make(map[string]string)

	for _, columnMap := range yamlSchema.Columns {
		for columnName, columnType := range columnMap {
			schema.Columns = append(schema.Columns, columnName)
			schema.Types[columnName] = columnType
		}
	}

	schema.Header = mapset.NewSet(schema.Columns...)

	return schema
}
