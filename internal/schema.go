package internal

import (
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
		panic(ErrFileNotFound(schemaPath))
	}

	var schema Schema

	var yamlSchema struct {
		TableName string              `yaml:"table_name,omitempty"`
		Key       string              `yaml:"key,omitempty"`
		Columns   []map[string]string `yaml:"columns"`
	}

	file, err := os.ReadFile(schemaPath)
	if err != nil {
		panic(ErrReadingFile(err))
	}

	if err := yaml.Unmarshal(file, &yamlSchema); err != nil {
		panic(err)
	}

	if len(yamlSchema.Columns) == 0 {
		panic(ErrSchemaRequired)
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
