package internal

import mapset "github.com/deckarep/golang-set/v2"

type Schema struct {
	TableName string
	Key       string
	Header    mapset.Set[string]
	Columns   []string
	Types     map[string]string
}

func (s *Schema) UnmarshalYAML(unmarshal func(any) error) error {
	var yamlSchema struct { // YAML schema
		TableName string              `yaml:"table_name"`
		Key       string              `yaml:"key"`
		Columns   []map[string]string `yaml:"columns"`
	}

	if err := unmarshal(&yamlSchema); err != nil {
		return err
	}

	s.TableName = yamlSchema.TableName
	s.Key = yamlSchema.Key
	s.Types = make(map[string]string)

	for _, columnMap := range yamlSchema.Columns {
		for columnName, columnType := range columnMap {
			s.Columns = append(s.Columns, columnName)
			s.Types[columnName] = columnType
		}
	}

	s.Header = mapset.NewSet(s.Columns...)

	return nil
}
