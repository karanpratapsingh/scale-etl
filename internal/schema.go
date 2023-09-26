package internal

import mapset "github.com/deckarep/golang-set/v2"

type Schema struct {
	TableName string
	Key       string
	Header    mapset.Set[string]
	Fields    []string
	Types     map[string]string
}

func (s *Schema) UnmarshalYAML(unmarshal func(any) error) error {
	var yamlSchema struct { // YAML schema
		TableName string              `yaml:"table_name"`
		Key       string              `yaml:"key"`
		Fields    []map[string]string `yaml:"fields"`
	}

	if err := unmarshal(&yamlSchema); err != nil {
		return err
	}

	s.TableName = yamlSchema.TableName
	s.Key = yamlSchema.Key
	s.Types = make(map[string]string)

	for _, fieldMap := range yamlSchema.Fields {
		for fieldName, fieldType := range fieldMap {
			s.Fields = append(s.Fields, fieldName)
			s.Types[fieldName] = fieldType
		}
	}

	s.Header = mapset.NewSet(s.Fields...)

	return nil
}
