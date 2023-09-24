package internal

import mapset "github.com/deckarep/golang-set/v2"

type Schema struct {
	Header mapset.Set[string]
	Fields []string
	Types  map[string]string
}

func (s *Schema) UnmarshalYAML(unmarshal func(any) error) error {
	var schema []map[string]string

	if err := unmarshal(&schema); err != nil {
		return err
	}

	s.Types = make(map[string]string)

	for _, fieldMap := range schema {
		for fieldValue, fieldType := range fieldMap {
			s.Fields = append(s.Fields, fieldValue)
			s.Types[fieldValue] = fieldType
		}
	}

	s.Header = mapset.NewSet(s.Fields...)

	return nil
}
