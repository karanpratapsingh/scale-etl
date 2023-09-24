package internal

import mapset "github.com/deckarep/golang-set/v2"

type Schema struct {
	Header mapset.Set[string]
	Value  map[string]any
}

func (s *Schema) UnmarshalYAML(unmarshal func(any) error) error {
	if err := unmarshal(&s.Value); err != nil {
		return err
	}

	s.Header = mapset.NewSetFromMapKeys(s.Value)

	return nil
}
