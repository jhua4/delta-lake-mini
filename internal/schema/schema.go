// Package schema handles table schema storage and enforcement.
package schema

import (
	"encoding/json"
	"fmt"
)

// Field represents a single column in the table schema.
type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`     // e.g. "int64", "float64", "string", "bool"
	Nullable bool   `json:"nullable"`
}

// Schema is an ordered list of fields.
type Schema struct {
	Fields []Field `json:"fields"`
}

// Marshal serialises the schema to a JSON string for storage in the log.
func (s *Schema) Marshal() (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Unmarshal parses a schema from its JSON string representation.
func Unmarshal(raw string) (*Schema, error) {
	var s Schema
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}
	return &s, nil
}

// Validate checks that every field in incoming matches the table schema.
// It returns a descriptive error if:
//   - a required column is missing from incoming
//   - an incoming column has a different type than declared
//   - a non-nullable column is absent (TODO: extend with null-value checking)
func (s *Schema) Validate(incoming *Schema) error {
	declared := make(map[string]Field, len(s.Fields))
	for _, f := range s.Fields {
		declared[f.Name] = f
	}

	// Check each incoming field against the declared schema.
	for _, inf := range incoming.Fields {
		decl, ok := declared[inf.Name]
		if !ok {
			return fmt.Errorf("column %q is not in table schema", inf.Name)
		}
		if decl.Type != inf.Type {
			return fmt.Errorf("column %q: expected type %q, got %q", inf.Name, decl.Type, inf.Type)
		}
	}

	// Check that every non-nullable declared column is present in incoming.
	incomingCols := make(map[string]bool, len(incoming.Fields))
	for _, f := range incoming.Fields {
		incomingCols[f.Name] = true
	}
	for _, decl := range s.Fields {
		if !decl.Nullable && !incomingCols[decl.Name] {
			return fmt.Errorf("non-nullable column %q is missing from write", decl.Name)
		}
	}

	return nil
}

// Equal returns true if both schemas have identical fields in the same order.
func (s *Schema) Equal(other *Schema) bool {
	if len(s.Fields) != len(other.Fields) {
		return false
	}
	for i := range s.Fields {
		if s.Fields[i] != other.Fields[i] {
			return false
		}
	}
	return true
}
