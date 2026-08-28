package hatSql

import (
	"fmt"
	"strings"
)

// NamedQueryTemplate is a reusable SQL statement with :name placeholders.
type NamedQueryTemplate struct{ Name, Source string }

func NewNamedQueryTemplate(name, source string) (NamedQueryTemplate, error) {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(source) == "" {
		return NamedQueryTemplate{}, fmt.Errorf("query template name and source are required")
	}
	return NamedQueryTemplate{Name: strings.TrimSpace(name), Source: source}, nil
}

// Bind replaces named placeholders with positional parameters. Placeholders in
// quoted literals are left unchanged and repeated names reuse one position.
func (template NamedQueryTemplate) Bind(values map[string]interface{}) (string, []interface{}, error) {
	positions := map[string]int{}
	parameters := make([]interface{}, 0)
	var output strings.Builder
	inQuote := false
	for index := 0; index < len(template.Source); index++ {
		current := template.Source[index]
		if current == '\'' {
			output.WriteByte(current)
			if inQuote && index+1 < len(template.Source) && template.Source[index+1] == '\'' {
				output.WriteByte('\'')
				index++
				continue
			}
			inQuote = !inQuote
			continue
		}
		if !inQuote && current == ':' && index+1 < len(template.Source) && namedTemplateStart(template.Source[index+1]) {
			end := index + 2
			for end < len(template.Source) && namedTemplatePart(template.Source[end]) {
				end++
			}
			name := template.Source[index+1 : end]
			position, exists := positions[name]
			if !exists {
				value, ok := values[name]
				if !ok {
					return "", nil, fmt.Errorf("query template %q parameter %q is required", template.Name, name)
				}
				parameters = append(parameters, value)
				position = len(parameters)
				positions[name] = position
			}
			fmt.Fprintf(&output, "$%d", position)
			index = end - 1
			continue
		}
		output.WriteByte(current)
	}
	return output.String(), parameters, nil
}
func namedTemplateStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}
func namedTemplatePart(value byte) bool {
	return namedTemplateStart(value) || value >= '0' && value <= '9'
}

func RequireNonEmpty(result QueryResult) error {
	if len(result.Rows) == 0 {
		return fmt.Errorf("query result must be non-empty")
	}
	return nil
}
func RequireExactlyOne(result QueryResult) (Row, error) {
	if len(result.Rows) != 1 {
		return nil, fmt.Errorf("query result must contain exactly one row, got %d", len(result.Rows))
	}
	return result.Rows[0], nil
}
