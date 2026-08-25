package hatSchema

import (
	"bytes"
	"fmt"
	"go/format"
	"sort"
	"strings"
	"unicode"
)

// ModelOptions controls deterministic Go source generation from a Schema.
type ModelOptions struct {
	Package   string
	TypeNames map[string]string
}

// GenerateGoModels emits one formatted Go file containing a model for every
// schema source in stable source-name order.
func GenerateGoModels(schema Schema, options ModelOptions) ([]byte, error) {
	packageName := strings.TrimSpace(options.Package)
	if !isGoIdentifier(packageName) {
		return nil, fmt.Errorf("hatSchema: invalid Go package name %q", packageName)
	}
	sourceNames := make([]string, 0, len(schema.Sources))
	for name := range schema.Sources {
		sourceNames = append(sourceNames, name)
	}
	sort.Strings(sourceNames)

	var body bytes.Buffer
	imports := make(map[string]struct{})
	for _, name := range sourceNames {
		source := schema.Sources[name]
		if source.Name == "" {
			source.Name = name
		}
		if err := validateSource(source); err != nil {
			return nil, err
		}
		typeName := options.TypeNames[name]
		if strings.TrimSpace(typeName) == "" {
			typeName = exportedIdentifier(source.Name)
		}
		if !isGoIdentifier(typeName) || !unicode.IsUpper([]rune(typeName)[0]) {
			return nil, fmt.Errorf("hatSchema: invalid exported model name %q", typeName)
		}
		if err := writeModel(&body, source, typeName, imports); err != nil {
			return nil, err
		}
	}

	var output bytes.Buffer
	fmt.Fprintf(&output, "package %s\n", packageName)
	if len(imports) > 0 {
		paths := make([]string, 0, len(imports))
		for path := range imports {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		output.WriteString("\nimport (\n")
		for _, path := range paths {
			fmt.Fprintf(&output, "\t%q\n", path)
		}
		output.WriteString(")\n")
	}
	output.WriteByte('\n')
	output.Write(body.Bytes())
	formatted, err := format.Source(output.Bytes())
	if err != nil {
		return nil, fmt.Errorf("hatSchema: format generated model: %w", err)
	}
	return formatted, nil
}

func writeModel(output *bytes.Buffer, source Source, typeName string, imports map[string]struct{}) error {
	fmt.Fprintf(output, "type %s struct {\n", typeName)
	fields := make(map[string]struct{}, len(source.Columns))
	for _, column := range source.Columns {
		field := exportedIdentifier(column.Name)
		if !isGoIdentifier(field) {
			return fmt.Errorf("hatSchema: column %q has no valid Go field name", column.Name)
		}
		if _, exists := fields[field]; exists {
			return fmt.Errorf("hatSchema: columns in source %q collide as Go field %q", source.Name, field)
		}
		fields[field] = struct{}{}
		goType, importPath, err := modelGoType(column.Type)
		if err != nil {
			return err
		}
		if importPath != "" {
			imports[importPath] = struct{}{}
		}
		if !column.NotNull {
			goType = "*" + goType
		}
		fmt.Fprintf(output, "\t%s %s `json:%q`\n", field, goType, column.Name)
	}
	output.WriteString("}\n\n")
	return nil
}

func modelGoType(columnType Type) (string, string, error) {
	switch columnType {
	case TypeText:
		return "string", "", nil
	case TypeNumber:
		return "float64", "", nil
	case TypeInteger:
		return "int64", "", nil
	case TypeDecimal:
		return "hatSql.SQLDecimal", "hatrie_cache/hat/hatSql", nil
	case TypeBoolean:
		return "bool", "", nil
	case TypeDate, TypeTimestamp:
		return "time.Time", "time", nil
	case TypeUUID:
		return "hatSql.SQLUUID", "hatrie_cache/hat/hatSql", nil
	case TypeDuration:
		return "time.Duration", "time", nil
	case TypeBinary:
		return "[]byte", "", nil
	case TypeJSON:
		return "any", "", nil
	default:
		return "", "", fmt.Errorf("hatSchema: unsupported column type %q", columnType)
	}
}

func exportedIdentifier(value string) string {
	var out strings.Builder
	uppercase := true
	for _, runeValue := range value {
		if !unicode.IsLetter(runeValue) && !unicode.IsDigit(runeValue) {
			uppercase = true
			continue
		}
		if out.Len() == 0 && unicode.IsDigit(runeValue) {
			out.WriteByte('X')
		}
		if uppercase {
			out.WriteRune(unicode.ToUpper(runeValue))
			uppercase = false
			continue
		}
		out.WriteRune(runeValue)
	}
	return out.String()
}

func isGoIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for index, runeValue := range value {
		if index == 0 {
			if runeValue != '_' && !unicode.IsLetter(runeValue) {
				return false
			}
			continue
		}
		if runeValue != '_' && !unicode.IsLetter(runeValue) && !unicode.IsDigit(runeValue) {
			return false
		}
	}
	return true
}
