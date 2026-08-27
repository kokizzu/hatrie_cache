package hatSql

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed fixtures/*.json
var embeddedQueryFixtureFiles embed.FS

// QueryFixture is a self-contained query report. Sources and expected rows use
// JSON-shaped values so fixtures can be attached to issues and replayed by any
// SQL client without Go-specific encoding.
type QueryFixture struct {
	Name            string                              `json:"name"`
	Query           string                              `json:"query"`
	Parameters      []interface{}                       `json:"parameters,omitempty"`
	Sources         map[string][]map[string]interface{} `json:"sources"`
	ExpectedColumns []string                            `json:"expected_columns,omitempty"`
	ExpectedRows    []map[string]interface{}            `json:"expected_rows,omitempty"`
	Notes           string                              `json:"notes,omitempty"`
}

// EmbeddedQueryFixtures loads and validates the checked-in reproducible query
// fixtures in deterministic name order.
func EmbeddedQueryFixtures() ([]QueryFixture, error) {
	entries, err := fs.ReadDir(embeddedQueryFixtureFiles, "fixtures")
	if err != nil {
		return nil, err
	}
	fixtures := make([]QueryFixture, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := embeddedQueryFixtureFiles.ReadFile("fixtures/" + entry.Name())
		if err != nil {
			return nil, err
		}
		var fixture QueryFixture
		if err := json.Unmarshal(data, &fixture); err != nil {
			return nil, fmt.Errorf("decode query fixture %s: %w", entry.Name(), err)
		}
		if err := validateQueryFixture(fixture); err != nil {
			return nil, fmt.Errorf("query fixture %s: %w", entry.Name(), err)
		}
		if _, exists := seen[fixture.Name]; exists {
			return nil, fmt.Errorf("duplicate query fixture name %q", fixture.Name)
		}
		seen[fixture.Name] = struct{}{}
		fixtures = append(fixtures, fixture)
	}
	sort.Slice(fixtures, func(left, right int) bool { return fixtures[left].Name < fixtures[right].Name })
	return fixtures, nil
}

// EmbeddedQueryFixture returns a named checked-in query fixture.
func EmbeddedQueryFixture(name string) (QueryFixture, bool) {
	fixtures, err := EmbeddedQueryFixtures()
	if err != nil {
		return QueryFixture{}, false
	}
	for _, fixture := range fixtures {
		if fixture.Name == name {
			return fixture, true
		}
	}
	return QueryFixture{}, false
}

func validateQueryFixture(fixture QueryFixture) error {
	if strings.TrimSpace(fixture.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if strings.TrimSpace(fixture.Query) == "" {
		return fmt.Errorf("query is required")
	}
	if err := ValidateSQLQuery(fixture.Query); err != nil {
		return err
	}
	if len(fixture.Sources) == 0 {
		return fmt.Errorf("at least one source is required")
	}
	for name, rows := range fixture.Sources {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("source name is required")
		}
		if rows == nil {
			return fmt.Errorf("source %q rows must be an array", name)
		}
	}
	return nil
}
