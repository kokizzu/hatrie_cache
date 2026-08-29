package hatriecache

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestHatrieSQLNotebookIsValidAndReproducible(t *testing.T) {
	data, err := os.ReadFile("notebooks/hatrie_sql_analysis.ipynb")
	if err != nil {
		t.Fatal(err)
	}
	var notebook struct {
		NBFormat int `json:"nbformat"`
		Cells    []struct {
			CellType string   `json:"cell_type"`
			Source   []string `json:"source"`
		} `json:"cells"`
	}
	if err := json.Unmarshal(data, &notebook); err != nil {
		t.Fatal(err)
	}
	if notebook.NBFormat != 4 || len(notebook.Cells) < 3 {
		t.Fatalf("notebook shape = %#v", notebook)
	}
	content := string(data)
	for _, required := range []string{"/api/sql", "'stream': True", "pyarrow", "to_parquet"} {
		if !strings.Contains(content, required) {
			t.Fatalf("notebook missing %q", required)
		}
	}
}
