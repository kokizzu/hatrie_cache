package hatCache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	if err := os.Chdir(filepath.Join("..", "..")); err != nil {
		fmt.Fprintln(os.Stderr, "change to repository root:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
