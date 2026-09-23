package hatDictionary

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCH046NegativeCacheRejectsInvalidOptions(t *testing.T) {
	source := SourceFunc(func(context.Context, []string) (map[string]string, error) {
		return nil, nil
	})
	for _, options := range []Options{
		{NegativeTTL: -time.Second},
		{MaxNegativeEntries: -1},
	} {
		if _, err := New(source, options); !errors.Is(err, ErrOptionsInvalid) {
			t.Fatalf("options %#v error = %v, want ErrOptionsInvalid", options, err)
		}
	}
}
