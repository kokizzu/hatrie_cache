package hatPeer

import (
	"context"
	"testing"
)

func t241BaselineAuthorize(context.Context, []byte, string, []string) error {
	return nil
}

func BenchmarkT241AuthorizationBaseline(b *testing.B) {
	command := []byte("get")
	roles := []string{"reader"}
	b.ReportAllocs()
	for range b.N {
		if err := t241BaselineAuthorize(context.Background(), command, "tenant-a", roles); err != nil {
			b.Fatal(err)
		}
	}
}
