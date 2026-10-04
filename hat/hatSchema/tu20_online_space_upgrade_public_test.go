package hatSchema_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSchema"
)

type publicTU20Backend struct{}

func (publicTU20Backend) ReadOld(context.Context, string) (int, bool, error) {
	return 0, false, nil
}

func (publicTU20Backend) ReadNew(context.Context, string) (string, bool, error) {
	return "", false, nil
}

func (publicTU20Backend) ScanOld(context.Context, string, int) ([]hatSchema.OnlineSpaceUpgradeRecord[int], string, bool, error) {
	return nil, "", true, nil
}

func (publicTU20Backend) WriteDual(context.Context, string, int, string) error {
	return nil
}

func (publicTU20Backend) WriteNew(context.Context, string, string) error {
	return nil
}

func (publicTU20Backend) Cutover(context.Context, string, uint64) error {
	return nil
}

func (publicTU20Backend) Rollback(context.Context, string, uint64) error {
	return nil
}

func TestOnlineSpaceUpgradePublicAPI(t *testing.T) {
	upgrade, err := hatSchema.NewOnlineSpaceUpgrade(hatSchema.OnlineSpaceUpgradeOptions[int, string]{
		Space:           "users",
		PreviousVersion: 1,
		NextVersion:     2,
		Convert: func(value int) (string, error) {
			return "value", nil
		},
		Backend: publicTU20Backend{},
	})
	if err != nil {
		t.Fatalf("NewOnlineSpaceUpgrade() error = %v", err)
	}
	progress := upgrade.Progress()
	if progress.Phase != hatSchema.OnlineSpaceUpgradePending || progress.Ready {
		t.Fatalf("unexpected initial progress: %+v", progress)
	}
	checkpoint := upgrade.Checkpoint()
	if checkpoint.Space != "users" || checkpoint.NextVersion != 2 {
		t.Fatalf("unexpected checkpoint: %+v", checkpoint)
	}
}
