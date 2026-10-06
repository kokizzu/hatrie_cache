package hatTopology

import "testing"

func TestConfigWatchFirstReplayOffsetUsesLogicalRingOrder(t *testing.T) {
	log := &ConfigWatchLog{
		history: []ConfigWatchEvent{
			{Version: 60},
			{Version: 80},
			{Version: 100},
			{Version: 20},
			{Version: 40},
			{Version: 60},
			{Version: 80},
			{Version: 100},
		},
		historyStart: 3,
		historySize:  5,
		historyLimit: 8,
	}

	tests := []struct {
		after uint64
		want  int
	}{
		{after: 0, want: 0},
		{after: 20, want: 1},
		{after: 39, want: 1},
		{after: 40, want: 2},
		{after: 79, want: 3},
		{after: 80, want: 4},
		{after: 100, want: 5},
	}
	for _, test := range tests {
		if got := log.firstReplayOffsetAfter(test.after); got != test.want {
			t.Errorf("firstReplayOffsetAfter(%d) = %d, want %d", test.after, got, test.want)
		}
	}
}
