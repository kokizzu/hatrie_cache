package hatReplication

import "testing"

func TestT209RelayBackpressureUsesHighAndLowWatermarks(t *testing.T) {
	backpressure := NewRelayBackpressure(RelayBackpressureOptions{
		Enabled:         true,
		HighWatermark:   100,
		ResumeWatermark: 40,
	})

	for _, test := range []struct {
		name        string
		lag         uint64
		wantAllowed bool
		wantPaused  bool
		wantChange  bool
	}{
		{name: "below high", lag: 99, wantAllowed: true},
		{name: "enter paused", lag: 100, wantPaused: true, wantChange: true},
		{name: "hysteresis holds", lag: 50, wantPaused: true},
		{name: "resume at low", lag: 40, wantAllowed: true, wantChange: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision := backpressure.Admit(test.lag)
			if decision.Allowed != test.wantAllowed {
				t.Fatalf("Admit(%d).Allowed = %t, want %t; decision = %#v", test.lag, decision.Allowed, test.wantAllowed, decision)
			}
			if decision.Paused != test.wantPaused {
				t.Fatalf("Admit(%d).Paused = %t, want %t; decision = %#v", test.lag, decision.Paused, test.wantPaused, decision)
			}
			if decision.Transitioned != test.wantChange {
				t.Fatalf("Admit(%d).Transitioned = %t, want %t; decision = %#v", test.lag, decision.Transitioned, test.wantChange, decision)
			}
			if decision.Lag != test.lag {
				t.Fatalf("Admit(%d).Lag = %d, want %d", test.lag, decision.Lag, test.lag)
			}
		})
	}
}

func TestT209RelayBackpressureDefaultsOff(t *testing.T) {
	backpressure := NewRelayBackpressure(RelayBackpressureOptions{})
	decision := backpressure.Admit(^uint64(0))
	if !decision.Allowed || decision.Paused || decision.Transitioned {
		t.Fatalf("disabled backpressure decision = %#v, want an unchanged allow", decision)
	}
	if snapshot := backpressure.Snapshot(); snapshot.Enabled {
		t.Fatalf("disabled backpressure snapshot = %#v, want disabled", snapshot)
	}
}

func TestT209RelayBackpressureNormalizesWatermarks(t *testing.T) {
	backpressure := NewRelayBackpressure(RelayBackpressureOptions{
		Enabled:         true,
		HighWatermark:   8,
		ResumeWatermark: 8,
	})
	snapshot := backpressure.Snapshot()
	if snapshot.HighWatermark != 8 {
		t.Fatalf("normalized high watermark = %d, want 8", snapshot.HighWatermark)
	}
	if snapshot.ResumeWatermark >= snapshot.HighWatermark {
		t.Fatalf("normalized resume watermark = %d, high watermark = %d; resume must be lower", snapshot.ResumeWatermark, snapshot.HighWatermark)
	}
}
