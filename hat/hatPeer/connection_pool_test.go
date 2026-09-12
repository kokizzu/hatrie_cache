package hatPeer

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type connectionPoolTestConnection struct {
	id     int
	closed atomic.Int32
}

func (connection *connectionPoolTestConnection) Close() error {
	connection.closed.Add(1)
	return nil
}

func TestConnectionPoolReusesIdleConnection(t *testing.T) {
	var dialed atomic.Int32
	var connections []*connectionPoolTestConnection
	var mu sync.Mutex
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:           2,
		MaxIdle:           2,
		MaxDialAttempts:   1,
		DialRetryDelay:    time.Nanosecond,
		DialRetryMaxDelay: time.Nanosecond,
		Dial: func(context.Context) (Connection, error) {
			id := int(dialed.Add(1))
			connection := &connectionPoolTestConnection{id: id}
			mu.Lock()
			connections = append(connections, connection)
			mu.Unlock()
			return connection, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	var used []int
	for range 2 {
		if err := p.Do(context.Background(), func(_ context.Context, connection Connection) error {
			used = append(used, connection.(*connectionPoolTestConnection).id)
			return nil
		}); err != nil {
			t.Fatalf("Do() error = %v", err)
		}
	}
	if !reflect.DeepEqual(used, []int{1, 1}) {
		t.Fatalf("used connection IDs = %v, want [1 1]", used)
	}
	if got := dialed.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1", got)
	}
	stats := p.Stats()
	if stats.Acquires != 2 || stats.DialAttempts != 1 || stats.Active != 0 || stats.Idle != 1 || stats.Open != 1 {
		t.Fatalf("Stats() = %#v, want two acquires and one idle connection", stats)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if connections[0].closed.Load() != 1 {
		t.Fatalf("connection close count = %d, want 1", connections[0].closed.Load())
	}
}

func TestConnectionPoolValidatesOptionsAndNilReceiver(t *testing.T) {
	cases := []struct {
		name    string
		want    error
		options ConnectionPoolOptions
	}{
		{name: "dial", want: ErrConnectionPoolDialRequired},
		{name: "max open", want: ErrConnectionPoolMaxOpenInvalid, options: ConnectionPoolOptions{MaxOpen: -1, Dial: func(context.Context) (Connection, error) { return nil, nil }}},
		{name: "max idle", want: ErrConnectionPoolMaxIdleInvalid, options: ConnectionPoolOptions{MaxIdle: -1, Dial: func(context.Context) (Connection, error) { return nil, nil }}},
		{name: "breaker threshold", want: ErrConnectionPoolCircuitThresholdInvalid, options: ConnectionPoolOptions{Breaker: &ConnectionPoolCircuitBreakerOptions{FailureThreshold: -1}, Dial: func(context.Context) (Connection, error) { return nil, nil }}},
		{name: "breaker interval", want: ErrConnectionPoolCircuitIntervalInvalid, options: ConnectionPoolOptions{Breaker: &ConnectionPoolCircuitBreakerOptions{OpenInterval: -1}, Dial: func(context.Context) (Connection, error) { return nil, nil }}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewConnectionPool(test.options); !errors.Is(err, test.want) {
				t.Fatalf("NewConnectionPool() error = %v, want %v", err, test.want)
			}
		})
	}
	var nilPool *ConnectionPool
	if err := nilPool.Do(context.Background(), func(context.Context, Connection) error { return nil }); !errors.Is(err, ErrConnectionPoolNil) {
		t.Fatalf("nil Do() error = %v", err)
	}
	if err := nilPool.Close(context.Background()); !errors.Is(err, ErrConnectionPoolNil) {
		t.Fatalf("nil Close() error = %v", err)
	}
	if got := nilPool.Stats(); got != (ConnectionPoolStats{}) {
		t.Fatalf("nil Stats() = %#v, want zero", got)
	}
	if got := nilPool.CircuitBreakerStats(); got != (ConnectionPoolCircuitBreakerStats{}) {
		t.Fatalf("nil CircuitBreakerStats() = %#v, want zero", got)
	}
}

func TestConnectionPoolRetriesDialFailures(t *testing.T) {
	var attempts atomic.Int32
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxDialAttempts:   3,
		DialRetryDelay:    time.Nanosecond,
		DialRetryMaxDelay: time.Nanosecond,
		Dial: func(context.Context) (Connection, error) {
			if attempts.Add(1) < 3 {
				return nil, errors.New("temporary dial failure")
			}
			return &connectionPoolTestConnection{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	stats := p.Stats()
	if stats.DialAttempts != 3 || stats.DialFailures != 2 {
		t.Fatalf("Stats() = %#v, want three attempts and two failures", stats)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestConnectionPoolDiscardsConnectionAfterHandlerError(t *testing.T) {
	var dialed atomic.Int32
	var first *connectionPoolTestConnection
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxDialAttempts: 1,
		Dial: func(context.Context) (Connection, error) {
			connection := &connectionPoolTestConnection{id: int(dialed.Add(1))}
			if connection.id == 1 {
				first = connection
			}
			return connection, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	wantErr := errors.New("handler failed")
	if err := p.Do(context.Background(), func(context.Context, Connection) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("first Do() error = %v, want %v", err, wantErr)
	}
	if first == nil || first.closed.Load() != 1 {
		t.Fatalf("first connection close count = %v, want 1", first)
	}
	if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
		t.Fatalf("second Do() error = %v", err)
	}
	if got := dialed.Load(); got != 2 {
		t.Fatalf("dial count = %d, want 2", got)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestConnectionPoolBackpressureHonorsContext(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Dial: func(context.Context) (Connection, error) {
			return &connectionPoolTestConnection{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	go func() {
		firstDone <- p.Do(context.Background(), func(context.Context, Connection) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := p.Do(ctx, func(context.Context, Connection) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked Do() error = %v, want deadline exceeded", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Do() error = %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestConnectionPoolCloseWaitsForActiveHandler(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	connection := &connectionPoolTestConnection{}
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxDialAttempts: 1,
		Dial:            func(context.Context) (Connection, error) { return connection, nil },
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	go func() {
		firstDone <- p.Do(context.Background(), func(context.Context, Connection) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := p.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("early Close() error = %v, want deadline exceeded", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Do() error = %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("final Close() error = %v", err)
	}
	if got := connection.closed.Load(); got != 1 {
		t.Fatalf("connection close count = %d, want 1", got)
	}
}

func TestConnectionPoolCircuitBreakerOpensAndRecovers(t *testing.T) {
	var dialed atomic.Int32
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxDialAttempts: 1,
		Breaker: &ConnectionPoolCircuitBreakerOptions{
			FailureThreshold: 2,
			OpenInterval:     10 * time.Millisecond,
		},
		Dial: func(context.Context) (Connection, error) {
			switch dialed.Add(1) {
			case 1, 2:
				return nil, errors.New("peer unavailable")
			default:
				return &connectionPoolTestConnection{}, nil
			}
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	for range 2 {
		if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); err == nil {
			t.Fatal("Do() unexpectedly succeeded while peer was unavailable")
		}
	}
	if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); !errors.Is(err, ErrConnectionPoolCircuitOpen) {
		t.Fatalf("open-circuit Do() error = %v, want %v", err, ErrConnectionPoolCircuitOpen)
	}
	if got := dialed.Load(); got != 2 {
		t.Fatalf("dial count while circuit open = %d, want 2", got)
	}
	stats := p.CircuitBreakerStats()
	if !stats.Enabled || stats.State != ConnectionPoolCircuitOpen || stats.Opens != 1 || stats.Rejects != 1 {
		t.Fatalf("CircuitBreakerStats() = %#v, want one open and one rejection", stats)
	}

	deadline := time.Now().Add(time.Second)
	for {
		err := p.Do(context.Background(), func(context.Context, Connection) error { return nil })
		if err == nil {
			break
		}
		if !errors.Is(err, ErrConnectionPoolCircuitOpen) {
			t.Fatalf("recovery Do() error = %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("circuit did not enter half-open recovery")
		}
		runtime.Gosched()
	}
	if got := dialed.Load(); got != 3 {
		t.Fatalf("dial count after recovery = %d, want 3", got)
	}
	stats = p.CircuitBreakerStats()
	if stats.State != ConnectionPoolCircuitClosed || stats.ConsecutiveFailures != 0 || stats.Probes != 1 {
		t.Fatalf("recovered CircuitBreakerStats() = %#v, want closed state", stats)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestConnectionPoolCircuitBreakerIgnoresContextFailures(t *testing.T) {
	var dialed atomic.Int32
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxDialAttempts: 1,
		Breaker: &ConnectionPoolCircuitBreakerOptions{
			FailureThreshold: 1,
			OpenInterval:     time.Hour,
		},
		Dial: func(context.Context) (Connection, error) {
			dialed.Add(1)
			return nil, context.Canceled
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	for range 2 {
		if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); !errors.Is(err, context.Canceled) {
			t.Fatalf("Do() error = %v, want context canceled", err)
		}
	}
	if got := dialed.Load(); got != 2 {
		t.Fatalf("dial count = %d, want 2", got)
	}
	stats := p.CircuitBreakerStats()
	if stats.State != ConnectionPoolCircuitClosed || stats.ConsecutiveFailures != 0 || stats.Opens != 0 {
		t.Fatalf("CircuitBreakerStats() = %#v, want unchanged closed state", stats)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestConnectionPoolCircuitBreakerAbortedProbeReopens(t *testing.T) {
	breaker := &connectionPoolCircuitBreaker{
		failureThreshold: 1,
		openInterval:     time.Millisecond,
		state:            ConnectionPoolCircuitClosed,
	}
	if !breaker.failure() {
		t.Fatal("failure() did not open the circuit")
	}
	time.Sleep(2 * time.Millisecond)
	if err := breaker.allow(); err != nil {
		t.Fatalf("allow() error = %v, want half-open probe", err)
	}
	breaker.abortProbe()
	stats := breaker.stats()
	if stats.State != ConnectionPoolCircuitOpen || stats.Probes != 1 {
		t.Fatalf("stats after aborted probe = %#v, want open state", stats)
	}
}

func BenchmarkConnectionPoolDo(b *testing.B) {
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Dial: func(context.Context) (Connection, error) {
			return &connectionPoolTestConnection{}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := p.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkConnectionPoolDialEachCall(b *testing.B) {
	dial := func(context.Context) Connection { return &connectionPoolTestConnection{} }
	b.ReportAllocs()
	for range b.N {
		connection := dial(context.Background())
		if err := connection.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

var benchmarkConnectionPoolSink uint64

func benchmarkConnectionPoolHandshake() Connection {
	value := uint64(0x9e3779b97f4a7c15)
	for range 256 {
		value ^= value << 7
		value ^= value >> 9
		value *= 0x9e3779b97f4a7c15
	}
	benchmarkConnectionPoolSink = value
	runtime.KeepAlive(benchmarkConnectionPoolSink)
	return &connectionPoolTestConnection{}
}

func BenchmarkConnectionPoolDoWithHandshake(b *testing.B) {
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Dial: func(context.Context) (Connection, error) {
			return benchmarkConnectionPoolHandshake(), nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := p.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkConnectionPoolDialEachCallWithHandshake(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		connection := benchmarkConnectionPoolHandshake()
		if err := connection.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnectionPoolDialStormWithoutBreaker(b *testing.B) {
	wantErr := errors.New("peer unavailable")
	var dialed atomic.Int64
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxDialAttempts: 1,
		Dial: func(context.Context) (Connection, error) {
			dialed.Add(1)
			return nil, wantErr
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = p.Do(context.Background(), func(context.Context, Connection) error { return nil })
	}
	b.StopTimer()
	b.ReportMetric(float64(dialed.Load()), "dial-calls")
	if err := p.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkConnectionPoolDialStormWithBreaker(b *testing.B) {
	wantErr := errors.New("peer unavailable")
	var dialed atomic.Int64
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxDialAttempts: 1,
		Breaker: &ConnectionPoolCircuitBreakerOptions{
			FailureThreshold: 1,
			OpenInterval:     time.Hour,
		},
		Dial: func(context.Context) (Connection, error) {
			dialed.Add(1)
			return nil, wantErr
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	_ = p.Do(context.Background(), func(context.Context, Connection) error { return nil })
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = p.Do(context.Background(), func(context.Context, Connection) error { return nil })
	}
	b.StopTimer()
	b.ReportMetric(float64(dialed.Load()), "dial-calls")
	if err := p.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}
