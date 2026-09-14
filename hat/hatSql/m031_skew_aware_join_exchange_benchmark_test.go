package hatSql

import (
	"strconv"
	"testing"
)

type m031BenchmarkJoinEvent struct {
	side      IncrementalJoinSide
	joinKey   string
	sourceKey string
}

func BenchmarkMZ031NaiveJoinExchange(b *testing.B) {
	events := m031BenchmarkJoinEvents()
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	lastMaxLoad := 0
	for operation := 0; operation < b.N; operation++ {
		loads := [4]int{}
		for _, event := range events {
			worker := int(skewAwareJoinExchangeHash(event.joinKey) % 4)
			loads[worker]++
			checksum += int64(worker)
		}
		lastMaxLoad = m031MaxWorkerLoad(loads[:])
	}
	b.StopTimer()
	b.ReportMetric(float64(lastMaxLoad), "max-load")
	if checksum == 0 {
		b.Fatal("unexpected checksum")
	}
}

func BenchmarkMZ031SkewAwareJoinExchange(b *testing.B) {
	exchange, err := NewSkewAwareJoinExchange(SkewAwareJoinExchangeOptions{
		Workers:         4,
		HotKeyThreshold: 100,
		BroadcastSide:   IncrementalJoinRight,
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := exchange.Observe("hot", 100); err != nil {
		b.Fatal(err)
	}
	events := m031BenchmarkJoinEvents()
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	lastMaxLoad := 0
	for operation := 0; operation < b.N; operation++ {
		loads := [4]int{}
		for _, event := range events {
			route, err := exchange.Route(event.side, event.joinKey, event.sourceKey)
			if err != nil {
				b.Fatal(err)
			}
			if route.Broadcast {
				for worker := 0; worker < 4; worker++ {
					loads[worker]++
				}
				checksum++
			} else {
				loads[route.Worker]++
				checksum += int64(route.Worker)
			}
		}
		lastMaxLoad = m031MaxWorkerLoad(loads[:])
	}
	b.StopTimer()
	b.ReportMetric(float64(lastMaxLoad), "max-load")
	if checksum == 0 {
		b.Fatal("unexpected checksum")
	}
}

func m031BenchmarkJoinEvents() []m031BenchmarkJoinEvent {
	events := make([]m031BenchmarkJoinEvent, 0, 100000)
	for index := 0; index < 80000; index++ {
		events = append(events, m031BenchmarkJoinEvent{
			side: IncrementalJoinLeft, joinKey: "hot", sourceKey: "probe-" + m031BenchmarkDecimal(index),
		})
	}
	for index := 0; index < 20000; index++ {
		events = append(events, m031BenchmarkJoinEvent{
			side: IncrementalJoinLeft, joinKey: "cold-" + m031BenchmarkDecimal(index%1000), sourceKey: "cold-source-" + m031BenchmarkDecimal(index),
		})
	}
	return events
}

func m031BenchmarkDecimal(value int) string {
	return strconv.Itoa(value)
}

func m031MaxWorkerLoad(loads []int) int {
	maximum := 0
	for _, load := range loads {
		if load > maximum {
			maximum = load
		}
	}
	return maximum
}
