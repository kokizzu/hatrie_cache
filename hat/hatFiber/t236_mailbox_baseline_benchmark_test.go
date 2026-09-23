package hatFiber

import (
	"sync"
	"testing"
)

func BenchmarkT236ChannelBaseline(b *testing.B) {
	const (
		producerCount = 4
		totalMessages = 4096
		queueCapacity = 256
	)

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		queue := make(chan int, queueCapacity)
		start := make(chan struct{})
		var producers sync.WaitGroup
		producers.Add(producerCount)
		for producer := 0; producer < producerCount; producer++ {
			go func() {
				defer producers.Done()
				<-start
				for message := 0; message < totalMessages/producerCount; message++ {
					queue <- message
				}
			}()
		}
		close(start)
		for message := 0; message < totalMessages; message++ {
			<-queue
		}
		producers.Wait()
	}
}
