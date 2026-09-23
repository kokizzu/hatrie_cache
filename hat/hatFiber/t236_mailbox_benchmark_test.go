package hatFiber

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func BenchmarkT236MailboxBatch(b *testing.B) {
	const (
		producerCount = 4
		totalMessages = 4096
		queueCapacity = 256
		batchSize     = 64
	)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		mailbox, err := NewMailbox[int](queueCapacity)
		if err != nil {
			b.Fatal(err)
		}
		start := make(chan struct{})
		var producers sync.WaitGroup
		producers.Add(producerCount)
		for producer := 0; producer < producerCount; producer++ {
			go func() {
				defer producers.Done()
				<-start
				for message := 0; message < totalMessages/producerCount; message++ {
					if err := mailbox.Send(ctx, message); err != nil {
						b.Error(err)
						return
					}
				}
			}()
		}
		close(start)
		batch := make([]int, batchSize)
		received := 0
		for received < totalMessages {
			count, err := mailbox.ReceiveBatch(batch)
			if errors.Is(err, ErrMailboxEmpty) {
				batch[0], err = mailbox.Receive(ctx)
				count = 1
			}
			if err != nil {
				b.Fatal(err)
			}
			received += count
		}
		producers.Wait()
		if err := mailbox.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
