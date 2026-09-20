package hatFiber

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type channelHandoffState struct {
	channel  *Channel[int]
	received int
}

func (state *channelHandoffState) receive(context.Context) (Step, error) {
	value, ok, err := state.channel.TryReceive()
	if errors.Is(err, ErrChannelWouldBlock) {
		return state.channel.WaitReceive()
	}
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("channel closed during benchmark")
	}
	state.received = value
	return StepDone, nil
}

func (state *channelHandoffState) send(context.Context) (Step, error) {
	if err := state.channel.TrySend(1); errors.Is(err, ErrChannelWouldBlock) {
		return state.channel.WaitSend(1)
	} else if err != nil {
		return 0, err
	}
	return StepDone, nil
}

func BenchmarkTU31FiberChannelHandoff(b *testing.B) {
	scheduler, err := New(Options{MaxFibers: 2})
	if err != nil {
		b.Fatal(err)
	}
	channel, err := NewChannel[int](scheduler, 0)
	if err != nil {
		b.Fatal(err)
	}
	state := &channelHandoffState{channel: channel}
	receive := StepFunc(state.receive)
	send := StepFunc(state.send)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		state.received = 0
		receiver, err := scheduler.Spawn(receive)
		if err != nil {
			b.Fatal(err)
		}
		sender, err := scheduler.Spawn(send)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := scheduler.Run(ctx, 0); err != nil {
			b.Fatal(err)
		}
		if state.received != 1 {
			b.Fatalf("received = %d, want 1", state.received)
		}
		if err := scheduler.Reap(receiver); err != nil {
			b.Fatal(err)
		}
		if err := scheduler.Reap(sender); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU31GoroutineChannelHandoff(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		channel := make(chan int)
		var waitGroup sync.WaitGroup
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-channel
		}()
		go func() {
			channel <- 1
		}()
		waitGroup.Wait()
	}
}
