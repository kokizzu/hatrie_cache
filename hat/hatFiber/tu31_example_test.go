package hatFiber

import (
	"context"
	"errors"
	"fmt"
)

func ExampleChannel() {
	scheduler, err := New(Options{MaxFibers: 2})
	if err != nil {
		panic(err)
	}
	channel, err := NewChannel[int](scheduler, 0)
	if err != nil {
		panic(err)
	}
	var received int
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		value, ok, err := channel.TryReceive()
		if errors.Is(err, ErrChannelWouldBlock) {
			return channel.WaitReceive()
		}
		if err != nil {
			return 0, err
		}
		if !ok {
			return StepDone, nil
		}
		received = value
		return StepDone, nil
	}); err != nil {
		panic(err)
	}
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if err := channel.TrySend(42); errors.Is(err, ErrChannelWouldBlock) {
			return channel.WaitSend(42)
		} else if err != nil {
			return 0, err
		}
		return StepDone, nil
	}); err != nil {
		panic(err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		panic(err)
	}
	fmt.Println(received)
	// Output: 42
}
