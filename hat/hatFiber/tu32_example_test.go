package hatFiber

import (
	"context"
	"fmt"
)

func ExampleLocal() {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		panic(err)
	}
	local, err := NewLocal[string](scheduler)
	if err != nil {
		panic(err)
	}
	var tenant string
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if err := local.Set("eu"); err != nil {
			return 0, err
		}
		value, ok, err := local.Get()
		if err != nil {
			return 0, err
		}
		if ok {
			tenant = value
		}
		return StepDone, nil
	}); err != nil {
		panic(err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		panic(err)
	}
	fmt.Println(tenant)
	// Output: eu
}
