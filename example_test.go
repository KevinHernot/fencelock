package fencelock_test

import (
	"context"
	"fmt"

	"github.com/kevinhernot/fencelock"
)

func ExampleManager_Acquire() {
	ctx := context.Background()
	manager := fencelock.NewManager(fencelock.NewMemoryStore())
	guard := fencelock.NewMemoryFencingTokenGuard()

	lock, _ := manager.Acquire(ctx, "orders:order-1", fencelock.WithMaxRetries(0))
	defer lock.Release(ctx)

	_ = guard.CheckAndAdvance(ctx, "orders:order-1", lock.FencingToken())

	fmt.Printf("write accepted with token %d\n", lock.FencingToken())
	// Output:
	// write accepted with token 1
}
