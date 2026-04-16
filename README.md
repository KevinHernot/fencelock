# Fencelock

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Distributed lock primitives for Go services that need fencing tokens, not just lease ownership.

`fencelock` is a small Go library for distributed locking with fencing tokens. It is built around the correctness rule that matters for TTL-based distributed locks: every successful acquisition returns a monotonically increasing fencing token, and protected resources must reject writes with stale tokens.

## Why Fencing Tokens Matter

A TTL lock can expire while the original holder is paused by GC, scheduling, or the network. Another process can acquire the same lock and safely continue only if downstream resources can reject the old holder when it wakes up.

The lock prevents concurrent owners most of the time. The fencing token protects the resource when the lease boundary is crossed.

## What Is Included

- `Manager` for acquiring and refreshing locks
- `Lock` with owner value and fencing token accessors
- backend-neutral `Store` interface
- `MemoryStore` for tests and local tools
- `ValkeyStore` for Valkey or Redis-compatible deployments
- `MemoryFencingTokenGuard` showing how resources reject stale tokens
- context helpers for passing fencing tokens through call paths

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kevinhernot/fencelock"
)

func main() {
	manager := fencelock.NewManager(fencelock.NewMemoryStore())
	guard := fencelock.NewMemoryFencingTokenGuard()

	lock, err := manager.Acquire(
		context.Background(),
		"orders:order-1",
		fencelock.WithTTL(10*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer lock.Release(context.Background())

	if err := guard.CheckAndAdvance(context.Background(), "orders:order-1", lock.FencingToken()); err != nil {
		panic(err)
	}

	fmt.Printf("write accepted with token %d\n", lock.FencingToken())
}
```

## Valkey Store

```go
client, err := valkey.NewClient(valkey.ClientOption{
	InitAddress: []string{"localhost:6379"},
})
if err != nil {
	panic(err)
}
defer client.Close()

manager := fencelock.NewManager(fencelock.NewValkeyStore(client))
lock, err := manager.Acquire(ctx, "jobs:daily-rollup")
```

`ValkeyStore` uses:

- `INCR` for monotonically increasing fencing tokens
- `SET NX EX` for lock acquisition
- Lua compare-and-delete for safe release
- Lua compare-and-pexpire for safe refresh

## Protected Resource Rule

Every write guarded by a lock should carry the fencing token.

```go
func WriteOrder(ctx context.Context, orderID string, token int64) error {
	return guard.CheckAndAdvance(ctx, "orders:"+orderID, token)
}
```

Reject any token less than or equal to the highest token already accepted for that resource.

## Development

```bash
go test ./...
```

## License

[MIT](LICENSE)
