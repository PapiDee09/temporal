package testcontext

import (
	"context"
	"sync"
	"time"
)

// timeoutContext has an active expiration that can be extended up to the fixed
// ceiling reported by Deadline without changing the context's identity.
type timeoutContext struct {
	context.Context
	ceiling time.Time
	done    chan struct{}

	mu                 sync.Mutex
	activeExpiration   time.Time
	timer              *time.Timer
	stopParentCallback func() bool
	err                error
}

func newTimeoutContext(parent context.Context, ceiling, activeExpiration time.Time) *timeoutContext {
	if ceiling.Before(activeExpiration) {
		activeExpiration = ceiling
	}
	ctx := &timeoutContext{
		Context:          context.WithoutCancel(parent),
		ceiling:          ceiling,
		done:             make(chan struct{}),
		activeExpiration: activeExpiration,
	}

	// Either callback may run immediately, and finishLocked needs both handles
	// initialized. Hold the lock until both are installed.
	ctx.mu.Lock()
	ctx.timer = time.AfterFunc(time.Until(activeExpiration), ctx.expire)
	ctx.stopParentCallback = context.AfterFunc(parent, ctx.cancel)
	ctx.mu.Unlock()
	return ctx
}

func (c *timeoutContext) Deadline() (time.Time, bool) {
	return c.ceiling, true
}

func (c *timeoutContext) Done() <-chan struct{} {
	return c.done
}

func (c *timeoutContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *timeoutContext) extend(expiration time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return
	}
	if c.ceiling.Before(expiration) {
		expiration = c.ceiling
	}
	if !expiration.After(c.activeExpiration) {
		return
	}

	c.activeExpiration = expiration
	c.timer.Reset(time.Until(expiration))
}

func (c *timeoutContext) effectiveExpiration() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activeExpiration
}

func (c *timeoutContext) cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.finishLocked(context.Canceled)
}

func (c *timeoutContext) expire() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return
	}
	// Reset may race a callback that already started. Re-check the active
	// expiration so that stale callbacks cannot cancel an extended context.
	if remaining := time.Until(c.activeExpiration); remaining > 0 {
		c.timer.Reset(remaining)
		return
	}
	c.finishLocked(context.DeadlineExceeded)
}

func (c *timeoutContext) finishLocked(err error) {
	if c.err != nil {
		return
	}
	c.err = err
	close(c.done)
	c.timer.Stop()
	c.stopParentCallback()
}
