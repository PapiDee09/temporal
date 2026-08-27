package await_test

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
)

func TestRequire_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	attempts := 0

	await.Require(t.Context(), t, func(t *await.T) {
		attempts++
	}, time.Second, 100*time.Millisecond)

	require.Equal(t, 1, attempts, "condition should be called exactly once")
}

func TestRequire_IgnoresLegacyTimingArguments(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		run  func(*testing.T, func(*await.T))
	}{
		{
			name: "Require",
			run: func(t *testing.T, condition func(*await.T)) {
				await.Require(t.Context(), t, condition, time.Nanosecond, time.Hour)
			},
		},
		{
			name: "Requiref",
			run: func(t *testing.T, condition func(*await.T)) {
				await.Requiref(t.Context(), t, condition, time.Nanosecond, time.Hour, "not ready")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var attempts atomic.Int32
			tc.run(t, func(t *await.T) {
				if attempts.Add(1) == 1 {
					t.Fail()
				}
			})

			require.Equal(t, int32(2), attempts.Load())
		})
	}
}

func TestRequire_RetriesUntilAttemptPasses(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		fail  func(*await.T, int32)
		stops bool
	}{
		{
			name: "Errorf",
			fail: func(t *await.T, attempt int32) {
				t.Errorf("not ready: %d", attempt)
			},
		},
		{
			name:  "FailNow",
			stops: true,
			fail: func(t *await.T, _ int32) {
				t.FailNow()
			},
		},
		{
			name:  "Fatal",
			stops: true,
			fail: func(t *await.T, _ int32) {
				t.Fatal("not ready")
			},
		},
		{
			name:  "Fatalf",
			stops: true,
			fail: func(t *await.T, attempt int32) {
				t.Fatalf("not ready: %d", attempt)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var attempts atomic.Int32
			var continuedAfterFailure atomic.Bool
			await.Require(t.Context(), t, func(t *await.T) {
				attempt := attempts.Add(1)
				if attempt < 3 {
					tc.fail(t, attempt)
					continuedAfterFailure.Store(true)
				}
			}, time.Second, 100*time.Millisecond)

			require.Equal(t, int32(3), attempts.Load())
			require.Equal(t, !tc.stops, continuedAfterFailure.Load())
		})
	}
}

func TestRequire_PropagatesParentContextValues(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "value")

	var got any
	await.Require(ctx, t, func(t *await.T) {
		got = t.Context().Value(contextKey{})
	}, time.Second, 100*time.Millisecond)

	require.Equal(t, "value", got)
}

func TestRequire_SetsAttemptContextDeadline(t *testing.T) {
	t.Parallel()

	longCtx := testcontext.For(t)
	longDeadline, ok := longCtx.Deadline()
	require.True(t, ok)

	attemptTimeout := 10 * time.Second * debug.TimeoutMultiplier

	var attemptCtx context.Context
	await.Require(longCtx, t, func(t *await.T) {
		attemptCtx = t.Context()
	}, time.Nanosecond, time.Hour)

	require.NotNil(t, attemptCtx)
	require.NotSame(t, longCtx, attemptCtx)

	attemptDeadline, ok := attemptCtx.Deadline()
	require.True(t, ok)
	require.True(t, attemptDeadline.Before(longDeadline))
	require.LessOrEqual(t, time.Until(attemptDeadline), attemptTimeout)
	require.Greater(t, time.Until(attemptDeadline), attemptTimeout-200*time.Millisecond)
}

func TestRequire_PollIntervalStartsAfterAttemptFinishes(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	var attemptStarts []time.Time
	var attemptEnds []time.Time
	attemptDuration := 60 * time.Millisecond
	minPollInterval := 500 * time.Millisecond

	await.Require(t.Context(), t, func(t *await.T) {
		attemptStarts = append(attemptStarts, time.Now())
		defer func() { attemptEnds = append(attemptEnds, time.Now()) }()

		time.Sleep(attemptDuration) //nolint:forbidigo // simulate attempt work to distinguish poll-after-start vs poll-after-end

		if attempts.Add(1) < 3 {
			t.Error("not ready")
		}
	}, time.Nanosecond, time.Hour)

	require.Equal(t, int32(3), attempts.Load())
	require.Len(t, attemptStarts, 3)
	require.Len(t, attemptEnds, 3)
	for i := 1; i < len(attemptStarts); i++ {
		gap := attemptStarts[i].Sub(attemptEnds[i-1])
		require.GreaterOrEqual(t, gap, minPollInterval,
			"poll interval should run after attempt finishes (gap=%v < %v)", gap, minPollInterval)
	}
}

func TestRequire_UsesAdaptivePollInterval(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	var attemptStarts []time.Time
	var attemptEnds []time.Time

	await.Require(t.Context(), t, func(t *await.T) {
		attemptStarts = append(attemptStarts, time.Now())
		defer func() { attemptEnds = append(attemptEnds, time.Now()) }()

		if attempts.Add(1) < 3 {
			t.Fail()
		}
	}, time.Nanosecond, time.Hour)

	require.Len(t, attemptStarts, 3)
	require.Len(t, attemptEnds, 3)
	require.GreaterOrEqual(t, attemptStarts[1].Sub(attemptEnds[0]), 500*time.Millisecond)
	require.GreaterOrEqual(t, attemptStarts[2].Sub(attemptEnds[1]), time.Second)
}

func TestRequire_FailureScenarios(t *testing.T) {
	t.Run("reports timeout", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(testcontext.For(t), 100*time.Millisecond)
		defer cancel()
		tb := newRecordingTB()
		tb.run(func() {
			await.Require(ctx, tb, func(t *await.T) {
				t.Error("not ready")
			}, time.Second, 100*time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "not satisfied after")
	})

	t.Run("cancels attempt context on timeout", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(testcontext.For(t), 100*time.Millisecond)
		defer cancel()
		tb := newRecordingTB()
		tb.run(func() {
			await.Require(ctx, tb, func(t *await.T) {
				<-t.Context().Done()
				if t.Context().Err() != context.DeadlineExceeded {
					t.Errorf("context error = %v", t.Context().Err())
				}
			}, 2*time.Second, time.Second)
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "not satisfied after")
	})

	t.Run("retries after attempt timeout until await timeout", func(t *testing.T) {
		attemptTimeoutEnv := 50 * time.Millisecond
		attemptTimeout := attemptTimeoutEnv * debug.TimeoutMultiplier
		awaitTimeout := attemptTimeout + 600*time.Millisecond
		t.Setenv("TEMPORAL_AWAIT_ATTEMPT_TIMEOUT", attemptTimeoutEnv.String())

		ctx, cancel := context.WithTimeout(testcontext.For(t), awaitTimeout)
		defer cancel()
		var attempts atomic.Int32
		var firstAttemptRemaining time.Duration

		tb := newRecordingTB()
		tb.run(func() {
			await.Require(ctx, tb, func(t *await.T) {
				if attempts.Add(1) == 1 {
					deadline, _ := t.Context().Deadline()
					firstAttemptRemaining = time.Until(deadline)
				}
				<-t.Context().Done()
			}, time.Nanosecond, time.Hour)
		})

		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "not satisfied after")
		require.Positive(t, firstAttemptRemaining)
		require.LessOrEqual(t, firstAttemptRemaining, attemptTimeout)
		require.Greater(t, attempts.Load(), int32(1))
	})

	t.Run("does not poll again after attempt consumes timeout", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(testcontext.For(t), 100*time.Millisecond)
		defer cancel()
		var attempts atomic.Int32

		tb := newRecordingTB()
		tb.run(func() {
			await.Require(ctx, tb, func(t *await.T) {
				attempts.Add(1)
				<-t.Context().Done() // block until timeout
			}, time.Second, 100*time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "not satisfied after")
		require.Equal(t, int32(1), attempts.Load())
	})

	t.Run("caps attempt context with parent deadline", func(t *testing.T) {
		t.Parallel()

		const parentTimeout = 100 * time.Millisecond
		ctx, cancel := context.WithTimeout(testcontext.For(t), parentTimeout)
		defer cancel()

		tb := newRecordingTB()
		tb.run(func() {
			await.Require(ctx, tb, func(t *await.T) {
				deadline, ok := t.Context().Deadline()
				if !ok {
					t.Error("missing deadline")
				}
				if time.Until(deadline) > parentTimeout {
					t.Errorf("deadline = %v", deadline)
				}
				<-t.Context().Done()
				if t.Context().Err() != context.DeadlineExceeded {
					t.Errorf("context error = %v", t.Context().Err())
				}
			}, 2*time.Second, time.Second)
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "not satisfied after")
	})

	t.Run("parent context cancellation stops polling", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(testcontext.For(t))
		defer cancel()
		var attempts atomic.Int32

		tb := newRecordingTB()
		tb.run(func() {
			await.Require(ctx, tb, func(t *await.T) {
				attempts.Add(1)
				t.Error("not ready")
				cancel()
			}, time.Second, 100*time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "context canceled before condition was satisfied")

		require.Equal(t, int32(1), attempts.Load(), "expected cancellation to stop polling")
	})

	t.Run("reports all attempt errors on timeout", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(testcontext.For(t), time.Second)
		defer cancel()
		var attempts atomic.Int32
		tb := newRecordingTB()
		tb.run(func() {
			await.Require(ctx, tb, func(t *await.T) {
				if attempts.Add(1) == 1 {
					t.Error("first attempt error")
					return
				}
				<-t.Context().Done()
				t.Error("last attempt error")
			}, time.Second, 100*time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "not satisfied after")
		require.Equal(t, "attempt errors:\n\n  --- attempt 1 ---\n    first attempt error\n\n  --- attempt 2 ---\n    last attempt error", tb.errors())
		require.Equal(t, int32(2), attempts.Load())
	})

	t.Run("Requiref includes message on timeout", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(testcontext.For(t), 100*time.Millisecond)
		defer cancel()
		tb := newRecordingTB()
		tb.run(func() {
			await.Requiref(ctx, tb, func(t *await.T) {
				t.Error("not ready")
			}, time.Second, 100*time.Millisecond, "workflow %s not ready", "wf-123")
		})
		require.True(t, tb.Failed())
		require.Contains(t, tb.fatals(), "workflow wf-123 not ready")
	})

	t.Run("panic propagates", func(t *testing.T) {
		t.Parallel()

		require.PanicsWithValue(t, "unexpected nil pointer", func() {
			await.Require(t.Context(), t, func(_ *await.T) {
				panic("unexpected nil pointer")
			}, time.Second, 100*time.Millisecond)
		})
	})

	t.Run("reports real TB misuse", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name   string
			misuse func(*recordingTB)
		}{
			{"Fatal stops real TB", func(tb *recordingTB) { tb.Fatal("wrong t used") }},
			{"Errorf marks real TB failed", func(tb *recordingTB) { tb.Errorf("assert-style misuse") }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				ctx := testcontext.For(t)
				tb := newRecordingTB()
				tb.run(func() {
					await.Require(ctx, tb, func(_ *await.T) {
						tc.misuse(tb)
					}, time.Second, 100*time.Millisecond)
				})
				require.True(t, tb.Failed())
				require.Contains(t, tb.fatals(), "use the *await.T")
			})
		}
	})

	t.Run("does not poll after prior failure", func(t *testing.T) {
		t.Parallel()

		ctx := testcontext.For(t)
		conditionCalled := false
		tb := newRecordingTB()
		tb.run(func() {
			tb.Errorf("previous failure")
			await.Require(ctx, tb, func(_ *await.T) {
				conditionCalled = true
			}, time.Second, 100*time.Millisecond)
		})
		require.True(t, tb.Failed())
		require.Empty(t, tb.fatals())
		require.False(t, conditionCalled, "condition should not run when test already failed")
	})
}

func TestRequire_SoftDeadlockLogsAndCancels(t *testing.T) {
	// not using T.Parallel() so it can use t.Setenv to override the deadlock timeouts
	t.Setenv("TEMPORAL_AWAIT_SOFT_DEADLOCK_TIMEOUT", "50ms")
	t.Setenv("TEMPORAL_AWAIT_HARD_DEADLOCK_TIMEOUT", "5s")

	const awaitTimeout = 10 * time.Second

	ctx := testcontext.For(t)
	tb := newRecordingTB()
	start := time.Now()
	tb.run(func() {
		// Await timeout is long so parent-cancel doesn't beat the soft timer.
		await.Require(ctx, tb, func(t *await.T) {
			<-t.Context().Done() // exits as soon as soft cancel fires
		}, awaitTimeout, 100*time.Millisecond)
	})
	elapsed := time.Since(start)
	require.False(t, tb.Failed(), "soft deadlock + clean exit should succeed")
	require.Contains(t, tb.logs(), "soft deadlock")
	require.NotContains(t, tb.fatals(), "still running")
	require.Less(t, elapsed, awaitTimeout,
		"should return shortly after soft cancel, not wait the full await timeout (elapsed=%v)", elapsed)
}

func TestRequire_DeadlockDetected(t *testing.T) {
	// not using T.Parallel() so it can use t.Setenv to override the deadlock timeouts.
	// Await timeout is long enough that the soft timer fires before parent cancellation,
	// so the path is: soft fires → log + cancel → condition still running → hard fires.
	t.Setenv("TEMPORAL_AWAIT_SOFT_DEADLOCK_TIMEOUT", "50ms")
	t.Setenv("TEMPORAL_AWAIT_HARD_DEADLOCK_TIMEOUT", "100ms")

	const awaitTimeout = 10 * time.Second

	ctx := testcontext.For(t)
	tb := newRecordingTB()
	start := time.Now()
	tb.run(func() {
		await.Require(ctx, tb, func(*await.T) {
			select {} // ignores t.Context()
		}, awaitTimeout, 50*time.Millisecond)
	})
	elapsed := time.Since(start)
	require.True(t, tb.Failed())
	require.Contains(t, tb.logs(), "soft deadlock")
	require.Contains(t, tb.fatals(), "still running")
	require.Contains(t, tb.fatals(), "does it honor t.Context()")
	require.Less(t, elapsed, awaitTimeout,
		"should fail at hard deadlock, not wait the full await timeout (elapsed=%v)", elapsed)
}

func TestRequire_WaitsForInFlightAttemptOnTimeout(t *testing.T) {
	t.Parallel()

	var finished atomic.Bool
	ctx, cancel := context.WithTimeout(testcontext.For(t), 100*time.Millisecond)
	defer cancel()
	tb := newRecordingTB()
	tb.run(func() {
		await.Require(ctx, tb, func(t *await.T) {
			<-t.Context().Done()
			finished.Store(true)
		}, time.Second, time.Second)
	})
	require.True(t, tb.Failed())
	require.Contains(t, tb.fatals(), "not satisfied after")
	require.True(t, finished.Load(), "Require returned before the running attempt exited")
}

// recordingTB is a minimal testing.TB implementation for testing failure scenarios.
type recordingTB struct {
	testing.TB    // embed for interface satisfaction
	ctx           context.Context
	mu            sync.Mutex
	failed        atomic.Bool
	errorMessages []string
	fatalMessages []string
	logMessages   []string
	cleanups      []func()
}

func newRecordingTB() *recordingTB {
	return &recordingTB{ctx: context.Background()}
}

func (r *recordingTB) Helper()      {}
func (r *recordingTB) Failed() bool { return r.failed.Load() }
func (r *recordingTB) Name() string { return "recordingTB" }
func (r *recordingTB) Logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logMessages = append(r.logMessages, fmt.Sprintf(format, args...))
}
func (r *recordingTB) Context() context.Context {
	return r.ctx
}

func (r *recordingTB) Cleanup(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, fn)
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed.Store(true)
	r.errorMessages = append(r.errorMessages, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.failed.Store(true)
	r.fatalMessages = append(r.fatalMessages, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) Fatal(args ...any) {
	r.mu.Lock()
	r.failed.Store(true)
	r.fatalMessages = append(r.fatalMessages, strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) FailNow() {
	r.failed.Store(true)
	runtime.Goexit()
}

func (r *recordingTB) run(fn func()) {
	done := make(chan struct{})
	go func() {
		defer func() {
			r.runCleanups()
			close(done)
		}()
		fn()
	}()
	<-done
}

func (r *recordingTB) runCleanups() {
	r.mu.Lock()
	cleanups := r.cleanups
	r.cleanups = nil
	r.mu.Unlock()

	for _, cleanup := range slices.Backward(cleanups) {
		cleanup()
	}
}

func (r *recordingTB) fatals() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.fatalMessages, "\n")
}

func (r *recordingTB) errors() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.errorMessages, "\n")
}

func (r *recordingTB) logs() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.logMessages, "\n")
}
