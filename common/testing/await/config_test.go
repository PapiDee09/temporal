package await

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/debug"
)

func TestRequire_SetsConfiguredAttemptContextDeadline(t *testing.T) {
	t.Setenv(attemptTimeoutEnvVar, "250ms")

	attemptTimeout := 250 * time.Millisecond * debug.TimeoutMultiplier
	parentCtx, cancel := context.WithTimeout(t.Context(), attemptTimeout+time.Second)
	defer cancel()

	var attemptCtx context.Context
	Require(parentCtx, t, func(t *T) {
		attemptCtx = t.Context()
	}, time.Nanosecond, time.Hour)

	require.NotNil(t, attemptCtx)
	require.NotSame(t, parentCtx, attemptCtx)

	attemptDeadline, ok := attemptCtx.Deadline()
	require.True(t, ok)
	require.LessOrEqual(t, time.Until(attemptDeadline), attemptTimeout)
	require.Greater(t, time.Until(attemptDeadline), attemptTimeout-100*time.Millisecond)
}

func TestConfig_OverrideTotalTimeout(t *testing.T) {
	t.Setenv(totalTimeoutEnvVar, "250ms")

	cfg := newConfig("")
	require.Equal(t, 250*time.Millisecond*debug.TimeoutMultiplier, cfg.totalTimeout)
}

func TestConfig_DefaultTotalTimeout(t *testing.T) {
	t.Setenv(totalTimeoutEnvVar, "")

	cfg := newConfig("")
	require.Equal(t, 90*time.Second*debug.TimeoutMultiplier, cfg.totalTimeout)
}

func TestConfig_NextPollIntervalCapsAtMaximum(t *testing.T) {
	cfg := newConfig("")

	require.Equal(t, 500*time.Millisecond, cfg.nextPollInterval(1))
	require.Equal(t, time.Second, cfg.nextPollInterval(2))
	require.Equal(t, 2*time.Second, cfg.nextPollInterval(3))
	require.Equal(t, 2*time.Second, cfg.nextPollInterval(4))
	require.Equal(t, 2*time.Second, cfg.nextPollInterval(20))
}
