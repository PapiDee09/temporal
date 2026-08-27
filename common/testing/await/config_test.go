package await

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/debug"
)

func TestConfig_OverrideAttemptTimeout(t *testing.T) {
	t.Setenv(attemptTimeoutEnvVar, "250ms")

	cfg := newConfig("")
	require.Equal(t, 250*time.Millisecond*debug.TimeoutMultiplier, cfg.attemptTimeout)
}

func TestConfig_OverrideTotalTimeout(t *testing.T) {
	t.Setenv(totalTimeoutEnvVar, "250ms")

	cfg := newConfig("")
	require.Equal(t, 250*time.Millisecond*debug.TimeoutMultiplier, cfg.totalTimeout)
}

func TestConfig_NextPollIntervalCapsAtMaximum(t *testing.T) {
	cfg := newConfig("")

	require.Equal(t, 500*time.Millisecond, cfg.nextPollInterval(1))
	require.Equal(t, time.Second, cfg.nextPollInterval(2))
	require.Equal(t, 2*time.Second, cfg.nextPollInterval(3))
	require.Equal(t, 2*time.Second, cfg.nextPollInterval(4))
	require.Equal(t, 2*time.Second, cfg.nextPollInterval(20))
}
