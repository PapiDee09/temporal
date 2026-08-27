package await

import (
	"os"
	"time"

	"go.temporal.io/server/common/debug"
)

const (
	totalTimeoutEnvVar     = "TEMPORAL_TEST_TIMEOUT"
	attemptTimeoutEnvVar   = "TEMPORAL_AWAIT_ATTEMPT_TIMEOUT"
	defaultTotalTimeout    = 90 * time.Second
	defaultMinPollInterval = 500 * time.Millisecond
	defaultMaxPollInterval = 2 * time.Second
)

type config struct {
	totalTimeout    time.Duration
	minPollInterval time.Duration
	maxPollInterval time.Duration
	attemptTimeout  time.Duration
	timeoutMsg      string
}

func newConfig(timeoutMsg string) config {
	return config{
		totalTimeout:    envDuration(totalTimeoutEnvVar, defaultTotalTimeout) * debug.TimeoutMultiplier,
		minPollInterval: defaultMinPollInterval,
		maxPollInterval: defaultMaxPollInterval,
		attemptTimeout:  envDuration(attemptTimeoutEnvVar, 10*time.Second) * debug.TimeoutMultiplier,
		timeoutMsg:      timeoutMsg,
	}
}

func (c config) nextPollInterval(attempt int) time.Duration {
	interval := c.minPollInterval
	for range attempt - 1 {
		interval = min(interval*2, c.maxPollInterval)
	}
	return interval
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if s := os.Getenv(name); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
