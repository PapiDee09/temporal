package testcore

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
	"google.golang.org/grpc/metadata"
)

type TestEnvSuite struct {
	parallelsuite.Suite[*TestEnvSuite]
}

func TestTestEnvSuite(t *testing.T) {
	parallelsuite.Run(t, &TestEnvSuite{})
}

func TestTestEnvContextCachesFinalDecoratedContext(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		earlyCtx := testcontext.For(t)
		ctx := finalizeTestContext(t)
		env := &TestEnv{ctx: ctx}

		first := env.Context()
		second := env.Context()
		require.Same(t, ctx, first)
		require.Same(t, first, second)
		require.NotSame(t, earlyCtx, first)
		md, ok := metadata.FromOutgoingContext(first)
		require.True(t, ok)
		require.Equal(t, []string{headers.ServerVersion}, md.Get(headers.ClientVersionHeaderName))

		testcontext.EnsureRemaining(env.Context(), t, testcontext.DefaultTimeout()+10*time.Second)
		time.Sleep(testcontext.DefaultTimeout() + time.Second) //nolint:forbidigo // advance past the original active expiration
		require.NoError(t, env.Context().Err())
	})
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_NoErrorWithoutExplicitRequest() {
	guard := newDedicatedClusterGuard(false)

	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_FailsWhenUnused() {
	guard := newDedicatedClusterGuard(true)

	s.EqualError(guard.validate(),
		`testcore.WithDedicatedCluster() was requested but no dedicated-cluster-only feature was used`)
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_NoErrorAfterUse() {
	guard := newDedicatedClusterGuard(true)
	guard.record("global hook")

	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_ConcurrentRecord() {
	guard := newDedicatedClusterGuard(true)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			guard.record("reason")
		})
	}
	wg.Wait()
	s.NoError(guard.validate())
}
