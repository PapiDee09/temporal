package nexus

import (
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/matchingservice/v1"
)

func TestMatchingDispatchResponseToError_SyncSuccess(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
						},
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.NoError(t, err)
}

func TestMatchingDispatchResponseToError_AsyncSuccess(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
							AsyncSuccess: &nexuspb.StartOperationResponse_Async{
								OperationId: "test-op-id",
							},
						},
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.NoError(t, err)
}

func TestMatchingDispatchResponseToError_RequestTimeout(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
}

func TestMatchingDispatchResponseToError_WorkerFailure(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "bad request from worker",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						Type: "SomeError",
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "SomeError", appErr.Type())
}

func TestMatchingDispatchResponseToError_OperationFailure_ApplicationError(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_Failure{
							Failure: &failurepb.Failure{
								Message: "activity failed",
								FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
									ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
										Type: "SomeError",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "SomeError", appErr.Type())
}

func TestMatchingDispatchResponseToError_OperationFailure_CanceledError(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_Failure{
							Failure: &failurepb.Failure{
								Message: "canceled",
								FailureInfo: &failurepb.Failure_CanceledFailureInfo{
									CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
								},
							},
						},
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var cancelErr *temporal.CanceledError
	require.ErrorAs(t, err, &cancelErr)
}

func TestMatchingDispatchResponseToError_EmptyOutcome(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
}

// A worker predating Temporal failure responses reports a handler error through the deprecated
// variant. It must yield the same error type and retry behavior as the current variant, since callers
// decide whether to re-deliver the task based on Retryable().
func TestDispatchResultToError_DeprecatedHandlerError(t *testing.T) {
	for _, tc := range []struct {
		name          string
		errType       string
		retryBehavior enumspb.NexusHandlerErrorRetryBehavior
		wantRetryable bool
	}{
		{
			name:          "non-retryable by type",
			errType:       string(nexus.HandlerErrorTypeBadRequest),
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			wantRetryable: false,
		},
		{
			name:          "retryable by type",
			errType:       string(nexus.HandlerErrorTypeUnavailable),
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			wantRetryable: true,
		},
		{
			name:          "explicit override wins over the type default",
			errType:       string(nexus.HandlerErrorTypeBadRequest),
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
			wantRetryable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := MatchingDispatchResponseToError(&matchingservice.DispatchNexusTaskResponse{
				//nolint:staticcheck // Exercising the deprecated variant on purpose.
				Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
					HandlerError: &nexuspb.HandlerError{
						ErrorType:     tc.errType,
						RetryBehavior: tc.retryBehavior,
						Failure:       &nexuspb.Failure{Message: "worker said no"},
					},
				},
			})

			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, err, &handlerErr)
			require.Equal(t, nexus.HandlerErrorType(tc.errType), handlerErr.Type)
			require.Equal(t, tc.wantRetryable, handlerErr.Retryable())
			cause, ok := handlerErr.Cause.(*nexus.FailureError)
			require.True(t, ok, "expected a FailureError cause, got %T", handlerErr.Cause)
			require.Equal(t, "worker said no", cause.Failure.Message)
		})
	}
}

// The deprecated operation error carries a Nexus-encoded failure rather than a Temporal one. It must
// still produce the SDK-shaped error the current variant produces, so callers can tell a worker's own
// failure apart from a delivery problem.
func TestDispatchResultToError_DeprecatedOperationError(t *testing.T) {
	deprecatedOperationError := func(f *nexuspb.Failure) *matchingservice.DispatchNexusTaskResponse {
		return &matchingservice.DispatchNexusTaskResponse{
			Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
				Response: &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							//nolint:staticcheck // Exercising the deprecated variant on purpose.
							Variant: &nexuspb.StartOperationResponse_OperationError{
								OperationError: &nexuspb.UnsuccessfulOperationError{
									OperationState: string(nexus.OperationStateFailed),
									Failure:        f,
								},
							},
						},
					},
				},
			},
		}
	}

	t.Run("a plain failure becomes an application error", func(t *testing.T) {
		err := MatchingDispatchResponseToError(
			deprecatedOperationError(&nexuspb.Failure{Message: "operation failed"}))

		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, "operation failed", appErr.Message())
		// Not a handler error, so callers treat it as the worker's answer rather than retrying.
		var handlerErr *nexus.HandlerError
		require.NotErrorAs(t, err, &handlerErr)
	})

	// A handler outside Temporal sends a failure with no Temporal failure info, so the deprecated
	// state field is the only signal that the operation was canceled rather than failed.
	t.Run("a plain failure with a canceled state becomes a canceled error", func(t *testing.T) {
		err := MatchingDispatchResponseToError(&matchingservice.DispatchNexusTaskResponse{
			Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
				Response: &nexuspb.Response{
					Variant: &nexuspb.Response_StartOperation{
						StartOperation: &nexuspb.StartOperationResponse{
							//nolint:staticcheck // Exercising the deprecated variant on purpose.
							Variant: &nexuspb.StartOperationResponse_OperationError{
								OperationError: &nexuspb.UnsuccessfulOperationError{
									OperationState: string(nexus.OperationStateCanceled),
									Failure:        &nexuspb.Failure{Message: "operation canceled"},
								},
							},
						},
					},
				},
			},
		})

		var canceledErr *temporal.CanceledError
		require.ErrorAs(t, err, &canceledErr)
		// The worker's own failure is kept as the cause.
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, errors.Unwrap(canceledErr), &appErr)
		require.Equal(t, "operation canceled", appErr.Message())
	})

	t.Run("a canceled failure round-trips to a canceled error", func(t *testing.T) {
		nf, convErr := TemporalFailureToNexusFailure(&failurepb.Failure{
			Message: "operation canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
			},
		})
		require.NoError(t, convErr)

		err := MatchingDispatchResponseToError(
			deprecatedOperationError(NexusFailureToProtoFailure(nf)))

		var canceledErr *temporal.CanceledError
		require.ErrorAs(t, err, &canceledErr)
	})
}

// A cancel dispatch shares every failure arm with a start dispatch and reports acceptance as nil.
func TestDispatchResultToError_CancelAccepted(t *testing.T) {
	result := ClassifyCancelOperationDispatch(&matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_CancelOperation{
					CancelOperation: &nexuspb.CancelOperationResponse{},
				},
			},
		},
	})
	require.NoError(t, DispatchResultToError(result))
}

// A handler failure keeps the worker's type and retry behavior, which is what a caller needs in order
// to decide whether re-delivering the task is worthwhile.
func TestDispatchResultToError_HandlerFailurePreservesRetryBehavior(t *testing.T) {
	for _, tc := range []struct {
		name          string
		errType       string
		retryBehavior enumspb.NexusHandlerErrorRetryBehavior
		wantRetryable bool
	}{
		{
			name:          "non-retryable by type",
			errType:       string(nexus.HandlerErrorTypeBadRequest),
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			wantRetryable: false,
		},
		{
			name:          "retryable by type",
			errType:       string(nexus.HandlerErrorTypeUnavailable),
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			wantRetryable: true,
		},
		{
			name:          "explicit override wins over the type default",
			errType:       string(nexus.HandlerErrorTypeBadRequest),
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
			wantRetryable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := MatchingDispatchResponseToError(&matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
					Failure: &failurepb.Failure{
						Message: "handler said no",
						FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
							NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
								Type:          tc.errType,
								RetryBehavior: tc.retryBehavior,
							},
						},
					},
				},
			})

			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, err, &handlerErr)
			require.Equal(t, nexus.HandlerErrorType(tc.errType), handlerErr.Type)
			require.Equal(t, tc.wantRetryable, handlerErr.Retryable())
		})
	}
}
