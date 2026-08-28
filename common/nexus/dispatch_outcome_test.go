package nexus

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

func startResponse(sor *nexuspb.StartOperationResponse) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{StartOperation: sor},
			},
		},
	}
}

func handlerFailureResponse(errType string, retry enumspb.NexusHandlerErrorRetryBehavior) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "handler said no",
				FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
						Type:          errType,
						RetryBehavior: retry,
					},
				},
			},
		},
	}
}

func TestClassifyStartOperationDispatch_SyncSuccess(t *testing.T) {
	payload := &commonpb.Payload{Data: []byte("v")}
	links := []*nexuspb.Link{{Url: "http://a.test/l", Type: "t"}}
	r := ClassifyStartOperationDispatch(startResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: payload, Links: links},
		},
	}))

	require.Equal(t, DispatchOutcomeSyncSuccess, r.Outcome)
	require.True(t, r.Outcome.Succeeded())
	require.Same(t, payload, r.SyncPayload)
	require.Equal(t, links, r.Links)
}

// An operation is allowed to succeed with no value.
func TestClassifyStartOperationDispatch_SyncSuccess_NoPayload(t *testing.T) {
	r := ClassifyStartOperationDispatch(startResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
		},
	}))

	require.Equal(t, DispatchOutcomeSyncSuccess, r.Outcome)
	require.True(t, r.Outcome.Succeeded())
	require.Nil(t, r.SyncPayload)
	require.Empty(t, r.Links)
}

func TestClassifyStartOperationDispatch_AsyncSuccess(t *testing.T) {
	for _, tc := range []struct {
		name      string
		async     *nexuspb.StartOperationResponse_Async
		wantToken string
	}{
		{
			name: "operation token wins over the deprecated id",
			//nolint:staticcheck // Exercising the deprecated field on purpose.
			async:     &nexuspb.StartOperationResponse_Async{OperationToken: "tok", OperationId: "id"},
			wantToken: "tok",
		},
		{
			name: "falls back to the deprecated id",
			//nolint:staticcheck // Exercising the deprecated field on purpose.
			async:     &nexuspb.StartOperationResponse_Async{OperationId: "id"},
			wantToken: "id",
		},
		{
			name:      "neither set",
			async:     &nexuspb.StartOperationResponse_Async{},
			wantToken: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := ClassifyStartOperationDispatch(startResponse(&nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_AsyncSuccess{AsyncSuccess: tc.async},
			}))
			require.Equal(t, DispatchOutcomeAsyncSuccess, r.Outcome)
			require.True(t, r.Outcome.Succeeded())
			require.Equal(t, tc.wantToken, r.OperationToken)
		})
	}
}

func TestClassifyStartOperationDispatch_OperationFailure(t *testing.T) {
	failure := &failurepb.Failure{Message: "op failed"}
	r := ClassifyStartOperationDispatch(startResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_Failure{Failure: failure},
	}))

	require.Equal(t, DispatchOutcomeOperationFailure, r.Outcome)
	require.False(t, r.Outcome.Succeeded())
	require.Same(t, failure, r.Failure)
}

func TestClassifyStartOperationDispatch_DeprecatedOperationError(t *testing.T) {
	opErr := &nexuspb.UnsuccessfulOperationError{
		OperationState: string(nexus.OperationStateCanceled),
		Failure:        &nexuspb.Failure{Message: "canceled"},
	}
	r := ClassifyStartOperationDispatch(startResponse(&nexuspb.StartOperationResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Variant: &nexuspb.StartOperationResponse_OperationError{OperationError: opErr},
	}))

	require.Equal(t, DispatchOutcomeOperationFailureDeprecated, r.Outcome)
	require.Same(t, opErr, r.OperationError)
	require.Nil(t, r.Failure)
}

// A handler failure and a plain worker failure arrive in the same response arm and have to be told
// apart by whether the failure carries Nexus handler failure info.
func TestClassifyStartOperationDispatch_HandlerFailureVsWorkerFailure(t *testing.T) {
	t.Run("handler failure", func(t *testing.T) {
		resp := handlerFailureResponse(string(nexus.HandlerErrorTypeBadRequest), enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED)
		r := ClassifyStartOperationDispatch(resp)
		require.Equal(t, DispatchOutcomeHandlerFailure, r.Outcome)
		require.False(t, r.Outcome.Succeeded())
		require.NotNil(t, r.Failure)
	})

	t.Run("worker failure", func(t *testing.T) {
		resp := &matchingservice.DispatchNexusTaskResponse{
			Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
				Failure: &failurepb.Failure{
					Message: "worker exploded",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeError"},
					},
				},
			},
		}
		r := ClassifyStartOperationDispatch(resp)
		require.Equal(t, DispatchOutcomeWorkerFailure, r.Outcome)
		require.False(t, r.Outcome.Succeeded())
		require.NotNil(t, r.Failure)
	})
}

func TestClassifyStartOperationDispatch_DeprecatedHandlerError(t *testing.T) {
	he := &nexuspb.HandlerError{
		ErrorType:     string(nexus.HandlerErrorTypeResourceExhausted),
		RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
	}
	r := ClassifyStartOperationDispatch(&matchingservice.DispatchNexusTaskResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{HandlerError: he},
	})

	require.Equal(t, DispatchOutcomeHandlerFailureDeprecated, r.Outcome)
	require.False(t, r.Outcome.Succeeded())
	require.Same(t, he, r.HandlerError)
	require.Nil(t, r.Failure)
}

func TestClassifyStartOperationDispatch_RequestTimeout(t *testing.T) {
	r := ClassifyStartOperationDispatch(&matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		},
	})

	require.Equal(t, DispatchOutcomeRequestTimeout, r.Outcome)
	require.False(t, r.Outcome.Succeeded())
}

func TestClassifyStartOperationDispatch_Unrecognized(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp *matchingservice.DispatchNexusTaskResponse
	}{
		{name: "nil response", resp: nil},
		{name: "no outcome", resp: &matchingservice.DispatchNexusTaskResponse{}},
		{name: "no start variant", resp: startResponse(&nexuspb.StartOperationResponse{})},
		{name: "nil start operation", resp: startResponse(nil)},
		{
			name: "a cancel answer to a start request",
			resp: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
					Response: &nexuspb.Response{
						Variant: &nexuspb.Response_CancelOperation{
							CancelOperation: &nexuspb.CancelOperationResponse{},
						},
					},
				},
			},
		},
		{
			name: "an empty response envelope",
			resp: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
					Response: &nexuspb.Response{},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := ClassifyStartOperationDispatch(tc.resp)
			require.Equal(t, DispatchOutcomeUnrecognized, r.Outcome)
			require.False(t, r.Outcome.Succeeded())
		})
	}
}

// A cancel dispatch shares every failure arm with a start dispatch, and differs only in that any
// response at all means the cancel was accepted.
func TestClassifyCancelOperationDispatch(t *testing.T) {
	t.Run("any response is accepted", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			resp *matchingservice.DispatchNexusTaskResponse
		}{
			{
				name: "cancel variant",
				resp: &matchingservice.DispatchNexusTaskResponse{
					Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
						Response: &nexuspb.Response{
							Variant: &nexuspb.Response_CancelOperation{
								CancelOperation: &nexuspb.CancelOperationResponse{},
							},
						},
					},
				},
			},
			{
				name: "empty envelope",
				resp: &matchingservice.DispatchNexusTaskResponse{
					Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
						Response: &nexuspb.Response{},
					},
				},
			},
			{
				// The outcome arm is what marks the task as answered, not the message inside it.
				name: "nil envelope",
				resp: &matchingservice.DispatchNexusTaskResponse{
					Outcome: &matchingservice.DispatchNexusTaskResponse_Response{},
				},
			},
			{
				name: "a start answer to a cancel request is still accepted",
				resp: startResponse(&nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_SyncSuccess{
						SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
					},
				}),
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := ClassifyCancelOperationDispatch(tc.resp)
				require.Equal(t, DispatchOutcomeCancelAccepted, r.Outcome)
				require.True(t, r.Outcome.Succeeded())
			})
		}
	})

	t.Run("failure arms match the start path", func(t *testing.T) {
		resp := handlerFailureResponse(string(nexus.HandlerErrorTypeNotFound), enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED)
		r := ClassifyCancelOperationDispatch(resp)
		require.Equal(t, DispatchOutcomeHandlerFailure, r.Outcome)
	})

	t.Run("deprecated handler error", func(t *testing.T) {
		he := &nexuspb.HandlerError{ErrorType: string(nexus.HandlerErrorTypeNotImplemented)}
		r := ClassifyCancelOperationDispatch(&matchingservice.DispatchNexusTaskResponse{
			//nolint:staticcheck // Exercising the deprecated variant on purpose.
			Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{HandlerError: he},
		})
		require.Equal(t, DispatchOutcomeHandlerFailureDeprecated, r.Outcome)
		require.Same(t, he, r.HandlerError)
	})

	t.Run("request timeout", func(t *testing.T) {
		r := ClassifyCancelOperationDispatch(&matchingservice.DispatchNexusTaskResponse{
			Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
				RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
			},
		})
		require.Equal(t, DispatchOutcomeRequestTimeout, r.Outcome)
	})

	t.Run("unrecognized", func(t *testing.T) {
		r := ClassifyCancelOperationDispatch(&matchingservice.DispatchNexusTaskResponse{})
		require.Equal(t, DispatchOutcomeUnrecognized, r.Outcome)
	})
}

// The classifier aliases the response's failure proto rather than copying it, which callers that
// convert failures in place rely on.
func TestClassifyStartOperationDispatch_FailureAliasesTheResponse(t *testing.T) {
	resp := handlerFailureResponse(string(nexus.HandlerErrorTypeInternal), enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED)
	r := ClassifyStartOperationDispatch(resp)
	//nolint:revive // The type is known from the fixture.
	require.Same(t, resp.GetOutcome().(*matchingservice.DispatchNexusTaskResponse_Failure).Failure, r.Failure)
}

// The outcome tag values are what existing dashboards query, so they are pinned here rather than only
// asserted end-to-end through a handler.
func TestDispatchResultOutcomeTag(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result DispatchResult
		want   string
	}{
		{
			name:   "sync success",
			result: DispatchResult{Outcome: DispatchOutcomeSyncSuccess},
			want:   "sync_success",
		},
		{
			name:   "async success",
			result: DispatchResult{Outcome: DispatchOutcomeAsyncSuccess},
			want:   "async_success",
		},
		{
			name:   "cancel accepted",
			result: DispatchResult{Outcome: DispatchOutcomeCancelAccepted},
			want:   "success",
		},
		{
			name:   "operation failure",
			result: DispatchResult{Outcome: DispatchOutcomeOperationFailure},
			want:   "failure",
		},
		{
			name:   "deprecated operation error",
			result: DispatchResult{Outcome: DispatchOutcomeOperationFailureDeprecated},
			want:   "operation_error",
		},
		{
			name:   "request timeout",
			result: DispatchResult{Outcome: DispatchOutcomeRequestTimeout},
			want:   "handler_timeout",
		},
		{
			name:   "unrecognized",
			result: DispatchResult{Outcome: DispatchOutcomeUnrecognized},
			want:   "handler_error:EMPTY_OUTCOME",
		},
		{
			// The zero value is not a named outcome; it must still land somewhere sensible.
			name:   "zero value",
			result: DispatchResult{},
			want:   "handler_error:EMPTY_OUTCOME",
		},
		{
			name: "handler failure reports its type",
			result: ClassifyStartOperationDispatch(handlerFailureResponse(
				string(nexus.HandlerErrorTypeBadRequest),
				enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			)),
			want: "handler_error:BAD_REQUEST",
		},
		{
			name: "deprecated handler error reports its type",
			result: ClassifyStartOperationDispatch(&matchingservice.DispatchNexusTaskResponse{
				//nolint:staticcheck // Exercising the deprecated variant on purpose.
				Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
					HandlerError: &nexuspb.HandlerError{
						ErrorType: string(nexus.HandlerErrorTypeNotFound),
					},
				},
			}),
			want: "handler_error:NOT_FOUND",
		},
		{
			// Not a handler error, so there is no type to report and the suffix bounds to UNKNOWN.
			name: "worker failure",
			result: ClassifyStartOperationDispatch(&matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
					Failure: &failurepb.Failure{
						Message: "worker exploded",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeError"},
						},
					},
				},
			}),
			want: "handler_error:UNKNOWN",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tag := tc.result.OutcomeTag()
			require.Equal(t, "outcome", tag.Key)
			require.Equal(t, tc.want, tag.Value)
		})
	}
}

func TestBoundHandlerErrorType(t *testing.T) {
	for _, spec := range []string{
		"BAD_REQUEST", "UNAUTHENTICATED", "UNAUTHORIZED", "NOT_FOUND", "REQUEST_TIMEOUT",
		"CONFLICT", "RESOURCE_EXHAUSTED", "INTERNAL", "NOT_IMPLEMENTED", "UNAVAILABLE",
		"UPSTREAM_TIMEOUT",
	} {
		require.Equal(t, spec, boundHandlerErrorType(spec), "spec types pass through verbatim")
	}
	// A worker picks this string, so it must not be able to mint new time series.
	require.Equal(t, "UNKNOWN", boundHandlerErrorType("whatever-the-worker-said"))
	require.Equal(t, "UNKNOWN", boundHandlerErrorType(""))
	require.Equal(t, "UNKNOWN", boundHandlerErrorType("bad_request"), "matching is case sensitive")
}
