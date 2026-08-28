package nexus

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/matchingservice/v1"
)

// MatchingDispatchResponseToError converts a DispatchNexusTaskResponse proto into a Go error.
// Returns nil if the response indicates success.
//
// For failure cases (worker explicitly returned an error), the Temporal SDK's failure
// converter is used to produce standard Go errors (ApplicationError, CanceledError).
// For transport-level issues (timeout, internal), a nexus.HandlerError is returned
// so the caller can check Retryable().
func MatchingDispatchResponseToError(resp *matchingservice.DispatchNexusTaskResponse) error {
	return DispatchResultToError(ClassifyStartOperationDispatch(resp))
}

// DispatchResultToError converts a classified dispatch into a Go error for server-internal
// consumption. Returns nil for the success outcomes.
//
// The errors this produces are SDK-shaped: a failure the worker reported becomes a
// *temporal.ApplicationError or *temporal.CanceledError, and a Nexus handler error becomes a
// *nexus.HandlerError whose Retryable() says whether re-delivering is worthwhile.
//
// Wire-facing paths convert through nexusrpc instead. The Nexus HTTP handler re-serializes a returned
// error with nexusrpc's failure converter, which flattens an SDK-shaped cause to just its message and
// drops the failure type and details.
func DispatchResultToError(result DispatchResult) error {
	if result.Outcome.Succeeded() {
		return nil
	}

	switch result.Outcome {
	case DispatchOutcomeHandlerFailure,
		DispatchOutcomeWorkerFailure,
		DispatchOutcomeOperationFailure:
		// The worker either failed the task (via RespondNexusTaskFailed) or answered with a failed
		// operation. Either way the failure proto is the worker's own.
		return temporal.GetDefaultFailureConverter().FailureToError(result.Failure)

	case DispatchOutcomeHandlerFailureDeprecated:
		return ProtoHandlerErrorToNexusHandlerError(result.HandlerError)

	case DispatchOutcomeOperationFailureDeprecated:
		// Same as DispatchOutcomeOperationFailure; the older worker just encoded its failure as a
		// Nexus failure rather than a Temporal one, so re-encode before converting.
		//nolint:staticcheck // Deprecated function still in use for backward compatibility.
		nf := ProtoFailureToNexusFailure(result.OperationError.GetFailure())
		failure, err := NexusFailureToTemporalFailure(nf)
		if err != nil {
			return nexus.NewHandlerErrorf(
				nexus.HandlerErrorTypeInternal, "malformed operation error: %v", err)
		}
		//nolint:staticcheck // Deprecated field on a deprecated variant.
		state := nexus.OperationState(result.OperationError.GetOperationState())
		return temporal.GetDefaultFailureConverter().FailureToError(applyOperationState(state, failure))

	case DispatchOutcomeRequestTimeout:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")

	default:
		return nexus.NewHandlerErrorf(
			nexus.HandlerErrorTypeInternal,
			"unsupported or unrecognized dispatch outcome: %s", result.Outcome,
		)
	}
}

// applyOperationState makes a failure carry the operation state the deprecated response reported in a
// separate field. Only the current response variant encodes cancellation in the failure itself, so a
// handler outside Temporal sends a failure with no Temporal failure info and the state would
// otherwise be lost. The original failure is kept as the cause.
func applyOperationState(state nexus.OperationState, failure *failurepb.Failure) *failurepb.Failure {
	if state != nexus.OperationStateCanceled || failure.GetCanceledFailureInfo() != nil {
		return failure
	}
	return &failurepb.Failure{
		Message: failure.GetMessage(),
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
		},
		Cause: failure,
	}
}

// ProtoHandlerErrorToNexusHandlerError converts the deprecated proto handler error a worker predating
// Temporal failure responses reports into a Nexus SDK handler error.
func ProtoHandlerErrorToNexusHandlerError(handlerErr *nexuspb.HandlerError) *nexus.HandlerError {
	var retryBehavior nexus.HandlerErrorRetryBehavior
	// nolint:exhaustive // unspecified is the default
	//revive:disable-next-line:enforce-switch-style // Unspecified leaves the zero value in place.
	switch handlerErr.GetRetryBehavior() {
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
	}
	// nolint:staticcheck // Deprecated function still in use for backward compatibility.
	cause := ProtoFailureToNexusFailure(handlerErr.GetFailure())
	return &nexus.HandlerError{
		// nolint:staticcheck // Deprecated field on a deprecated variant.
		Type:          nexus.HandlerErrorType(handlerErr.GetErrorType()),
		RetryBehavior: retryBehavior,
		Cause:         &nexus.FailureError{Failure: cause},
	}
}
