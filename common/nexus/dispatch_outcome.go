package nexus

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/metrics"
)

// DispatchOutcome names the arm of matching's DispatchNexusTaskResponse that came back from a
// DispatchNexusTask call. The nested oneofs and the deprecated variants collapse into one flat set of
// cases.
//
// The zero value is the empty string and is not a valid outcome, so a switch over a DispatchOutcome
// needs a default clause. For metric tags use DispatchResult.OutcomeTag, not the string value.
type DispatchOutcome string

const (
	// DispatchOutcomeUnrecognized is a response this build cannot interpret: no outcome set, an
	// outcome variant added after this build, or a worker answer that carries nothing usable.
	DispatchOutcomeUnrecognized DispatchOutcome = "unrecognized-outcome"

	// DispatchOutcomeSyncSuccess means the worker ran the operation to completion inline.
	DispatchOutcomeSyncSuccess DispatchOutcome = "sync-success"

	// DispatchOutcomeAsyncSuccess means the worker started the operation; the result arrives later,
	// out of band.
	DispatchOutcomeAsyncSuccess DispatchOutcome = "async-success"

	// DispatchOutcomeCancelAccepted means the worker accepted the cancellation request.
	DispatchOutcomeCancelAccepted DispatchOutcome = "cancel-accepted"

	// DispatchOutcomeOperationFailure means the worker ran the operation and it failed or was
	// canceled. The task was handled; this is the handler's answer, not a delivery problem.
	DispatchOutcomeOperationFailure DispatchOutcome = "operation-failure"

	// DispatchOutcomeOperationFailureDeprecated is DispatchOutcomeOperationFailure as reported by a
	// worker predating Temporal failure responses.
	DispatchOutcomeOperationFailureDeprecated DispatchOutcome = "operation-failure-deprecated"

	// DispatchOutcomeHandlerFailure means the worker refused the task with a Nexus handler error,
	// whose retry behavior says whether another attempt is worthwhile.
	DispatchOutcomeHandlerFailure DispatchOutcome = "nexus-handler-failure"

	// DispatchOutcomeWorkerFailure means the worker failed the task with a failure that is not a
	// Nexus handler error, e.g. an application error sent via RespondNexusTaskFailed.
	DispatchOutcomeWorkerFailure DispatchOutcome = "worker-failure"

	// DispatchOutcomeHandlerFailureDeprecated is DispatchOutcomeHandlerFailure as reported by a worker
	// predating Temporal failure responses.
	DispatchOutcomeHandlerFailureDeprecated DispatchOutcome = "nexus-handler-failure-deprecated"

	// DispatchOutcomeRequestTimeout means matching gave up before the task was answered: no worker
	// was polling the task queue, or a worker took the task and never responded.
	DispatchOutcomeRequestTimeout DispatchOutcome = "request-timeout"
)

// Succeeded reports whether the worker accepted the request. An asynchronous start counts: the worker
// took responsibility for the operation, even though it has not finished it.
func (o DispatchOutcome) Succeeded() bool {
	switch o {
	case DispatchOutcomeSyncSuccess, DispatchOutcomeAsyncSuccess, DispatchOutcomeCancelAccepted:
		return true
	default:
		return false
	}
}

// DispatchResult is the classified form of a DispatchNexusTaskResponse: the outcome, plus whatever
// that arm of the response carried, hoisted out of the nested oneofs.
//
// Exactly one field group is populated, determined by Outcome. Everything else is nil or empty.
type DispatchResult struct {
	Outcome DispatchOutcome

	// SyncPayload is the operation's result. Set for DispatchOutcomeSyncSuccess, where it may still
	// be nil: an operation is allowed to succeed with no value.
	SyncPayload *commonpb.Payload

	// OperationToken is set for DispatchOutcomeAsyncSuccess, with the deprecated operation ID
	// already folded in.
	OperationToken string

	// Links are the handler links the worker attached to a successful start. Set for
	// DispatchOutcomeSyncSuccess and DispatchOutcomeAsyncSuccess.
	Links []*nexuspb.Link

	// Failure is the Temporal failure the worker reported. Set for DispatchOutcomeHandlerFailure,
	// DispatchOutcomeWorkerFailure and DispatchOutcomeOperationFailure.
	//
	// This aliases the proto inside the response rather than copying it, so callers that convert it
	// in place mutate the response too.
	Failure *failurepb.Failure

	// HandlerError is set only for DispatchOutcomeHandlerFailureDeprecated.
	HandlerError *nexuspb.HandlerError

	// OperationError is set only for DispatchOutcomeOperationFailureDeprecated.
	OperationError *nexuspb.UnsuccessfulOperationError
}

// handlerErrorType returns the Nexus handler error type the worker reported, or "" when the outcome is
// not a handler error.
func (r DispatchResult) handlerErrorType() string {
	switch r.Outcome {
	case DispatchOutcomeHandlerFailure:
		return r.Failure.GetNexusHandlerFailureInfo().GetType()
	case DispatchOutcomeHandlerFailureDeprecated:
		//nolint:staticcheck // Deprecated field on a deprecated variant.
		return r.HandlerError.GetErrorType()
	default:
		return ""
	}
}

// ClassifyStartOperationDispatch classifies matching's response to a dispatched StartOperation task.
func ClassifyStartOperationDispatch(resp *matchingservice.DispatchNexusTaskResponse) DispatchResult {
	return classifyDispatchNexusTaskResponse(resp, classifyStartOperationResponse)
}

// ClassifyCancelOperationDispatch classifies matching's response to a dispatched CancelOperation task.
func ClassifyCancelOperationDispatch(resp *matchingservice.DispatchNexusTaskResponse) DispatchResult {
	return classifyDispatchNexusTaskResponse(
		resp,
		func(*nexuspb.StartOperationResponse) DispatchResult {
			// A cancel response carries no fields, so any response means the worker accepted.
			return DispatchResult{Outcome: DispatchOutcomeCancelAccepted}
		})
}

// classifyDispatchNexusTaskResponse converts a DispatchNexusTaskResponse into a DispatchResult object.
func classifyDispatchNexusTaskResponse(
	resp *matchingservice.DispatchNexusTaskResponse,
	onResponseFn func(*nexuspb.StartOperationResponse) DispatchResult,
) DispatchResult {
	switch t := resp.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		// A handler error is a Nexus-level refusal whose retry behavior is meaningful; anything else
		// is an arbitrary failure the worker chose to report.
		outcome := DispatchOutcomeWorkerFailure
		if t.Failure.GetNexusHandlerFailureInfo() != nil {
			outcome = DispatchOutcomeHandlerFailure
		}
		return DispatchResult{Outcome: outcome, Failure: t.Failure}

	case *matchingservice.DispatchNexusTaskResponse_HandlerError: //nolint:staticcheck // Deprecated, still sent by older workers.
		return DispatchResult{
			Outcome: DispatchOutcomeHandlerFailureDeprecated,
			//nolint:staticcheck // Deprecated field on a deprecated variant.
			HandlerError: t.HandlerError,
		}

	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		return DispatchResult{Outcome: DispatchOutcomeRequestTimeout}

	case *matchingservice.DispatchNexusTaskResponse_Response:
		// How we handle the "Response" field depends on the context. (i.e. if the Nexus task was to
		// start a new operation or cancel an existing one.)
		return onResponseFn(t.Response.GetStartOperation())

	default:
		return DispatchResult{Outcome: DispatchOutcomeUnrecognized}
	}
}

// classifyStartOperationResponse classifies the answer a worker gave to a StartOperation request.
func classifyStartOperationResponse(resp *nexuspb.StartOperationResponse) DispatchResult {
	switch t := resp.GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		return DispatchResult{
			Outcome:     DispatchOutcomeSyncSuccess,
			SyncPayload: t.SyncSuccess.GetPayload(),
			Links:       t.SyncSuccess.GetLinks(),
		}

	case *nexuspb.StartOperationResponse_AsyncSuccess:
		token := t.AsyncSuccess.GetOperationToken()
		if token == "" {
			// Workers predating the operation-token rename only set the operation ID.
			//nolint:staticcheck // Deprecated, still sent by older workers.
			token = t.AsyncSuccess.GetOperationId()
		}
		return DispatchResult{
			Outcome:        DispatchOutcomeAsyncSuccess,
			OperationToken: token,
			Links:          t.AsyncSuccess.GetLinks(),
		}

	case *nexuspb.StartOperationResponse_Failure:
		return DispatchResult{
			Outcome: DispatchOutcomeOperationFailure,
			Failure: t.Failure,
		}

	case *nexuspb.StartOperationResponse_OperationError: //nolint:staticcheck // Deprecated, still sent by older workers.
		return DispatchResult{
			Outcome: DispatchOutcomeOperationFailureDeprecated,
			//nolint:staticcheck // Deprecated field on a deprecated variant.
			OperationError: t.OperationError,
		}

	default:
		return DispatchResult{Outcome: DispatchOutcomeUnrecognized}
	}
}

// OutcomeTag returns the metrics outcome tag for a dispatch.
//
// The handler-error suffix is bounded by boundHandlerErrorType(). A worker failure that is not a handler
// error or has a non-spec type will be reported as "handler_error:UNKNOWN".
func (r DispatchResult) OutcomeTag() metrics.Tag {
	return metrics.OutcomeTag(r.metricOutcome())
}

func (r DispatchResult) metricOutcome() string {
	// NOTE: Some of these are confusing (e.g. "success" for CancelAccepted), but
	// changing these would break existing dashboards.
	switch r.Outcome {
	case DispatchOutcomeSyncSuccess:
		return "sync_success"
	case DispatchOutcomeAsyncSuccess:
		return "async_success"
	case DispatchOutcomeCancelAccepted:
		return "success"
	case DispatchOutcomeOperationFailure:
		return "failure"
	case DispatchOutcomeOperationFailureDeprecated:
		return "operation_error"
	case DispatchOutcomeHandlerFailure,
		DispatchOutcomeWorkerFailure,
		DispatchOutcomeHandlerFailureDeprecated:
		// A worker failure has no handler error type to report and will map to UNKNOWN.
		return "handler_error:" + boundHandlerErrorType(r.handlerErrorType())
	case DispatchOutcomeRequestTimeout:
		return "handler_timeout"
	default:
		return "handler_error:EMPTY_OUTCOME"
	}
}

// handlerErrorTypes are the handler error types that may appear verbatim in a metric tag.
// Keep in sync with the HandlerErrorType consts in nexus-rpc/sdk-go/nexus/errors.go.
var handlerErrorTypes = map[string]struct{}{
	string(nexus.HandlerErrorTypeBadRequest):        {},
	string(nexus.HandlerErrorTypeUnauthenticated):   {},
	string(nexus.HandlerErrorTypeUnauthorized):      {},
	string(nexus.HandlerErrorTypeNotFound):          {},
	string(nexus.HandlerErrorTypeRequestTimeout):    {},
	string(nexus.HandlerErrorTypeConflict):          {},
	string(nexus.HandlerErrorTypeResourceExhausted): {},
	string(nexus.HandlerErrorTypeInternal):          {},
	string(nexus.HandlerErrorTypeNotImplemented):    {},
	string(nexus.HandlerErrorTypeUnavailable):       {},
	string(nexus.HandlerErrorTypeUpstreamTimeout):   {},
}

// boundHandlerErrorType bounds the metric cardinality a worker can introduce through a handler error
// type. Types in the Nexus spec pass through; anything else, including the empty string, collapses to
// UNKNOWN.
func boundHandlerErrorType(errType string) string {
	if _, ok := handlerErrorTypes[errType]; ok {
		return errType
	}
	return "UNKNOWN"
}
