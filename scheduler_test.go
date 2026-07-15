package asr

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestScheduledRecognizerPreservesAuthoritativeQueueAndCoalescesPreviews(t *testing.T) {
	base := &schedulerBlockingRecognizer{requests: make(chan schedulerPendingRequest, 5)}
	recognizer := newTestScheduledRecognizer(t, base)
	results := make(chan schedulerCallResult, 5)

	startSchedulerCall(recognizer, results, "formal-1", 1, true)
	first := receiveSchedulerRequest(t, base.requests)
	startSchedulerCall(recognizer, results, "formal-3", 3, true)
	startSchedulerCall(recognizer, results, "formal-2", 2, true)
	waitForSchedulerStats(t, recognizer, func(stats SchedulerStats) bool {
		return stats.AuthoritativePending == 2
	})
	startSchedulerCall(recognizer, results, "preview-4", 4, false)
	startSchedulerCall(recognizer, results, "preview-5", 5, false)
	waitForSchedulerStats(t, recognizer, func(stats SchedulerStats) bool {
		return stats.AuthoritativePending == 2 && stats.PreviewPending && stats.Superseded >= 1
	})

	close(first.release)
	second := receiveSchedulerRequest(t, base.requests)
	if second.requestID != "formal-2" {
		t.Fatalf("second request = %q, want formal-2", second.requestID)
	}
	close(second.release)
	third := receiveSchedulerRequest(t, base.requests)
	if third.requestID != "formal-3" {
		t.Fatalf("third request = %q, want formal-3", third.requestID)
	}
	close(third.release)
	preview := receiveSchedulerRequest(t, base.requests)
	if preview.requestID != "preview-5" {
		t.Fatalf("preview request = %q, want preview-5", preview.requestID)
	}
	close(preview.release)

	seen := collectSchedulerResults(t, results, 5)
	for _, id := range []string{"formal-1", "formal-2", "formal-3", "preview-5"} {
		if seen[id] != nil {
			t.Fatalf("%s error = %v", id, seen[id])
		}
	}
	if !errors.Is(seen["preview-4"], ErrRequestSuperseded) {
		t.Fatalf("preview-4 error = %v, want ErrRequestSuperseded", seen["preview-4"])
	}
}

func TestScheduledRecognizerAuthoritativeRequestClearsPreviews(t *testing.T) {
	base := &schedulerBlockingRecognizer{requests: make(chan schedulerPendingRequest, 3)}
	recognizer := newTestScheduledRecognizer(t, base)
	results := make(chan schedulerCallResult, 3)

	startSchedulerCall(recognizer, results, "preview-active", 1, false)
	activePreview := receiveSchedulerRequest(t, base.requests)
	startSchedulerCall(recognizer, results, "preview-pending", 2, false)
	waitForSchedulerStats(t, recognizer, func(stats SchedulerStats) bool {
		return stats.PreviewPending
	})
	startSchedulerCall(recognizer, results, "formal", 2, true)

	formal := receiveSchedulerRequest(t, base.requests)
	if formal.requestID != "formal" || !formal.authoritative {
		t.Fatalf("request after authoritative submission = %+v", formal)
	}
	close(formal.release)
	close(activePreview.release)

	seen := collectSchedulerResults(t, results, 3)
	if seen["formal"] != nil {
		t.Fatalf("formal error = %v", seen["formal"])
	}
	for _, id := range []string{"preview-active", "preview-pending"} {
		if !errors.Is(seen[id], ErrRequestSuperseded) {
			t.Fatalf("%s error = %v, want ErrRequestSuperseded", id, seen[id])
		}
	}
}

func TestNewScheduledRecognizerRejectsInvalidDependencies(t *testing.T) {
	_, err := NewScheduledRecognizer(
		nil, //nolint:staticcheck // Exercise the public constructor's nil guard.
		schedulerStaticRecognizer{name: "test", model: "model"},
	)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context error = %v, want ErrInvalidConfig", err)
	}

	tests := []struct {
		name string
		base Recognizer
	}{
		{name: "nil recognizer"},
		{name: "empty name", base: schedulerStaticRecognizer{model: "model"}},
		{name: "empty model", base: schedulerStaticRecognizer{name: "test"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewScheduledRecognizer(context.Background(), test.base)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

type schedulerCallResult struct {
	err error
	id  string
}

type schedulerPendingRequest struct {
	release       chan struct{}
	requestID     string
	authoritative bool
}

type schedulerBlockingRecognizer struct {
	requests chan schedulerPendingRequest
}

func (r *schedulerBlockingRecognizer) ProviderName() string { return "scheduler-test" }

func (r *schedulerBlockingRecognizer) ProviderModel() string { return "scheduler-model" }

func (r *schedulerBlockingRecognizer) Transcribe(
	ctx context.Context,
	request TranscriptionRequest,
) (TranscriptionResult, error) {
	release := make(chan struct{})
	select {
	case r.requests <- schedulerPendingRequest{
		requestID:     request.RequestID,
		authoritative: request.Authoritative,
		release:       release,
	}:
	case <-ctx.Done():
		return TranscriptionResult{}, errors.Join(ErrProviderUnavailable, ctx.Err())
	}
	select {
	case <-release:
		return TranscriptionResult{
			ProviderResult: ProviderResult{Text: request.RequestID},
			RequestID:      request.RequestID,
		}, nil
	case <-ctx.Done():
		return TranscriptionResult{}, errors.Join(ErrProviderUnavailable, ctx.Err())
	}
}

type schedulerStaticRecognizer struct {
	name  string
	model string
}

func (r schedulerStaticRecognizer) ProviderName() string { return r.name }

func (r schedulerStaticRecognizer) ProviderModel() string { return r.model }

func (r schedulerStaticRecognizer) Transcribe(
	context.Context,
	TranscriptionRequest,
) (TranscriptionResult, error) {
	return TranscriptionResult{}, nil
}

func newTestScheduledRecognizer(t *testing.T, base Recognizer) *ScheduledRecognizer {
	t.Helper()
	recognizer, err := NewScheduledRecognizer(context.Background(), base)
	if err != nil {
		t.Fatalf("new scheduled recognizer: %v", err)
	}
	t.Cleanup(recognizer.Close)
	return recognizer
}

func startSchedulerCall(
	recognizer *ScheduledRecognizer,
	results chan<- schedulerCallResult,
	id string,
	endAt float64,
	authoritative bool,
) {
	go func() {
		_, err := recognizer.Transcribe(context.Background(), TranscriptionRequest{
			RequestID:     id,
			AudioEndAt:    endAt,
			Authoritative: authoritative,
			Samples:       []float32{0},
		})
		results <- schedulerCallResult{id: id, err: err}
	}()
}

func receiveSchedulerRequest(t *testing.T, requests <-chan schedulerPendingRequest) schedulerPendingRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for scheduled request")
		return schedulerPendingRequest{}
	}
}

func collectSchedulerResults(
	t *testing.T,
	results <-chan schedulerCallResult,
	want int,
) map[string]error {
	t.Helper()
	seen := make(map[string]error, want)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(seen) < want {
		select {
		case result := <-results:
			seen[result.id] = result.err
		case <-deadline.C:
			t.Fatalf("scheduler results = %+v", seen)
		}
	}
	return seen
}

func waitForSchedulerStats(
	t *testing.T,
	recognizer *ScheduledRecognizer,
	ready func(SchedulerStats) bool,
) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		stats := recognizer.Stats()
		if ready(stats) {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for scheduler state: %+v", stats)
		}
	}
}
