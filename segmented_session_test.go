package asr

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSegmentedSessionUsesSoftBoundaryForPreviewAndEndForFinal(t *testing.T) {
	const sampleRate = 1_000
	recognizer := &segmentedRecordingRecognizer{requests: make(chan TranscriptionRequest, 8)}
	session, err := NewSegmentedSession(context.Background(), recognizer, SegmentedSessionConfig{
		Session: SessionConfig{
			SessionID:           "segmented-session",
			SampleRate:          sampleRate,
			Channels:            1,
			TailFinalizeSilence: time.Second,
			MaxWindowDuration:   61 * time.Second,
			TailAnchorEnabled:   true,
		},
		MaxBufferedSamples:     30_000,
		IdlePreRollSamples:     100,
		LongSpeechCommitAfter:  30 * time.Second,
		LongSpeechCommitPrefix: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new segmented session: %v", err)
	}
	t.Cleanup(session.Close)

	ctx := context.Background()
	if err := session.Push(ctx, AudioChunk{
		Samples: make([]float32, 25_000),
		Boundaries: []SpeechBoundary{{
			Type: SpeechBoundaryStart, SourceSegmentIndex: 0, StartSample: 0,
		}},
	}); err != nil {
		t.Fatalf("push active speech: %v", err)
	}
	select {
	case request := <-recognizer.requests:
		t.Fatalf("request submitted before boundary: %+v", request)
	default:
	}

	if err := session.Push(ctx, AudioChunk{Boundaries: []SpeechBoundary{{
		Type: SpeechBoundarySoft, SourceSegmentIndex: 0, StartSample: 0, EndSample: 500,
	}}}); err != nil {
		t.Fatalf("push soft boundary: %v", err)
	}
	previewRequest := receiveSegmentedRequest(t, recognizer.requests)
	if !strings.Contains(previewRequest.RequestID, ":intermediate:") || len(previewRequest.Samples) != 500 ||
		previewRequest.Authoritative {
		t.Fatalf("preview request = %+v samples=%d", previewRequest, len(previewRequest.Samples))
	}
	preview := receiveIntermediateEvent(t, session.Events())
	if preview.EndAt != 0.5 || preview.Text == "" {
		t.Fatalf("intermediate result = %+v", preview)
	}

	if err := session.Push(ctx, AudioChunk{Boundaries: []SpeechBoundary{{
		Type: SpeechBoundaryEnd, SourceSegmentIndex: 0, StartSample: 0, EndSample: 25_000,
	}}}); err != nil {
		t.Fatalf("push speech end: %v", err)
	}
	finalRequest := receiveSegmentedRequest(t, recognizer.requests)
	if !strings.Contains(finalRequest.RequestID, ":window:1:") || len(finalRequest.Samples) != 25_000 ||
		!finalRequest.Authoritative {
		t.Fatalf("final request = %+v samples=%d", finalRequest, len(finalRequest.Samples))
	}
	if session.activeSpeech != nil || session.nextSegmentIndex != 1 {
		t.Fatalf("session state = active:%+v next:%d", session.activeSpeech, session.nextSegmentIndex)
	}
}

func TestSegmentedSessionCommitsAgedSoftBoundaryDuringLongSpeech(t *testing.T) {
	const sampleRate = 1_000
	recognizer := &segmentedRecordingRecognizer{requests: make(chan TranscriptionRequest, 8)}
	session, err := NewSegmentedSession(context.Background(), recognizer, SegmentedSessionConfig{
		Session: SessionConfig{
			SessionID:                "long-speech-commit-session",
			SampleRate:               sampleRate,
			Channels:                 1,
			TailFinalizeSilence:      time.Second,
			TailFinalizeResultWait:   time.Second,
			ShortSegmentMaxDuration:  6 * time.Second,
			ShortSegmentNeighborWait: 3 * time.Second,
			MaxWindowDuration:        30 * time.Second,
			TailAnchorEnabled:        true,
		},
		MaxBufferedSamples:     30_000,
		IdlePreRollSamples:     100,
		LongSpeechCommitAfter:  15 * time.Second,
		LongSpeechCommitPrefix: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new segmented session: %v", err)
	}
	t.Cleanup(session.Close)

	ctx := context.Background()
	if err := session.Push(ctx, AudioChunk{
		Samples: make([]float32, 4_500),
		Boundaries: []SpeechBoundary{
			{Type: SpeechBoundaryStart, SourceSegmentIndex: 0, StartSample: 0},
			{Type: SpeechBoundarySoft, SourceSegmentIndex: 0, StartSample: 0, EndSample: 4_000},
		},
	}); err != nil {
		t.Fatalf("push initial long speech: %v", err)
	}
	previewRequest := receiveSegmentedRequest(t, recognizer.requests)
	if previewRequest.Authoritative || len(previewRequest.Samples) != 4_000 {
		t.Fatalf("preview request = %+v samples=%d", previewRequest, len(previewRequest.Samples))
	}
	if err := session.Push(ctx, AudioChunk{
		Samples: make([]float32, 1_500),
		Boundaries: []SpeechBoundary{{
			Type: SpeechBoundarySoft, SourceSegmentIndex: 0, StartSample: 0, EndSample: 5_000,
		}},
	}); err != nil {
		t.Fatalf("push last soft boundary in prefix: %v", err)
	}
	prefixPreview := receiveSegmentedRequest(t, recognizer.requests)
	if prefixPreview.Authoritative || len(prefixPreview.Samples) != 5_000 {
		t.Fatalf("prefix preview request = %+v samples=%d", prefixPreview, len(prefixPreview.Samples))
	}
	if err := session.Push(ctx, AudioChunk{
		Samples: make([]float32, 4_000),
		Boundaries: []SpeechBoundary{{
			Type: SpeechBoundarySoft, SourceSegmentIndex: 0, StartSample: 0, EndSample: 9_000,
		}},
	}); err != nil {
		t.Fatalf("push later soft boundary: %v", err)
	}
	laterPreview := receiveSegmentedRequest(t, recognizer.requests)
	if laterPreview.Authoritative || len(laterPreview.Samples) != 9_000 {
		t.Fatalf("later preview request = %+v samples=%d", laterPreview, len(laterPreview.Samples))
	}

	if err := session.Push(ctx, AudioChunk{Samples: make([]float32, 5_000)}); err != nil {
		t.Fatalf("advance commit watermark: %v", err)
	}
	formalRequest := receiveSegmentedRequest(t, recognizer.requests)
	if !formalRequest.Authoritative || !strings.Contains(formalRequest.RequestID, ":window:1:") ||
		len(formalRequest.Samples) != 5_000 {
		t.Fatalf("formal request = %+v samples=%d", formalRequest, len(formalRequest.Samples))
	}
	if session.activeSpeech == nil || session.activeSpeech.startSample != 5_000 ||
		len(session.activeSpeech.softBoundaries) != 1 || session.activeSpeech.softBoundaries[0] != 9_000 ||
		session.sampleBase != 5_000 ||
		session.nextSegmentIndex != 1 {
		t.Fatalf("post-commit state = active:%+v base:%d next:%d", session.activeSpeech, session.sampleBase, session.nextSegmentIndex)
	}
	result := waitForFinalSegment(t, session.Events(), time.Second)
	if result.SegmentIndex != 0 || result.State != TranscriptStateStable || result.Text != "recognized text" ||
		result.FinalizationReason != FinalizationLongSpeech {
		t.Fatalf("committed result = %+v", result)
	}

	if err := session.Push(ctx, AudioChunk{Samples: make([]float32, 5_000)}); err != nil {
		t.Fatalf("advance second commit watermark: %v", err)
	}
	nextFormal := receiveSegmentedRequest(t, recognizer.requests)
	if !nextFormal.Authoritative || len(nextFormal.Samples) != 4_000 ||
		!strings.Contains(nextFormal.RequestID, ":chain:2:") {
		t.Fatalf("next formal request = %+v samples=%d", nextFormal, len(nextFormal.Samples))
	}
	nextResult := waitForFinalSegment(t, session.Events(), time.Second)
	if nextResult.SegmentIndex != 1 || nextResult.State != TranscriptStateStable ||
		nextResult.FinalizationReason != FinalizationLongSpeech || session.activeSpeech.startSample != 9_000 ||
		session.sampleBase != 9_000 {
		t.Fatalf("second committed result/state = %+v active:%+v base:%d", nextResult, session.activeSpeech, session.sampleBase)
	}
}

func TestSegmentedSessionRejectsInvalidLongSpeechCommitWindow(t *testing.T) {
	_, err := NewSegmentedSession(context.Background(), &segmentedRecordingRecognizer{
		requests: make(chan TranscriptionRequest, 1),
	}, SegmentedSessionConfig{
		Session:                SessionConfig{SessionID: "invalid-commit-window", SampleRate: 1_000, Channels: 1},
		LongSpeechCommitAfter:  5 * time.Second,
		LongSpeechCommitPrefix: 5 * time.Second,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid commit window error = %v, want ErrInvalidConfig", err)
	}
}

func TestSegmentedSessionCompactsIdlePCMAndBoundsActiveSpeech(t *testing.T) {
	recognizer := &segmentedRecordingRecognizer{requests: make(chan TranscriptionRequest, 2)}
	session, err := NewSegmentedSession(context.Background(), recognizer, SegmentedSessionConfig{
		Session:            SessionConfig{SessionID: "buffer-session", SampleRate: 1_000, Channels: 1},
		MaxBufferedSamples: 1_000,
		IdlePreRollSamples: 100,
	})
	if err != nil {
		t.Fatalf("new segmented session: %v", err)
	}
	t.Cleanup(session.Close)

	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 1_000)}); err != nil {
		t.Fatalf("push idle PCM: %v", err)
	}
	if len(session.samples) != 100 || session.sampleBase != 900 {
		t.Fatalf("idle buffer = base:%d samples:%d", session.sampleBase, len(session.samples))
	}
	if err := session.Push(context.Background(), AudioChunk{
		Samples: make([]float32, 800),
		Boundaries: []SpeechBoundary{{
			Type: SpeechBoundaryStart, SourceSegmentIndex: 0, StartSample: 900,
		}},
	}); err != nil {
		t.Fatalf("start speech after idle compaction: %v", err)
	}
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 101)}); !errors.Is(err, ErrPCMBufferLimit) {
		t.Fatalf("active buffer overflow = %v, want ErrPCMBufferLimit", err)
	}
}

func TestSegmentedSessionWaitDoesNotRequireEventConsumption(t *testing.T) {
	recognizer := &segmentedRecordingRecognizer{requests: make(chan TranscriptionRequest, 4)}
	session, err := NewSegmentedSession(context.Background(), recognizer, SegmentedSessionConfig{
		Session: SessionConfig{
			SessionID:              "wait-session",
			SampleRate:             1_000,
			Channels:               1,
			TailFinalizeSilence:    time.Millisecond,
			TailFinalizeResultWait: time.Second,
		},
		MaxBufferedSamples: 2_000,
		IdlePreRollSamples: 100,
	})
	if err != nil {
		t.Fatalf("new segmented session: %v", err)
	}
	t.Cleanup(session.Close)
	if err := session.Push(context.Background(), AudioChunk{
		Samples: make([]float32, 1_000),
		Boundaries: []SpeechBoundary{
			{Type: SpeechBoundaryStart, SourceSegmentIndex: 0, StartSample: 0},
			{Type: SpeechBoundaryEnd, SourceSegmentIndex: 0, StartSample: 0, EndSample: 1_000},
		},
	}); err != nil {
		t.Fatalf("push complete speech: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish session: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait without consuming events: %v", err)
	}
}

func receiveSegmentedRequest(t *testing.T, requests <-chan TranscriptionRequest) TranscriptionRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for request")
		return TranscriptionRequest{}
	}
}

func receiveIntermediateEvent(t *testing.T, events <-chan Event) IntermediateResult {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case event := <-events:
			if event.Type == EventIntermediateResult && event.Intermediate != nil {
				return *event.Intermediate
			}
		case <-deadline.C:
			t.Fatal("timed out waiting for intermediate event")
			return IntermediateResult{}
		}
	}
}

type segmentedRecordingRecognizer struct {
	requests chan TranscriptionRequest
}

func (r *segmentedRecordingRecognizer) ProviderName() string { return "segmented-test" }

func (r *segmentedRecordingRecognizer) ProviderModel() string { return "segmented-model" }

func (r *segmentedRecordingRecognizer) Transcribe(
	_ context.Context,
	request TranscriptionRequest,
) (TranscriptionResult, error) {
	r.requests <- request
	return TranscriptionResult{
		RequestID: request.RequestID,
		ProviderResult: ProviderResult{
			Text:     "recognized text",
			Provider: r.ProviderName(),
			Model:    r.ProviderModel(),
		},
	}, nil
}
