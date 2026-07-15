package asr

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSessionRecoversS1FromAdjacentPairWindows(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			switch {
			case strings.Contains(request.RequestID, ":window:1:"):
				return TranscriptionResult{}, ErrProviderUnavailable
			case strings.Contains(request.RequestID, ":window:2:"):
				return testTranscriptionResult(request, "今天发布星河系统"), nil
			case strings.Contains(request.RequestID, ":window:3:"):
				return testTranscriptionResult(request, "星河系统支持العربية"), nil
			default:
				return TranscriptionResult{}, ErrInvalidRequest
			}
		},
	}
	session := newTestSession(t, recognizer, true)
	defer session.Close()

	ctx := context.Background()
	for index := 1; index <= 3; index++ {
		if err := session.AddSegment(ctx, testSegment(index)); err != nil {
			t.Fatalf("add segment %d: %v", index, err)
		}
	}
	if err := session.Stop(ctx); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	events := collectUntilCompleted(t, session.Events())
	stable := latestStableSegments(events)
	wants := map[int]string{1: "今天发布", 2: "星河系统", 3: "支持العربية"}
	for index, want := range wants {
		if got := stable[index].Text; got != want {
			t.Fatalf("stable S%d = %q, want %q (events: %+v)", index, got, want, events)
		}
	}
	if requests := recognizer.Requests(); len(requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(requests))
	} else {
		for _, request := range requests {
			if request.Context.Prompt != "领域上下文" || len(request.Context.Terms) != 1 || request.Context.Terms[0] != "星河系统" {
				t.Fatalf("context not propagated: %+v", request.Context)
			}
			if request.Language != "auto" || len(request.LanguageHints) != 2 {
				t.Fatalf("language settings not propagated: %+v", request)
			}
		}
	}
}

func TestSessionFinalizesSingleSegmentWithoutWaitingForAnotherVAD(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			return testTranscriptionResult(request, "hello世界"), nil
		},
	}
	session := newTestSession(t, recognizer, true)
	defer session.Close()
	if err := session.AddSegment(context.Background(), testSegment(1)); err != nil {
		t.Fatalf("add segment: %v", err)
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	stable := latestStableSegments(collectUntilCompleted(t, session.Events()))
	if got := stable[1]; got.Text != "hello世界" || got.EvidenceQuality != EvidenceStandalone || got.FinalizationReason != FinalizationAudioStop {
		t.Fatalf("single segment result = %+v", got)
	}
}

func TestSessionKeepsShortSegmentOpenForNeighborContext(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			return testTranscriptionResult(request, request.RequestID), nil
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:                "short-neighbor-session",
		SampleRate:               1_000,
		Channels:                 1,
		ContextSilence:           10 * time.Millisecond,
		TailFinalizeSilence:      10 * time.Millisecond,
		TailFinalizeResultWait:   time.Second,
		ShortSegmentMaxDuration:  2 * time.Second,
		ShortSegmentNeighborWait: 200 * time.Millisecond,
		MaxWindowDuration:        3 * time.Second,
		TailAnchorEnabled:        true,
	})
	if err != nil {
		t.Fatalf("new ASR session: %v", err)
	}
	defer session.Close()

	ctx := context.Background()
	if err := session.AddSegment(ctx, testSegment(1)); err != nil {
		t.Fatalf("add first short segment: %v", err)
	}
	assertNoFinalSegmentWithin(t, session.Events(), 50*time.Millisecond)
	if err := session.SpeechStarted(ctx); err != nil {
		t.Fatalf("continue speech before neighbor deadline: %v", err)
	}
	if err := session.AddSegment(ctx, testSegment(2)); err != nil {
		t.Fatalf("add neighboring segment: %v", err)
	}
	waitForRequestCount(t, recognizer, 2)
	requests := recognizer.Requests()
	if !strings.Contains(requests[1].RequestID, ":chain:1:window:2:1:2") {
		t.Fatalf("neighbor request = %q, want same-chain S1+S2 window", requests[1].RequestID)
	}
	if len(requests[1].Samples) != 2_010 {
		t.Fatalf("neighbor samples = %d, want S1 + context silence + S2", len(requests[1].Samples))
	}
}

func TestSessionFinalizesLongSegmentWithoutNeighborWait(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			return testTranscriptionResult(request, "long segment has enough context"), nil
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:                "long-standalone-session",
		SampleRate:               1_000,
		Channels:                 1,
		TailFinalizeSilence:      10 * time.Millisecond,
		TailFinalizeResultWait:   time.Second,
		ShortSegmentMaxDuration:  2 * time.Second,
		ShortSegmentNeighborWait: 200 * time.Millisecond,
		MaxWindowDuration:        4 * time.Second,
		TailAnchorEnabled:        true,
	})
	if err != nil {
		t.Fatalf("new ASR session: %v", err)
	}
	defer session.Close()

	segment := Segment{
		Index:          1,
		StartAt:        0,
		EndAt:          3,
		StreamDuration: 3,
		Samples:        make([]float32, 3_000),
	}
	if err := session.AddSegment(context.Background(), segment); err != nil {
		t.Fatalf("add long segment: %v", err)
	}
	result := waitForFinalSegment(t, session.Events(), 100*time.Millisecond)
	if result.State != TranscriptStateStable || result.EvidenceQuality != EvidenceStandalone ||
		result.FinalizationReason != FinalizationSilenceTimeout {
		t.Fatalf("long segment final = %+v", result)
	}
}

func TestSessionFinalizesLongSegmentOnNextSpeechWithoutJoiningNewSegment(t *testing.T) {
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseFirst) })
	}
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			if strings.Contains(request.RequestID, ":chain:1:") {
				<-releaseFirst
				return testTranscriptionResult(request, "long segment final"), nil
			}
			return testTranscriptionResult(request, "next segment final"), nil
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:                "long-next-speech-session",
		SampleRate:               1_000,
		Channels:                 1,
		ContextSilence:           10 * time.Millisecond,
		TailFinalizeSilence:      time.Second,
		TailFinalizeResultWait:   time.Second,
		ShortSegmentMaxDuration:  2 * time.Second,
		ShortSegmentNeighborWait: 2 * time.Second,
		MaxWindowDuration:        5 * time.Second,
		TailAnchorEnabled:        true,
	})
	if err != nil {
		t.Fatalf("new ASR session: %v", err)
	}
	defer session.Close()
	defer release()

	ctx := context.Background()
	first := Segment{
		Index:          1,
		StartAt:        0,
		EndAt:          3,
		StreamDuration: 3,
		Samples:        make([]float32, 3_000),
	}
	if err := session.AddSegment(ctx, first); err != nil {
		t.Fatalf("add long segment: %v", err)
	}
	waitForRequestCount(t, recognizer, 1)
	if err := session.SpeechStarted(ctx); err != nil {
		t.Fatalf("start next speech: %v", err)
	}
	second := Segment{
		Index:          2,
		StartAt:        3.1,
		EndAt:          4.1,
		StreamDuration: 4.1,
		Samples:        make([]float32, 1_000),
	}
	if err := session.AddSegment(ctx, second); err != nil {
		t.Fatalf("add next segment while long segment is finalizing: %v", err)
	}
	waitForRequestCount(t, recognizer, 2)
	requests := recognizer.Requests()
	if !strings.Contains(requests[1].RequestID, ":chain:2:window:2:2:2") {
		t.Fatalf("next request = %q, want standalone S2 in a new chain", requests[1].RequestID)
	}
	if len(requests[1].Samples) != 1_000 {
		t.Fatalf("next request samples = %d, want only S2", len(requests[1].Samples))
	}

	release()
	if err := session.Stop(ctx); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	stable := latestStableSegments(collectUntilCompleted(t, session.Events()))
	if result := stable[1]; result.Text != "long segment final" ||
		result.FinalizationReason != FinalizationLongSegment {
		t.Fatalf("long segment result = %+v", result)
	}
	if result := stable[2]; result.Text != "next segment final" ||
		result.FinalizationReason != FinalizationAudioStop {
		t.Fatalf("next segment result = %+v", result)
	}
}

func TestSessionFinalizesShortSegmentAfterNeighborWait(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			return testTranscriptionResult(request, "short final"), nil
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:                "short-timeout-session",
		SampleRate:               1_000,
		Channels:                 1,
		TailFinalizeSilence:      10 * time.Millisecond,
		TailFinalizeResultWait:   time.Second,
		ShortSegmentMaxDuration:  2 * time.Second,
		ShortSegmentNeighborWait: 80 * time.Millisecond,
		MaxWindowDuration:        3 * time.Second,
		TailAnchorEnabled:        true,
	})
	if err != nil {
		t.Fatalf("new ASR session: %v", err)
	}
	defer session.Close()
	if err := session.AddSegment(context.Background(), testSegment(1)); err != nil {
		t.Fatalf("add short segment: %v", err)
	}

	assertNoFinalSegmentWithin(t, session.Events(), 30*time.Millisecond)
	result := waitForFinalSegment(t, session.Events(), 150*time.Millisecond)
	if result.Text != "short final" || result.FinalizationReason != FinalizationSilenceTimeout {
		t.Fatalf("short timeout final = %+v", result)
	}
}

func TestSessionUsesSameProviderTailAnchorForTwoSegments(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			switch {
			case strings.Contains(request.RequestID, ":tail:"):
				return testTranscriptionResult(request, "共享片段"), nil
			case strings.Contains(request.RequestID, ":window:2:"):
				return testTranscriptionResult(request, "前段内容共享片段"), nil
			default:
				return testTranscriptionResult(request, "错误预览"), nil
			}
		},
	}
	session := newTestSession(t, recognizer, true)
	defer session.Close()
	for index := 1; index <= 2; index++ {
		if err := session.AddSegment(context.Background(), testSegment(index)); err != nil {
			t.Fatalf("add segment %d: %v", index, err)
		}
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	stable := latestStableSegments(collectUntilCompleted(t, session.Events()))
	if stable[1].Text != "前段内容" || stable[2].Text != "共享片段" {
		t.Fatalf("tail results = %+v", stable)
	}
	requests := recognizer.Requests()
	if len(requests) != 3 {
		t.Fatalf("requests = %d, want standalone + pair + tail anchor", len(requests))
	}
}

func TestSessionStopCompletesAfterSilenceAlreadySealedChain(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			return testTranscriptionResult(request, "already stable"), nil
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:              "sealed-session",
		SampleRate:             1_000,
		Channels:               1,
		TailFinalizeSilence:    10 * time.Millisecond,
		TailFinalizeResultWait: time.Second,
		MaxWindowDuration:      3 * time.Second,
		TailAnchorEnabled:      true,
	})
	if err != nil {
		t.Fatalf("new ASR session: %v", err)
	}
	defer session.Close()
	if err := session.AddSegment(context.Background(), testSegment(1)); err != nil {
		t.Fatalf("add segment: %v", err)
	}

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	sealed := false
	for !sealed {
		select {
		case event := <-session.Events():
			sealed = event.Segment != nil && event.Segment.State == TranscriptStateStable
		case <-deadline.C:
			t.Fatal("timed out waiting for silence finalization")
		}
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("stop sealed session: %v", err)
	}
	collectUntilCompleted(t, session.Events())
}

func TestSessionRejectsDuplicateSegmentIndex(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			return testTranscriptionResult(request, "segment"), nil
		},
	}
	session := newTestSession(t, recognizer, true)
	defer session.Close()
	segment := testSegment(1)
	if err := session.AddSegment(context.Background(), segment); err != nil {
		t.Fatalf("add first segment: %v", err)
	}
	if err := session.AddSegment(context.Background(), segment); !errors.Is(err, ErrSegmentInvalid) {
		t.Fatalf("duplicate segment error = %v, want ErrSegmentInvalid", err)
	}
}

func TestSessionClassifiesOversizedPairWindow(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			return testTranscriptionResult(request, "segment"), nil
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:              "window-limit-session",
		SampleRate:             1_000,
		Channels:               1,
		ContextSilence:         10 * time.Millisecond,
		TailFinalizeSilence:    time.Second,
		TailFinalizeResultWait: time.Second,
		MaxWindowDuration:      2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new ASR session: %v", err)
	}
	defer session.Close()
	err = session.AddSegment(context.Background(), testSegment(1))
	if err != nil {
		t.Fatalf("add first segment: %v", err)
	}
	err = session.AddSegment(context.Background(), testSegment(2))
	if !errors.Is(err, ErrSegmentInvalid) || !errors.Is(err, ErrWindowTooLong) {
		t.Fatalf("oversized pair error = %v, want segment and window classification", err)
	}
}

func TestSessionAllowsTwoThirtySecondVADSegmentsWithSafetyMargin(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			return testTranscriptionResult(request, "long segment"), nil
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:              "window-margin-session",
		SampleRate:             1_000,
		Channels:               1,
		ContextSilence:         200 * time.Millisecond,
		TailFinalizeSilence:    time.Second,
		TailFinalizeResultWait: time.Second,
		MaxWindowDuration:      65 * time.Second,
	})
	if err != nil {
		t.Fatalf("new ASR session: %v", err)
	}
	defer session.Close()
	segments := []Segment{
		{Index: 1, StartAt: 0, EndAt: 31, StreamDuration: 31, Samples: make([]float32, 31_000)},
		{Index: 2, StartAt: 31.1, EndAt: 61.176, StreamDuration: 61.176, Samples: make([]float32, 30_076)},
	}
	for _, segment := range segments {
		if err := session.AddSegment(context.Background(), segment); err != nil {
			t.Fatalf("add segment %d: %v", segment.Index, err)
		}
	}
}

func TestSessionFinalizesUnresolvedPreviewsAsDegraded(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			switch {
			case strings.Contains(request.RequestID, ":tail:"):
				return testTranscriptionResult(request, "standalone second"), nil
			case strings.Contains(request.RequestID, ":window:2:"):
				return testTranscriptionResult(request, "pair text without shared boundary"), nil
			default:
				return testTranscriptionResult(request, "standalone first"), nil
			}
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:              "degraded-session",
		SampleRate:             1_000,
		Channels:               1,
		ContextSilence:         10 * time.Millisecond,
		TailFinalizeSilence:    time.Second,
		TailFinalizeResultWait: 30 * time.Millisecond,
		MaxWindowDuration:      3 * time.Second,
		TailAnchorEnabled:      true,
	})
	if err != nil {
		t.Fatalf("new ASR session: %v", err)
	}
	defer session.Close()
	for index := 1; index <= 2; index++ {
		if err := session.AddSegment(context.Background(), testSegment(index)); err != nil {
			t.Fatalf("add segment %d: %v", index, err)
		}
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	results := latestStableSegments(collectUntilCompleted(t, session.Events()))
	for index := 1; index <= 2; index++ {
		if result := results[index]; result.State != TranscriptStateDegraded || result.Text == "" {
			t.Fatalf("S%d result = %+v, want non-empty degraded final", index, result)
		}
	}
}

func TestSessionUsesStandaloneTailFallbackWhenThreeSegmentAlignmentFails(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			switch {
			case strings.Contains(request.RequestID, ":tail:"):
				return testTranscriptionResult(request, "final third segment"), nil
			case strings.Contains(request.RequestID, ":window:1:"):
				return testTranscriptionResult(request, "first segment"), nil
			default:
				return testTranscriptionResult(request, "pair without reliable overlap"), nil
			}
		},
	}
	session := newTestSession(t, recognizer, true)
	defer session.Close()
	for index := 1; index <= 3; index++ {
		if err := session.AddSegment(context.Background(), testSegment(index)); err != nil {
			t.Fatalf("add segment %d: %v", index, err)
		}
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	events := collectUntilCompleted(t, session.Events())
	results := latestStableSegments(events)
	if result := results[3]; result.Text != "final third segment" || result.State != TranscriptStateDegraded {
		t.Fatalf("tail fallback result = %+v", result)
	}
	for _, event := range events {
		if event.Error != nil && event.Error.Message == tailFinalizationTimeoutMessage {
			t.Fatalf("unexpected tail timeout event: %+v", event)
		}
	}
}

func TestSessionDoesNotCountPendingRequestQueueTimeAsTailTimeout(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			time.Sleep(80 * time.Millisecond)
			return testTranscriptionResult(request, "delayed final"), nil
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:              "pending-tail-session",
		SampleRate:             1_000,
		Channels:               1,
		TailFinalizeSilence:    time.Millisecond,
		TailFinalizeResultWait: 10 * time.Millisecond,
		MaxWindowDuration:      3 * time.Second,
		TailAnchorEnabled:      true,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	defer session.Close()
	if err := session.AddSegment(context.Background(), testSegment(1)); err != nil {
		t.Fatalf("add segment: %v", err)
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	events := collectUntilCompleted(t, session.Events())
	if result := latestStableSegments(events)[1]; result.Text != "delayed final" || result.State != TranscriptStateStable {
		t.Fatalf("delayed tail result = %+v", result)
	}
	for _, event := range events {
		if event.Error != nil && event.Error.Message == tailFinalizationTimeoutMessage {
			t.Fatalf("pending request was treated as timeout: %+v", event)
		}
	}
}

func TestSessionFinalizesEverySegmentInEightySecondChainWhenAlignmentFails(t *testing.T) {
	recognizer := &recordingRecognizer{
		transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
			switch {
			case strings.Contains(request.RequestID, ":fallback:2"):
				return testTranscriptionResult(request, "segment two fallback"), nil
			case strings.Contains(request.RequestID, ":fallback:3"):
				return testTranscriptionResult(request, "segment three fallback"), nil
			case strings.Contains(request.RequestID, ":tail:"):
				return testTranscriptionResult(request, "segment four tail"), nil
			case strings.Contains(request.RequestID, ":window:1:"):
				return testTranscriptionResult(request, "segment one standalone"), nil
			default:
				return testTranscriptionResult(request, "pair text without separable boundary"), nil
			}
		},
	}
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:              "eighty-second-chain",
		SampleRate:             1_000,
		Channels:               1,
		ContextSilence:         100 * time.Millisecond,
		TailFinalizeSilence:    time.Second,
		TailFinalizeResultWait: time.Second,
		MaxWindowDuration:      65 * time.Second,
		TailAnchorEnabled:      true,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	defer session.Close()
	for index := 1; index <= 4; index++ {
		startAt := float64(index-1) * 20.1
		segment := Segment{
			Index:          index,
			StartAt:        startAt,
			EndAt:          startAt + 20,
			StreamDuration: startAt + 20,
			Samples:        make([]float32, 20_000),
		}
		if err := session.AddSegment(context.Background(), segment); err != nil {
			t.Fatalf("add segment %d: %v", index, err)
		}
	}
	if err := session.Stop(context.Background()); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	events := collectUntilCompleted(t, session.Events())
	results := latestStableSegments(events)
	for index := 1; index <= 4; index++ {
		result, exists := results[index]
		if !exists || result.Text == "" || result.State == TranscriptStatePreview ||
			result.State == TranscriptStateProvisional {
			t.Fatalf("S%d unresolved result = %+v (events: %+v)", index, result, events)
		}
	}
	requests := recognizer.Requests()
	for _, marker := range []string{":fallback:2", ":fallback:3", ":tail:4"} {
		found := false
		for _, request := range requests {
			if strings.Contains(request.RequestID, marker) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing fallback request %q in %+v", marker, requests)
		}
	}
}

func TestComposeWindowSamplesPreservesContiguousAudio(t *testing.T) {
	segments := []Segment{
		{StartAt: 0, EndAt: 1, Samples: make([]float32, 1_000)},
		{StartAt: 1, EndAt: 2, Samples: make([]float32, 1_000)},
	}
	if got := len(composeWindowSamples(segments, 1_000, 200*time.Millisecond)); got != 2_000 {
		t.Fatalf("contiguous window samples = %d, want 2000", got)
	}
	segments[1].StartAt = 1.5
	segments[1].EndAt = 2.5
	if got := len(composeWindowSamples(segments, 1_000, 200*time.Millisecond)); got != 2_200 {
		t.Fatalf("gapped window samples = %d, want 2200", got)
	}
}

func TestLateStandalonePreviewDoesNotOverwriteFinalOrProvisionalResult(t *testing.T) {
	tests := []TranscriptState{TranscriptStateStable, TranscriptStateDegraded, TranscriptStateProvisional}
	for _, state := range tests {
		t.Run(string(state), func(t *testing.T) {
			session := &Session{
				cfg:    SessionConfig{SessionID: "preview-order"},
				events: make(chan Event, 1),
			}
			actor := &sessionActor{session: session}
			chain := &chainState{
				segmentResults: map[int]SegmentResult{
					1: {SegmentIndex: 1, Revision: 2, State: state, Text: "authoritative"},
				},
			}
			actor.publishPreview(chain, windowEvidence{
				task:   windowTask{segments: []Segment{{Index: 1}}},
				result: WindowResult{WindowIndex: 1, Text: "late preview"},
			})
			if got := chain.segmentResults[1]; got.State != state || got.Text != "authoritative" || got.Revision != 2 {
				t.Fatalf("result overwritten by late preview: %+v", got)
			}
			select {
			case event := <-session.events:
				t.Fatalf("unexpected late preview event: %+v", event)
			default:
			}
		})
	}
}

func newTestSession(t *testing.T, recognizer Recognizer, tailAnchor bool) *Session {
	t.Helper()
	session, err := NewSession(recognizer, nil, SessionConfig{
		SessionID:              "test-session",
		Language:               "auto",
		LanguageHints:          []string{"zh-Hans", "ar"},
		Context:                RecognitionContext{Prompt: "领域上下文", Terms: []string{"星河系统"}},
		SampleRate:             1_000,
		Channels:               1,
		ContextSilence:         10 * time.Millisecond,
		TailFinalizeSilence:    10 * time.Second,
		TailFinalizeResultWait: time.Second,
		MaxWindowDuration:      3 * time.Second,
		TailAnchorEnabled:      tailAnchor,
	})
	if err != nil {
		t.Fatalf("new ASR session: %v", err)
	}
	return session
}

func testSegment(index int) Segment {
	start := float64((index - 1) * 2)
	return Segment{
		Index:          index,
		StartAt:        start,
		EndAt:          start + 1,
		StreamDuration: start + 1,
		Samples:        make([]float32, 1_000),
	}
}

func testTranscriptionResult(request TranscriptionRequest, text string) TranscriptionResult {
	return TranscriptionResult{
		RequestID: request.RequestID,
		ProviderResult: ProviderResult{
			Text:     text,
			Provider: "test-provider",
			Model:    "test-model",
		},
	}
}

func collectUntilCompleted(t *testing.T, events <-chan Event) []Event {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	collected := make([]Event, 0, 16)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("event stream closed before completion: %+v", collected)
			}
			collected = append(collected, event)
			if event.Type == EventCompleted {
				return collected
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for completion: %+v", collected)
		}
	}
}

func assertNoFinalSegmentWithin(t *testing.T, events <-chan Event, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if event.Segment != nil && (event.Segment.State == TranscriptStateStable ||
				event.Segment.State == TranscriptStateDegraded) {
				t.Fatalf("segment finalized before neighbor wait elapsed: %+v", *event.Segment)
			}
			if event.RevisionBatch != nil {
				for _, segment := range event.RevisionBatch.Segments {
					if segment.State == TranscriptStateStable || segment.State == TranscriptStateDegraded {
						t.Fatalf("segment finalized before neighbor wait elapsed: %+v", segment)
					}
				}
			}
		case <-timer.C:
			return
		}
	}
}

func waitForFinalSegment(t *testing.T, events <-chan Event, timeout time.Duration) SegmentResult {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if event.Segment != nil && (event.Segment.State == TranscriptStateStable ||
				event.Segment.State == TranscriptStateDegraded) {
				return *event.Segment
			}
		case <-timer.C:
			t.Fatal("timed out waiting for final segment")
			return SegmentResult{}
		}
	}
}

func waitForRequestCount(t *testing.T, recognizer *recordingRecognizer, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for len(recognizer.Requests()) < want {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("requests = %d, want at least %d", len(recognizer.Requests()), want)
		}
	}
}

func latestStableSegments(events []Event) map[int]SegmentResult {
	results := make(map[int]SegmentResult)
	apply := func(segment SegmentResult) {
		if segment.State != TranscriptStateStable && segment.State != TranscriptStateDegraded {
			return
		}
		if existing, ok := results[segment.SegmentIndex]; !ok || segment.Revision >= existing.Revision {
			results[segment.SegmentIndex] = segment
		}
	}
	for _, event := range events {
		if event.Segment != nil {
			apply(*event.Segment)
		}
		if event.RevisionBatch != nil {
			for _, segment := range event.RevisionBatch.Segments {
				apply(segment)
			}
		}
	}
	return results
}

type recordingRecognizer struct {
	mu         sync.Mutex
	requests   []TranscriptionRequest
	transcribe func(TranscriptionRequest) (TranscriptionResult, error)
}

func (r *recordingRecognizer) ProviderName() string { return "test-provider" }

func (r *recordingRecognizer) ProviderModel() string { return "test-model" }

func (r *recordingRecognizer) Transcribe(_ context.Context, request TranscriptionRequest) (TranscriptionResult, error) {
	r.mu.Lock()
	r.requests = append(r.requests, request)
	r.mu.Unlock()
	if r.transcribe == nil {
		return TranscriptionResult{}, errors.New("test recognizer missing transcribe function")
	}
	return r.transcribe(request)
}

func (r *recordingRecognizer) Requests() []TranscriptionRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]TranscriptionRequest(nil), r.requests...)
}
