package asr

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestElevenLabsRealtimeSessionProtocol(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if err := verifyElevenLabsHandshake(request); err != nil {
			serverErr <- err
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := conn.WriteJSON(map[string]any{
			realtimeFieldMessageType: elevenLabsMessageSessionStarted,
			"session_id":             "eleven-session-server",
			"config":                 map[string]any{realtimeFieldSampleRate: 16000},
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyElevenLabsAudioChunk(conn, 3200, false); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{
			realtimeFieldMessageType: elevenLabsMessagePartial,
			"text":                   "Hello wor",
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyElevenLabsAudioChunk(conn, 3200, true); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{
			realtimeFieldMessageType: elevenLabsMessageCommitted,
			"text":                   "Hello world.",
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			realtimeFieldMessageType: elevenLabsMessageCommittedWithTimestamps,
			"text":                   "Hello world.",
			"language_code":          "en",
			"words": []map[string]any{{
				"text": "Hello", "start": 0.1, "end": 0.4, "type": "word",
			}, {
				"text": "world", "start": 0.5, "end": 0.9, "type": "word",
			}},
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _, _ = conn.ReadMessage()
		serverErr <- nil
	}))
	defer server.Close()

	provider, err := NewElevenLabsRealtimeProvider(ElevenLabsRealtimeConfig{
		Endpoint:      server.URL,
		APIKey:        "test-key",
		EmitPartials:  true,
		FinishTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new ElevenLabs realtime provider: %v", err)
	}
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID:  "eleven-session-client",
			Language:   "en-US",
			SampleRate: 16000,
			Channels:   1,
			Format:     AudioFormatRawPCM16,
			Context: RecognitionContext{
				Prompt: " Earlier context. ",
				Terms:  []string{"ElevenLabs", "Scribe", "ElevenLabs", "term that is longer than twenty runes"},
			},
		},
		ChunkDuration: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new ElevenLabs realtime session: %v", err)
	}
	defer session.Close()
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 1600)}); err != nil {
		t.Fatalf("push ElevenLabs audio: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish ElevenLabs audio: %v", err)
	}

	events := make([]Event, 0, 4)
	for event := range session.Events() {
		events = append(events, event)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait ElevenLabs session: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("mock ElevenLabs server: %v", err)
	}
	assertElevenLabsRealtimeEvents(t, events)
}

func TestElevenLabsRealtimeProviderDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	provider, err := NewElevenLabsRealtimeProvider(ElevenLabsRealtimeConfig{APIKey: "test-key"})
	if err != nil {
		t.Fatalf("new ElevenLabs provider: %v", err)
	}
	if provider.Name() != defaultElevenLabsRealtimeName ||
		provider.Model() != defaultElevenLabsRealtimeModel ||
		provider.cfg.Endpoint != defaultElevenLabsRealtimeEndpoint ||
		provider.cfg.CommitStrategy != ElevenLabsCommitStrategyVAD ||
		provider.cfg.VADSilenceThreshold != 300*time.Millisecond ||
		*provider.cfg.VADThreshold != defaultElevenLabsVADThreshold ||
		provider.cfg.MinSpeechDuration != defaultElevenLabsMinSpeech ||
		provider.cfg.MinSilenceDuration != defaultElevenLabsMinSilence ||
		provider.cfg.ManualCommitInterval != defaultElevenLabsManualCommit ||
		provider.cfg.EmitPartials ||
		*provider.cfg.MinTranscriptLogProb != defaultElevenLabsMinTranscriptLogProb ||
		!provider.ServerVADEnabled() {
		t.Fatalf("unexpected ElevenLabs defaults: %+v", provider)
	}
	capabilities := provider.StreamingCapabilities()
	if len(capabilities.SampleRates) != 6 || capabilities.SupportsPrompt ||
		!capabilities.SupportsTerms || !capabilities.SupportsServerVAD ||
		capabilities.SupportsLanguageHints {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	thresholdBelowMinimum := 0.09
	logProbAboveMaximum := 0.1
	invalidConfigs := []ElevenLabsRealtimeConfig{
		{},
		{APIKey: "test-key", CommitStrategy: "semantic"},
		{APIKey: "test-key", VADSilenceThreshold: 200 * time.Millisecond},
		{APIKey: "test-key", VADThreshold: &thresholdBelowMinimum},
		{APIKey: "test-key", MinTranscriptLogProb: &logProbAboveMaximum},
		{APIKey: "test-key", MinSpeechDuration: 20 * time.Millisecond},
		{APIKey: "test-key", MinSilenceDuration: 3 * time.Second},
		{APIKey: "test-key", ManualCommitInterval: 500 * time.Millisecond},
		{APIKey: "test-key", FilterBackgroundAudio: true},
		{APIKey: "test-key", Endpoint: "file:///tmp/elevenlabs.sock"},
	}
	for index, config := range invalidConfigs {
		if _, err := NewElevenLabsRealtimeProvider(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
	if _, err := NewElevenLabsRealtimeProvider(ElevenLabsRealtimeConfig{
		APIKey: "test-key", FilterBackgroundAudio: true, DisableTimestamps: true,
	}); err != nil {
		t.Fatalf("background filtering without timestamps must be valid: %v", err)
	}
}

func TestElevenLabsRejectsInvalidFinalAndRetractsPreview(t *testing.T) {
	t.Parallel()
	provider, err := NewElevenLabsRealtimeProvider(ElevenLabsRealtimeConfig{
		APIKey:       "test-key",
		EmitPartials: true,
	})
	if err != nil {
		t.Fatalf("new ElevenLabs provider: %v", err)
	}
	stream := &elevenLabsRealtimeStream{
		provider: provider,
		request:  StreamingRequest{SessionID: "filter-test"},
		ctx:      context.Background(),
		events:   make(chan ProviderStreamEvent, 8),
	}
	stream.handleServerEvent(elevenLabsServerEvent{
		MessageType: elevenLabsMessagePartial,
		Text:        "好。",
	})
	stream.handleServerEvent(elevenLabsServerEvent{
		MessageType: elevenLabsMessageCommitted,
		Text:        "。",
	})
	stream.handleServerEvent(elevenLabsServerEvent{
		MessageType: elevenLabsMessageCommittedWithTimestamps,
		Text:        "。",
		Words:       []elevenLabsTranscriptWord{{Text: "。", LogProb: -11.25}},
	})

	started := <-stream.events
	preview := <-stream.events
	discarded := <-stream.events
	if !started.Started || started.ResultID == "" || preview.Text != "好。" ||
		!discarded.Discarded || discarded.ResultID != started.ResultID {
		t.Fatalf("provider events = %+v %+v %+v", started, preview, discarded)
	}
	stream.handleServerEvent(elevenLabsServerEvent{
		MessageType: elevenLabsMessagePartial,
		Text:        "Linda。",
	})
	stream.handleServerEvent(elevenLabsServerEvent{
		MessageType: elevenLabsMessageCommitted,
	})
	stream.handleServerEvent(elevenLabsServerEvent{
		MessageType: elevenLabsMessageCommittedWithTimestamps,
	})
	emptyFinalStarted := <-stream.events
	emptyFinalPreview := <-stream.events
	emptyFinalDiscarded := <-stream.events
	if !emptyFinalStarted.Started || emptyFinalPreview.Text != "Linda。" ||
		!emptyFinalDiscarded.Discarded || emptyFinalDiscarded.ResultID != emptyFinalStarted.ResultID {
		t.Fatalf(
			"empty final events = %+v %+v %+v",
			emptyFinalStarted,
			emptyFinalPreview,
			emptyFinalDiscarded,
		)
	}

	session := &RealtimeSession{
		provider: provider,
		cfg: RealtimeSessionConfig{Request: StreamingRequest{
			SessionID: "filter-test",
		}},
		ctx:    context.Background(),
		events: make(chan Event, 2),
		items:  make(map[string]*realtimeResultState),
	}
	session.emitTranscript(ProviderStreamEvent{
		ResultID: started.ResultID,
		Started:  true,
		Text:     preview.Text,
	})
	session.discardTranscript(discarded)
	previewEvent := <-session.events
	discardEvent := <-session.events
	if previewEvent.Segment == nil || previewEvent.Segment.State != TranscriptStatePreview ||
		discardEvent.Segment == nil || discardEvent.Segment.State != TranscriptStateDiscarded ||
		discardEvent.Segment.SegmentIndex != previewEvent.Segment.SegmentIndex ||
		discardEvent.Segment.Revision != previewEvent.Segment.Revision+1 {
		t.Fatalf("session events = %+v %+v", previewEvent, discardEvent)
	}
}

func TestElevenLabsDefaultSuppressesPartials(t *testing.T) {
	t.Parallel()
	provider, err := NewElevenLabsRealtimeProvider(ElevenLabsRealtimeConfig{APIKey: "test-key"})
	if err != nil {
		t.Fatalf("new ElevenLabs provider: %v", err)
	}
	stream := &elevenLabsRealtimeStream{
		provider: provider,
		request:  StreamingRequest{SessionID: "stable-only"},
		ctx:      context.Background(),
		events:   make(chan ProviderStreamEvent, 4),
	}
	stream.handleServerEvent(elevenLabsServerEvent{
		MessageType: elevenLabsMessagePartial,
		Text:        "Lin。",
	})
	if len(stream.events) != 0 {
		t.Fatalf("default provider emitted partial: %+v", <-stream.events)
	}
	stream.handleServerEvent(elevenLabsServerEvent{
		MessageType: elevenLabsMessageCommittedWithTimestamps,
		Text:        "Where are you going?",
		Words: []elevenLabsTranscriptWord{
			{Text: "Where", LogProb: -0.2},
			{Text: "are", LogProb: -0.01},
			{Text: "you", LogProb: -0.01},
			{Text: "going?", LogProb: -1.5},
		},
	})
	started := <-stream.events
	stable := <-stream.events
	if !started.Started || stable.ResultID != started.ResultID || !stable.IsFinal ||
		stable.Text != "Where are you going?" {
		t.Fatalf("stable-only events = %+v %+v", started, stable)
	}
}

func TestElevenLabsRealtimeManualCommitAndEmptyTail(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("commit_strategy") != ElevenLabsCommitStrategyManual {
			serverErr <- ErrProviderRequest
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := conn.WriteJSON(map[string]any{
			realtimeFieldMessageType: elevenLabsMessageSessionStarted, "session_id": "manual-server",
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyElevenLabsAudioChunk(conn, 3200, true); err != nil {
			serverErr <- err
			return
		}
		if err := verifyElevenLabsAudioChunk(conn, 3200, true); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{
			realtimeFieldMessageType: elevenLabsMessageCommitted, "text": "Manual result.",
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			realtimeFieldMessageType: elevenLabsErrorInsufficientAudio, "error": "no speech in tail",
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _, _ = conn.ReadMessage()
		serverErr <- nil
	}))
	defer server.Close()

	provider, err := NewElevenLabsRealtimeProvider(ElevenLabsRealtimeConfig{
		Endpoint: server.URL, APIKey: "test-key", CommitStrategy: ElevenLabsCommitStrategyManual,
		DisableTimestamps: true, FinishTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new manual ElevenLabs provider: %v", err)
	}
	// Use one chunk as the periodic interval so the protocol test stays small.
	provider.cfg.ManualCommitInterval = 100 * time.Millisecond
	if provider.ServerVADEnabled() {
		t.Fatal("manual ElevenLabs provider must not report server VAD")
	}
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID: "manual-client", Language: "auto", SampleRate: 16000,
			Channels: 1, Format: AudioFormatRawPCM16,
		},
		ChunkDuration: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new manual ElevenLabs session: %v", err)
	}
	defer session.Close()
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 1600)}); err != nil {
		t.Fatalf("push manual ElevenLabs audio: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish manual ElevenLabs audio: %v", err)
	}

	events := make([]Event, 0, 3)
	for event := range session.Events() {
		events = append(events, event)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait manual ElevenLabs session: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("mock manual ElevenLabs server: %v", err)
	}
	if len(events) != 3 || events[0].Type != EventSessionReady ||
		events[1].Segment == nil || events[1].Segment.Text != "Manual result." ||
		events[1].Segment.State != TranscriptStateStable || events[2].Type != EventCompleted {
		t.Fatalf("events = %+v", events)
	}
}

func TestElevenLabsHelpers(t *testing.T) {
	t.Parallel()
	providerErr := elevenLabsProviderError{
		messageType: elevenLabsErrorAuth,
		detail:      "missing permission speech_to_text",
	}
	if got := providerErr.Error(); got != "elevenlabs realtime provider error: auth_error: missing permission speech_to_text" {
		t.Fatalf("provider error = %q", got)
	}
	for _, text := range []string{"...", "！？…", "  --  "} {
		if elevenLabsTranscriptHasContent(text) {
			t.Fatalf("punctuation-only transcript %q must not have content", text)
		}
	}
	for _, text := range []string{"Where are you going?", "去哪里？", "[laughter]", "123"} {
		if !elevenLabsTranscriptHasContent(text) {
			t.Fatalf("transcript %q must have content", text)
		}
	}
	if elevenLabsTranscriptAccepted("明天。", []elevenLabsTranscriptWord{
		{Text: "明", LogProb: -13.4375},
		{Text: "天", LogProb: -3.546875},
		{Text: "。", LogProb: -5.8125},
	}, defaultElevenLabsMinTranscriptLogProb) {
		t.Fatal("low-confidence transcript must be rejected")
	}
	if !elevenLabsTranscriptAccepted("好。", []elevenLabsTranscriptWord{
		{Text: "好", LogProb: -0.1},
		{Text: "。", LogProb: -2},
	}, defaultElevenLabsMinTranscriptLogProb) {
		t.Fatal("high-confidence short transcript must be accepted")
	}
	if got := elevenLabsAudioFormat(16000); got != "pcm_16000" {
		t.Fatalf("audio format = %q", got)
	}
	if got := elevenLabsAudioFormat(11025); got != "" {
		t.Fatalf("unsupported audio format = %q", got)
	}
	terms := elevenLabsKeyterms([]string{
		" Scribe ", "Scribe", "实时识别", "term that is longer than twenty runes",
	})
	if !slices.Equal(terms, []string{"Scribe", "实时识别"}) {
		t.Fatalf("keyterms = %v", terms)
	}
	start, end := elevenLabsWordRange([]elevenLabsTranscriptWord{
		{Start: 0.2, End: 0.5}, {Start: 0.6, End: 1.1}, {Start: -1, End: 0},
	})
	if start != 200*time.Millisecond || end != 1100*time.Millisecond {
		t.Fatalf("word range = %s-%s", start, end)
	}
	errorCases := map[string]error{
		elevenLabsErrorAuth:          ErrUnauthorized,
		elevenLabsErrorQuota:         ErrRateLimited,
		elevenLabsErrorQueueOverflow: ErrProviderUnavailable,
		elevenLabsErrorInput:         ErrProviderRequest,
		elevenLabsErrorTranscriber:   ErrProviderResponse,
	}
	for messageType, want := range errorCases {
		if got := classifyElevenLabsServerError(messageType); !errors.Is(got, want) {
			t.Fatalf("classify %q = %v, want %v", messageType, got, want)
		}
	}
}

func verifyElevenLabsHandshake(request *http.Request) error {
	query := request.URL.Query()
	keyterms := query["keyterms"]
	if request.Header.Get(elevenLabsAPIKeyHeader) != "test-key" ||
		query.Get("model_id") != defaultElevenLabsRealtimeModel ||
		query.Get("audio_format") != "pcm_16000" ||
		query.Get("commit_strategy") != ElevenLabsCommitStrategyVAD ||
		query.Get("include_timestamps") != "true" ||
		query.Get("include_language_detection") != "true" ||
		query.Get("language_code") != "en" ||
		query.Get("vad_silence_threshold_secs") != "0.3" ||
		!slices.Equal(keyterms, []string{"ElevenLabs", "Scribe"}) {
		return ErrProviderRequest
	}
	return nil
}

func verifyElevenLabsAudioChunk(
	conn *websocket.Conn,
	expectedBytes int,
	expectedCommit bool,
) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	data, err := base64.StdEncoding.DecodeString(event["audio_base_64"].(string))
	if err != nil || event[realtimeFieldMessageType] != elevenLabsMessageInputAudio ||
		len(data) != expectedBytes || event["commit"] != expectedCommit ||
		event[realtimeFieldSampleRate] != float64(16000) {
		return ErrProviderRequest
	}
	if _, hasPreviousText := event["previous_text"]; hasPreviousText {
		return ErrProviderRequest
	}
	return nil
}

func assertElevenLabsRealtimeEvents(t *testing.T, events []Event) {
	t.Helper()
	if len(events) != 4 || events[0].Type != EventSessionReady || events[3].Type != EventCompleted {
		t.Fatalf("events = %+v", events)
	}
	if events[1].Segment == nil || events[1].Segment.Text != "Hello wor" ||
		events[1].Segment.State != TranscriptStatePreview {
		t.Fatalf("partial transcript = %+v", events[1])
	}
	if events[2].Segment == nil || events[2].Segment.Text != "Hello world." ||
		events[2].Segment.Revision != 2 || events[2].Segment.State != TranscriptStateStable ||
		events[2].Segment.FinalizationReason != FinalizationProviderFinal {
		t.Fatalf("committed transcript = %+v", events[2])
	}
}
