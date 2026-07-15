package asr

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestGeminiRealtimeSessionProtocol(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("key") != "test-key" {
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, upgradeErr := upgrader.Upgrade(response, request, nil)
		if upgradeErr != nil {
			serverErr <- errors.Join(ErrProviderResponse, upgradeErr)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := verifyGeminiSetup(conn); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyGeminiAudio(conn, 1280); err != nil {
			serverErr <- err
			return
		}
		if err := verifyGeminiAudioStreamEnd(conn); err != nil {
			serverErr <- err
			return
		}
		if err := verifyGeminiAudio(conn, 1280); err != nil {
			serverErr <- err
			return
		}
		if err := verifyGeminiAudioStreamEnd(conn); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(geminiInputTranscriptionEvent("Hello ")); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"serverContent": map[string]any{"generationComplete": true},
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		time.Sleep(10 * time.Millisecond)
		if err := conn.WriteJSON(geminiInputTranscriptionEvent("world.")); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _, _ = conn.ReadMessage()
		serverErr <- nil
	}))
	defer server.Close()

	provider, err := NewGeminiRealtimeProvider(GeminiRealtimeConfig{
		Endpoint:             server.URL,
		APIKey:               "test-key",
		FinalTranscriptDrain: 40 * time.Millisecond,
		FinishIdleTimeout:    200 * time.Millisecond,
		FinishTimeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new Gemini realtime provider: %v", err)
	}
	// Keep the protocol test small while exercising the same long-turn flush
	// path used by the production default.
	provider.cfg.MaxContinuousTurn = 40 * time.Millisecond
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID:  "gemini-session-1",
			Language:   "en-US",
			SampleRate: 16000,
			Channels:   1,
			Format:     AudioFormatRawPCM16,
		},
		ChunkDuration: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new Gemini realtime session: %v", err)
	}
	defer session.Close()
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 1280)}); err != nil {
		t.Fatalf("push Gemini audio: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish Gemini audio: %v", err)
	}

	events := make([]Event, 0, 5)
	for event := range session.Events() {
		events = append(events, event)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait Gemini session: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("mock Gemini server: %v", err)
	}
	assertGeminiRealtimeEvents(t, events)
}

func TestGeminiRealtimeProviderDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	provider, err := NewGeminiRealtimeProvider(GeminiRealtimeConfig{APIKey: "test-key"})
	if err != nil {
		t.Fatalf("new Gemini realtime provider: %v", err)
	}
	if provider.Name() != defaultGeminiRealtimeName || provider.Model() != defaultGeminiRealtimeModel ||
		provider.cfg.Endpoint != defaultGeminiRealtimeEndpoint ||
		provider.cfg.StartOfSpeechSensitivity != GeminiStartSensitivityHigh ||
		provider.cfg.EndOfSpeechSensitivity != GeminiEndSensitivityHigh ||
		provider.cfg.PrefixPadding != defaultGeminiPrefixPadding ||
		provider.cfg.SilenceDuration != 300*time.Millisecond ||
		provider.cfg.MaxContinuousTurn != 15*time.Second || !provider.ServerVADEnabled() {
		t.Fatalf("unexpected Gemini defaults: %+v", provider)
	}
	capabilities := provider.StreamingCapabilities()
	if len(capabilities.SampleRates) != 6 || !capabilities.SupportsServerVAD ||
		capabilities.SupportsPrompt || capabilities.SupportsLanguageHints {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	invalidConfigs := []GeminiRealtimeConfig{
		{},
		{APIKey: "test-key", StartOfSpeechSensitivity: "medium"},
		{APIKey: "test-key", EndOfSpeechSensitivity: "medium"},
		{APIKey: "test-key", PrefixPadding: -time.Millisecond},
		{APIKey: "test-key", SilenceDuration: 500 * time.Microsecond},
		{APIKey: "test-key", MaxContinuousTurn: time.Second},
		{APIKey: "test-key", FinalTranscriptDrain: 3 * time.Second},
		{APIKey: "test-key", FinalTranscriptDrain: time.Second, FinishIdleTimeout: 500 * time.Millisecond},
		{APIKey: "test-key", Endpoint: "file:///tmp/gemini.sock"},
	}
	for index, config := range invalidConfigs {
		if _, err := NewGeminiRealtimeProvider(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
}

func TestNewGeminiRealtimeResamplerSkipsNativeSampleRate(t *testing.T) {
	t.Parallel()
	resampler, err := newGeminiRealtimeResampler(geminiRealtimeSampleRate)
	if err != nil {
		t.Fatalf("create native-rate Gemini resampler: %v", err)
	}
	if resampler != nil {
		t.Fatal("native 16kHz Gemini audio must bypass resampling")
	}

	resampler, err = newGeminiRealtimeResampler(48000)
	if err != nil {
		t.Fatalf("create 48kHz Gemini resampler: %v", err)
	}
	if resampler == nil {
		t.Fatal("non-native Gemini audio must use a resampler")
	}
}

func TestGeminiRealtimeFinalizesTailAfterIdleWithoutBoundary(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := verifyGeminiSetup(conn); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{"setupComplete": map[string]any{}}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyGeminiAudio(conn, 640); err != nil {
			serverErr <- err
			return
		}
		if err := verifyGeminiAudioStreamEnd(conn); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(geminiInputTranscriptionEvent("尾段")); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		_, _, _ = conn.ReadMessage()
		serverErr <- nil
	}))
	defer server.Close()

	provider, err := NewGeminiRealtimeProvider(GeminiRealtimeConfig{
		Endpoint:             server.URL,
		APIKey:               "test-key",
		FinalTranscriptDrain: 20 * time.Millisecond,
		FinishIdleTimeout:    60 * time.Millisecond,
		FinishTimeout:        time.Second,
	})
	if err != nil {
		t.Fatalf("new Gemini realtime provider: %v", err)
	}
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID: "gemini-idle-tail", SampleRate: 16000, Channels: 1, Format: AudioFormatRawPCM16,
		},
		ChunkDuration: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new Gemini realtime session: %v", err)
	}
	defer session.Close()
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 320)}); err != nil {
		t.Fatalf("push Gemini audio: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish Gemini audio: %v", err)
	}
	events := make([]Event, 0, 4)
	for event := range session.Events() {
		events = append(events, event)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait Gemini session: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("mock Gemini server: %v", err)
	}
	if len(events) != 4 || events[1].Segment == nil || events[1].Segment.State != TranscriptStateProvisional ||
		events[2].Segment == nil || events[2].Segment.Text != "尾段" ||
		events[2].Segment.State != TranscriptStateStable || events[3].Type != EventCompleted {
		t.Fatalf("events = %+v", events)
	}
}

func TestAppendGeminiTranscriptSupportsDeltaAndCumulativeEvents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		current string
		next    string
		want    string
	}{
		{current: "", next: "Hello ", want: "Hello "},
		{current: "Hello ", next: "world", want: "Hello world"},
		{current: "Hello", next: "Hello world", want: "Hello world"},
		{current: "你好", next: "你好", want: "你好"},
	}
	for _, test := range tests {
		if got := appendGeminiTranscript(test.current, test.next); got != test.want {
			t.Fatalf("append %q + %q = %q, want %q", test.current, test.next, got, test.want)
		}
	}
}

func TestClassifyGeminiServerError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		serverError geminiServerError
		want        error
	}{
		{serverError: geminiServerError{Code: http.StatusUnauthorized}, want: ErrUnauthorized},
		{serverError: geminiServerError{Status: "RESOURCE_EXHAUSTED"}, want: ErrRateLimited},
		{serverError: geminiServerError{Status: "UNAVAILABLE"}, want: ErrProviderUnavailable},
		{serverError: geminiServerError{Status: "INVALID_ARGUMENT"}, want: ErrProviderResponse},
	}
	for _, test := range tests {
		if got := classifyGeminiServerError(test.serverError); !errors.Is(got, test.want) {
			t.Fatalf("classify %+v = %v, want %v", test.serverError, got, test.want)
		}
	}
}

func TestGeminiInterruptionDoesNotFinalizeNewInputTurn(t *testing.T) {
	t.Parallel()
	stream := &geminiRealtimeStream{}
	if stream.hasInputTurnBoundary(&geminiServerContent{Interrupted: true}) {
		t.Fatal("interruption must not finalize the new input turn")
	}
	if stream.hasInputTurnBoundary(&geminiServerContent{TurnComplete: true}) {
		t.Fatal("turnComplete for interrupted model output must be ignored")
	}
	if !stream.hasInputTurnBoundary(&geminiServerContent{GenerationComplete: true}) {
		t.Fatal("generationComplete must finalize the associated input transcription")
	}
	if stream.hasInputTurnBoundary(&geminiServerContent{TurnComplete: true}) {
		t.Fatal("turnComplete after generationComplete must not finalize another input turn")
	}
	if !stream.hasInputTurnBoundary(&geminiServerContent{TurnComplete: true}) {
		t.Fatal("standalone turnComplete must remain a finalization fallback")
	}
}

func verifyGeminiSetup(conn *websocket.Conn) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	setup, ok := event["setup"].(map[string]any)
	if !ok || setup["model"] != "models/"+defaultGeminiRealtimeModel {
		return ErrProviderRequest
	}
	generation, ok := setup["generationConfig"].(map[string]any)
	if !ok {
		return ErrProviderRequest
	}
	modalities, ok := generation["responseModalities"].([]any)
	if !ok || len(modalities) != 1 || modalities[0] != "AUDIO" {
		return ErrProviderRequest
	}
	if _, exists := setup["inputAudioTranscription"].(map[string]any); !exists {
		return ErrProviderRequest
	}
	realtimeInput, ok := setup["realtimeInputConfig"].(map[string]any)
	if !ok || realtimeInput["activityHandling"] != geminiActivityHandlingInterrupts {
		return ErrProviderRequest
	}
	activity, ok := realtimeInput["automaticActivityDetection"].(map[string]any)
	if !ok || activity["disabled"] != false ||
		activity["startOfSpeechSensitivity"] != GeminiStartSensitivityHigh ||
		activity["endOfSpeechSensitivity"] != GeminiEndSensitivityHigh ||
		activity["prefixPaddingMs"] != float64(defaultGeminiPrefixPadding.Milliseconds()) ||
		activity["silenceDurationMs"] != float64((300*time.Millisecond).Milliseconds()) {
		return ErrProviderRequest
	}
	if _, ok := setup["contextWindowCompression"].(map[string]any); !ok {
		return ErrProviderRequest
	}
	return nil
}

func verifyGeminiAudio(conn *websocket.Conn, expectedBytes int) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	realtimeInput, ok := event["realtimeInput"].(map[string]any)
	if !ok {
		return ErrProviderRequest
	}
	audio, ok := realtimeInput["audio"].(map[string]any)
	if !ok || audio["mimeType"] != geminiAudioMIMEType {
		return ErrProviderRequest
	}
	data, err := base64.StdEncoding.DecodeString(audio["data"].(string))
	if err != nil || len(data) != expectedBytes {
		return ErrProviderRequest
	}
	return nil
}

func verifyGeminiAudioStreamEnd(conn *websocket.Conn) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	realtimeInput, ok := event["realtimeInput"].(map[string]any)
	if !ok || realtimeInput["audioStreamEnd"] != true {
		return ErrProviderRequest
	}
	return nil
}

func geminiInputTranscriptionEvent(text string) map[string]any {
	return map[string]any{
		"serverContent": map[string]any{
			"inputTranscription": map[string]any{"text": text},
		},
	}
}

func assertGeminiRealtimeEvents(t *testing.T, events []Event) {
	t.Helper()
	if len(events) != 5 || events[0].Type != EventSessionReady || events[4].Type != EventCompleted {
		t.Fatalf("events = %+v", events)
	}
	if events[1].Segment == nil || events[1].Segment.Text != "Hello" ||
		events[1].Segment.State != TranscriptStateProvisional {
		t.Fatalf("first transcript = %+v", events[1])
	}
	if events[2].Segment == nil || events[2].Segment.Text != "Hello world." ||
		events[2].Segment.Revision != 2 || events[2].Segment.State != TranscriptStateProvisional {
		t.Fatalf("late transcript = %+v", events[2])
	}
	if events[3].Segment == nil || events[3].Segment.Text != "Hello world." ||
		events[3].Segment.Revision != 3 || events[3].Segment.State != TranscriptStateStable {
		t.Fatalf("final transcript = %+v", events[3])
	}
	if events[3].Segment.FinalizationReason != FinalizationProviderFinal {
		t.Fatalf("finalization reason = %q", events[3].Segment.FinalizationReason)
	}
}
