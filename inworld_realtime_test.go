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

func TestInworldRealtimeSessionProtocol(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != inworldEndpointPath ||
			request.Header.Get(inworldAuthorizationHeader) != "Basic test-key" {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := verifyInworldConfig(conn); err != nil {
			serverErr <- err
			return
		}
		if err := verifyInworldAudioChunk(conn, 3200); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(inworldServerEvent{Result: &inworldServerResult{
			Transcription: &inworldTranscription{Transcript: "Hello wor"},
		}}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyInworldEndTurn(conn); err != nil {
			serverErr <- err
			return
		}
		nextClientEvent := make(chan map[string]any, 1)
		nextClientErr := make(chan error, 1)
		go func() {
			var event map[string]any
			if err := conn.ReadJSON(&event); err != nil {
				nextClientErr <- errors.Join(ErrProviderRequest, err)
				return
			}
			nextClientEvent <- event
		}()
		select {
		case <-nextClientEvent:
			serverErr <- ErrProviderRequest
			return
		case err := <-nextClientErr:
			serverErr <- err
			return
		case <-time.After(20 * time.Millisecond):
		}
		if err := conn.WriteJSON(inworldServerEvent{Result: &inworldServerResult{
			Transcription: &inworldTranscription{
				Transcript: "Hello world.",
				IsFinal:    true,
				WordTimestamps: []inworldWordTimestamp{
					{Word: "Hello", Confidence: 0.98, StartTimeMS: 100, EndTimeMS: 400},
					{Word: "world", Confidence: 0.96, StartTimeMS: 500, EndTimeMS: 900},
				},
			},
		}}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		select {
		case event := <-nextClientEvent:
			if err := verifyInworldCloseStreamEvent(event); err != nil {
				serverErr <- err
				return
			}
		case err := <-nextClientErr:
			serverErr <- err
			return
		case <-time.After(time.Second):
			serverErr <- ErrRequestTimeout
			return
		}
		serverErr <- nil
	}))
	defer server.Close()

	provider, err := NewInworldRealtimeProvider(InworldRealtimeConfig{
		Endpoint:              server.URL,
		APIKey:                "test-key",
		IncludeWordTimestamps: true,
		FinishTimeout:         2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new Inworld realtime provider: %v", err)
	}
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID:     "inworld-session-client",
			Language:      "en-US",
			LanguageHints: []string{"fr-FR"},
			SampleRate:    16000,
			Channels:      1,
			Format:        AudioFormatRawPCM16,
			Context: RecognitionContext{
				Prompt: "Weather forecast",
				Terms:  []string{"Inworld", "weather station", "Inworld"},
			},
		},
		ChunkDuration: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new Inworld realtime session: %v", err)
	}
	defer session.Close()
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 1600)}); err != nil {
		t.Fatalf("push Inworld audio: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish Inworld audio: %v", err)
	}

	events := make([]Event, 0, 4)
	for event := range session.Events() {
		events = append(events, event)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait Inworld session: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("mock Inworld server: %v", err)
	}
	assertInworldRealtimeEvents(t, events)
}

func TestInworldRealtimeProviderDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	provider, err := NewInworldRealtimeProvider(InworldRealtimeConfig{APIKey: "test-key"})
	if err != nil {
		t.Fatalf("new Inworld provider: %v", err)
	}
	if provider.Name() != defaultInworldRealtimeName ||
		provider.Model() != defaultInworldRealtimeModel ||
		provider.cfg.Endpoint != defaultInworldRealtimeEndpoint ||
		*provider.cfg.VADThreshold != defaultInworldVADThreshold ||
		provider.cfg.DisablePartials || !provider.ServerVADEnabled() {
		t.Fatalf("unexpected Inworld defaults: %+v", provider)
	}
	capabilities := provider.StreamingCapabilities()
	if len(capabilities.SampleRates) != 6 || capabilities.SupportsPrompt ||
		!capabilities.SupportsTerms || capabilities.SupportsLanguageHints ||
		!capabilities.SupportsServerVAD {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	belowMinimum := -0.1
	aboveMaximum := 1.1
	invalidConfigs := []InworldRealtimeConfig{
		{},
		{APIKey: "test-key\nsecond-header"},
		{APIKey: "test-key", Endpoint: "file:///tmp/inworld.sock"},
		{APIKey: "test-key", VADThreshold: &belowMinimum},
		{APIKey: "test-key", EndOfTurnConfidenceThreshold: &aboveMaximum},
		{APIKey: "test-key", InactivityTimeout: 1500 * time.Millisecond},
		{APIKey: "test-key", Model: "soniox/stt-rt-v5", DisableServerVAD: true},
	}
	for index, config := range invalidConfigs {
		if _, err := NewInworldRealtimeProvider(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
}

func TestInworldRealtimeRejectsUnsupportedTermCharacters(t *testing.T) {
	t.Parallel()
	provider, err := NewInworldRealtimeProvider(InworldRealtimeConfig{APIKey: "test-key"})
	if err != nil {
		t.Fatalf("new Inworld provider: %v", err)
	}
	_, err = provider.Start(context.Background(), StreamingRequest{
		SessionID:  "invalid-prompt",
		Language:   "auto",
		SampleRate: 16000,
		Channels:   1,
		Format:     AudioFormatRawPCM16,
		Context:    RecognitionContext{Terms: []string{"C#"}},
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid term error = %v", err)
	}
}

func TestInworldRealtimeAutoLanguageDoesNotPromoteLanguageHint(t *testing.T) {
	t.Parallel()
	request := StreamingRequest{
		Language:      automaticLanguage,
		LanguageHints: []string{"zh-CN", "en-US"},
	}
	if language := inworldRequestLanguage(request); language != "" {
		t.Fatalf("auto language = %q, want omitted", language)
	}
	request.Language = "en-US"
	if language := inworldRequestLanguage(request); language != "en" {
		t.Fatalf("explicit language = %q, want en", language)
	}
}

func TestInworldRealtimePromptsUseTermsOnly(t *testing.T) {
	t.Parallel()
	prompts, err := normalizeInworldPrompts(RecognitionContext{
		Prompt: "请将所有内容输出为中文",
		Terms:  []string{"Inworld", "weather station", "Inworld"},
	})
	if err != nil {
		t.Fatalf("normalize prompts: %v", err)
	}
	if !slices.Equal(prompts, []string{"Inworld", "weather station"}) {
		t.Fatalf("prompts = %v", prompts)
	}
}

func TestInworldRealtimeClassifiesHandshakeAuthorizationError(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	provider, err := NewInworldRealtimeProvider(InworldRealtimeConfig{
		Endpoint: server.URL,
		APIKey:   "bad-key",
	})
	if err != nil {
		t.Fatalf("new Inworld provider: %v", err)
	}
	_, err = provider.Start(context.Background(), StreamingRequest{
		SessionID:  "unauthorized",
		Language:   "auto",
		SampleRate: 16000,
		Channels:   1,
		Format:     AudioFormatRawPCM16,
	})
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("authorization error = %v", err)
	}
}

func TestInworldRealtimeServerErrorClassification(t *testing.T) {
	t.Parallel()
	tests := []struct {
		errorCode int
		status    string
		want      error
	}{
		{errorCode: inworldGRPCUnauthenticated, want: ErrUnauthorized},
		{errorCode: inworldGRPCResourceExhausted, want: ErrRateLimited},
		{errorCode: inworldGRPCDeadlineExceeded, want: ErrRequestTimeout},
		{errorCode: inworldGRPCUnavailable, want: ErrProviderUnavailable},
		{status: "INVALID_ARGUMENT", want: ErrProviderRequest},
	}
	for _, test := range tests {
		classified := classifyInworldServerError(inworldServerError{
			Code: test.errorCode, Status: test.status,
		})
		if !errors.Is(classified, test.want) {
			t.Fatalf("classification for code=%d status=%q = %v", test.errorCode, test.status, classified)
		}
	}
}

func TestInworldRealtimeFinishTimeoutDoesNotClaimProviderUnavailable(t *testing.T) {
	t.Parallel()
	serverDone := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			close(serverDone)
			return
		}
		defer func() {
			_ = conn.Close()
			close(serverDone)
		}()
		var config inworldClientEvent
		if err := conn.ReadJSON(&config); err != nil {
			return
		}
		var audio inworldClientEvent
		if err := conn.ReadJSON(&audio); err != nil {
			return
		}
		var endTurn inworldClientEvent
		if err := conn.ReadJSON(&endTurn); err != nil {
			return
		}
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()

	provider, err := NewInworldRealtimeProvider(InworldRealtimeConfig{
		Endpoint:      server.URL,
		APIKey:        "test-key",
		FinishTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new Inworld provider: %v", err)
	}
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID: "inworld-timeout", Language: "auto", SampleRate: 16000,
			Channels: 1, Format: AudioFormatRawPCM16,
		},
		ChunkDuration: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new Inworld session: %v", err)
	}
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 1600)}); err != nil {
		t.Fatalf("push audio: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish input: %v", err)
	}
	for range session.Events() {
	}
	waitErr := session.Wait(context.Background())
	if !errors.Is(waitErr, ErrRequestTimeout) || errors.Is(waitErr, ErrProviderUnavailable) {
		t.Fatalf("wait error = %v", waitErr)
	}
	<-serverDone
}

func verifyInworldConfig(conn *websocket.Conn) error {
	var event inworldClientEvent
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	config := event.TranscribeConfig
	if config == nil || event.AudioChunk != nil || event.EndTurn != nil || event.CloseStream != nil ||
		config.ModelID != defaultInworldRealtimeModel ||
		config.AudioEncoding != inworldAudioEncodingLinear16 || config.Language != "en" ||
		config.SampleRateHertz != 16000 || config.NumberOfChannels != 1 ||
		!config.IncludeWordTimestamps || config.InworldSTTV1Config == nil ||
		config.InworldSTTV1Config.VADThreshold != defaultInworldVADThreshold ||
		!slices.Equal(config.Prompts, []string{"Inworld", "weather station"}) {
		return ErrProviderRequest
	}
	return nil
}

func verifyInworldAudioChunk(conn *websocket.Conn, wantBytes int) error {
	var event inworldClientEvent
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	if event.AudioChunk == nil || event.TranscribeConfig != nil ||
		event.EndTurn != nil || event.CloseStream != nil {
		return ErrProviderRequest
	}
	audio, err := base64.StdEncoding.DecodeString(event.AudioChunk.Content)
	if err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	if len(audio) != wantBytes {
		return ErrProviderRequest
	}
	return nil
}

func verifyInworldEndTurn(conn *websocket.Conn) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	value, exists := event["endTurn"]
	if !exists {
		return ErrProviderRequest
	}
	if _, ok := value.(map[string]any); !ok || len(event) != 1 {
		return ErrProviderRequest
	}
	return nil
}

func verifyInworldCloseStreamEvent(event map[string]any) error {
	value, exists := event["closeStream"]
	if !exists {
		return ErrProviderRequest
	}
	if _, ok := value.(map[string]any); !ok || len(event) != 1 {
		return ErrProviderRequest
	}
	return nil
}

func assertInworldRealtimeEvents(t *testing.T, events []Event) {
	t.Helper()
	if len(events) != 4 || events[0].Type != EventSessionReady ||
		events[1].Segment == nil || events[2].Segment == nil ||
		events[1].Segment.State != TranscriptStatePreview ||
		events[1].Segment.Text != "Hello wor" ||
		events[2].Segment.State != TranscriptStateStable ||
		events[2].Segment.Text != "Hello world." ||
		events[2].Segment.SegmentIndex != events[1].Segment.SegmentIndex ||
		events[2].Segment.Revision != events[1].Segment.Revision+1 ||
		events[3].Type != EventCompleted {
		t.Fatalf("unexpected Inworld events: %+v", events)
	}
}
