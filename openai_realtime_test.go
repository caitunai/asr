package asr

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestOpenAIRealtimeSessionProtocol(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != openAIRealtimeEndpointPath ||
			request.URL.Query().Get("intent") != "transcription" ||
			request.Header.Get("Authorization") != "Bearer test-key" {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, upgradeErr := upgrader.Upgrade(response, request, nil)
		if upgradeErr != nil {
			serverErr <- errors.Join(ErrProviderResponse, upgradeErr)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := verifyOpenAIRealtimeSessionUpdate(conn); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{"type": qwenEventSessionUpdated}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyOpenAIRealtimeAudioAppend(conn, 23998); err != nil {
			serverErr <- err
			return
		}
		if err := verifyOpenAIRealtimeAudioAppend(conn, 2); err != nil {
			serverErr <- err
			return
		}
		if err := verifyOpenAIRealtimeAudioAppend(conn, 4800); err != nil {
			serverErr <- err
			return
		}
		if err := verifyOpenAIRealtimeCommit(conn); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"type": openAIEventAudioCommitted, "item_id": "openai-item-1",
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		serverEvents := []map[string]any{
			{
				"type":    qwenEventTranscriptionDelta,
				"item_id": "openai-item-1", "delta": "Hello ",
			},
			{
				"type":    qwenEventTranscriptionDelta,
				"item_id": "openai-item-1", "delta": "world",
			},
			{
				"type":    qwenEventTranscriptionCompleted,
				"item_id": "openai-item-1", "transcript": "Hello world.",
			},
		}
		for _, event := range serverEvents {
			if err := conn.WriteJSON(event); err != nil {
				serverErr <- errors.Join(ErrProviderResponse, err)
				return
			}
		}
		serverErr <- nil
	}))
	defer server.Close()

	provider, err := NewOpenAIRealtimeProvider(OpenAIRealtimeConfig{
		Endpoint:       server.URL,
		APIKey:         "test-key",
		CommitInterval: 500 * time.Millisecond,
		FinishTimeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new OpenAI realtime provider: %v", err)
	}
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID:  "openai-session-1",
			Language:   "en-US",
			SampleRate: 16000,
			Channels:   1,
			Format:     AudioFormatRawPCM16,
		},
		ChunkDuration: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new OpenAI realtime session: %v", err)
	}
	defer session.Close()
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 8000)}); err != nil {
		t.Fatalf("push OpenAI audio: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish OpenAI audio: %v", err)
	}

	events := make([]Event, 0, 5)
	for event := range session.Events() {
		events = append(events, event)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait OpenAI session: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("mock OpenAI server: %v", err)
	}
	assertOpenAIRealtimeEvents(t, events)
}

func TestOpenAIRealtimeProviderDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	provider, err := NewOpenAIRealtimeProvider(OpenAIRealtimeConfig{
		APIKey: "test-key",
	})
	if err != nil {
		t.Fatalf("new OpenAI realtime provider: %v", err)
	}
	if provider.Name() != defaultOpenAIRealtimeName || provider.Model() != defaultOpenAIRealtimeModel ||
		provider.cfg.Endpoint != defaultOpenAIRealtimeEndpoint ||
		provider.cfg.Delay != defaultOpenAIRealtimeDelay ||
		provider.cfg.CommitInterval != defaultOpenAIRealtimeCommitInterval ||
		provider.cfg.TurnDetectionType != OpenAITurnDetectionSemanticVAD ||
		provider.cfg.SemanticVADEagerness != OpenAISemanticVADEagernessAuto ||
		!provider.ServerVADEnabled() {
		t.Fatalf("unexpected OpenAI defaults: %+v", provider)
	}
	capabilities := provider.StreamingCapabilities()
	if len(capabilities.SampleRates) != 3 || capabilities.SampleRates[0] != 8000 ||
		capabilities.SampleRates[1] != 16000 || capabilities.SampleRates[2] != 24000 ||
		!capabilities.SupportsLanguageHints || !capabilities.SupportsServerVAD || capabilities.SupportsPrompt {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	_, err = NewOpenAIRealtimeProvider(OpenAIRealtimeConfig{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing API key error = %v", err)
	}
}

func TestOpenAIRealtimeTurnDetectionConfig(t *testing.T) {
	t.Parallel()
	threshold := 0.35
	tests := []struct {
		name       string
		config     OpenAIRealtimeConfig
		wantType   string
		wantNil    bool
		wantDetail func(*testing.T, map[string]any)
	}{
		{
			name:     "semantic default",
			config:   OpenAIRealtimeConfig{APIKey: "test-key"},
			wantType: OpenAITurnDetectionSemanticVAD,
			wantDetail: func(t *testing.T, config map[string]any) {
				t.Helper()
				if config["eagerness"] != OpenAISemanticVADEagernessAuto {
					t.Fatalf("semantic config = %+v", config)
				}
			},
		},
		{
			name: "server vad",
			config: OpenAIRealtimeConfig{
				APIKey:                 "test-key",
				TurnDetectionType:      OpenAITurnDetectionServerVAD,
				ServerVADThreshold:     &threshold,
				ServerVADPrefixPadding: 250 * time.Millisecond,
				ServerVADSilence:       900 * time.Millisecond,
			},
			wantType: OpenAITurnDetectionServerVAD,
			wantDetail: func(t *testing.T, config map[string]any) {
				t.Helper()
				if config["threshold"] != 0.35 || config["prefix_padding_ms"] != int64(250) ||
					config["silence_duration_ms"] != int64(900) {
					t.Fatalf("server VAD config = %+v", config)
				}
			},
		},
		{
			name:    "disabled",
			config:  OpenAIRealtimeConfig{APIKey: "test-key", DisableTurnDetection: true},
			wantNil: true,
		},
		{
			name:    "whisper requires manual commit",
			config:  OpenAIRealtimeConfig{APIKey: "test-key", Model: openAIRealtimeWhisperModel},
			wantNil: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			provider, err := NewOpenAIRealtimeProvider(test.config)
			if err != nil {
				t.Fatalf("new provider: %v", err)
			}
			stream := &openAIRealtimeStream{provider: provider}
			turnDetection := stream.turnDetectionConfig()
			if test.wantNil {
				if turnDetection != nil || provider.ServerVADEnabled() {
					t.Fatalf("turn detection = %+v", turnDetection)
				}
				return
			}
			config, ok := turnDetection.(map[string]any)
			if !ok || config[qwenFieldType] != test.wantType || !provider.ServerVADEnabled() {
				t.Fatalf("turn detection = %+v", turnDetection)
			}
			test.wantDetail(t, config)
		})
	}
}

func TestOpenAIRealtimeRejectsInvalidTurnDetectionConfig(t *testing.T) {
	t.Parallel()
	invalidThreshold := 1.1
	tests := []OpenAIRealtimeConfig{
		{APIKey: "test-key", TurnDetectionType: "client_vad"},
		{APIKey: "test-key", SemanticVADEagerness: "urgent"},
		{APIKey: "test-key", ServerVADThreshold: &invalidThreshold},
		{APIKey: "test-key", ServerVADPrefixPadding: -time.Millisecond},
		{APIKey: "test-key", ServerVADSilence: 500 * time.Microsecond},
	}
	for index, config := range tests {
		if _, err := NewOpenAIRealtimeProvider(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
}

func TestOpenAIRealtimeCommitAccounting(t *testing.T) {
	t.Parallel()
	stream := &openAIRealtimeStream{
		manualCommits:  1,
		pendingResults: make(map[string]struct{}),
	}
	stream.commitStarted("auto-or-manual-item")
	if stream.manualCommits != 0 || len(stream.pendingResults) != 1 {
		t.Fatalf("started state: manual=%d pending=%v", stream.manualCommits, stream.pendingResults)
	}
	stream.commitFinished("auto-or-manual-item")
	if len(stream.pendingResults) != 0 {
		t.Fatalf("finished state: pending=%v", stream.pendingResults)
	}

	stream.closing = true
	stream.manualCommits = 1
	stream.pendingResults["active"] = struct{}{}
	if !stream.settleEmptyCloseCommit(openAIRealtimeServerError{
		Code:    "input_audio_buffer_commit_empty",
		Message: "Cannot commit because the buffer has no audio.",
	}) || stream.manualCommits != 0 {
		t.Fatalf("empty commit state: manual=%d", stream.manualCommits)
	}
}

func TestOpenAIRealtimeTranscriptionConfigUsesModelSpecificDelay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		model     string
		wantDelay bool
	}{
		{name: "default transcribe model", model: defaultOpenAIRealtimeModel},
		{name: "realtime whisper", model: openAIRealtimeWhisperModel, wantDelay: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stream := &openAIRealtimeStream{
				provider: &OpenAIRealtimeProvider{cfg: OpenAIRealtimeConfig{
					Model: test.model,
					Delay: OpenAIRealtimeDelayLow,
				}},
				request: StreamingRequest{Language: "en-US"},
			}
			transcription := stream.transcriptionConfig()
			if transcription[defaultHTTPModelField] != test.model || transcription["language"] != "en" {
				t.Fatalf("transcription config = %+v", transcription)
			}
			delay, hasDelay := transcription["delay"]
			if hasDelay != test.wantDelay || (hasDelay && delay != OpenAIRealtimeDelayLow) {
				t.Fatalf("delay = %v, present = %t", delay, hasDelay)
			}
		})
	}
}

func verifyOpenAIRealtimeSessionUpdate(conn *websocket.Conn) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	session, ok := event[qwenFieldSession].(map[string]any)
	if event[qwenFieldType] != qwenEventSessionUpdate || !ok ||
		session[qwenFieldType] != openAIRealtimeSessionType {
		return ErrProviderRequest
	}
	audioSection, ok := session[qwenFieldAudio].(map[string]any)
	if !ok {
		return ErrProviderRequest
	}
	input, ok := audioSection["input"].(map[string]any)
	if !ok {
		return ErrProviderRequest
	}
	turnDetection, ok := input["turn_detection"].(map[string]any)
	if !ok || turnDetection[qwenFieldType] != OpenAITurnDetectionSemanticVAD ||
		turnDetection["eagerness"] != OpenAISemanticVADEagernessAuto {
		return ErrProviderRequest
	}
	format, ok := input["format"].(map[string]any)
	if !ok || format[qwenFieldType] != "audio/pcm" || format["rate"] != float64(24000) {
		return ErrProviderRequest
	}
	transcription, ok := input["transcription"].(map[string]any)
	_, hasDelay := transcription["delay"]
	if !ok || transcription[defaultHTTPModelField] != defaultOpenAIRealtimeModel ||
		hasDelay || transcription["language"] != "en" {
		return ErrProviderRequest
	}
	return nil
}

func verifyOpenAIRealtimeAudioAppend(conn *websocket.Conn, expectedBytes int) error {
	var event struct {
		Type  string `json:"type"`
		Audio string `json:"audio"`
	}
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	audioData, err := base64.StdEncoding.DecodeString(event.Audio)
	if err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	if event.Type != qwenEventAudioAppend || len(audioData) != expectedBytes {
		return ErrProviderRequest
	}
	return nil
}

func verifyOpenAIRealtimeCommit(conn *websocket.Conn) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	eventID, ok := event[qwenFieldEventID].(string)
	if event[qwenFieldType] != qwenEventAudioCommit || !ok || !strings.Contains(eventID, "_commit_") {
		return ErrProviderRequest
	}
	return nil
}

func assertOpenAIRealtimeEvents(t *testing.T, events []Event) {
	t.Helper()
	if len(events) != 5 || events[0].Type != EventSessionReady || events[4].Type != EventCompleted {
		t.Fatalf("events = %+v", events)
	}
	if events[1].Segment == nil || events[1].Segment.Text != "Hello" ||
		events[1].Segment.State != TranscriptStateProvisional {
		t.Fatalf("first partial = %+v", events[1])
	}
	if events[2].Segment == nil || events[2].Segment.Text != "Hello world" ||
		events[2].Segment.Revision != 2 {
		t.Fatalf("second partial = %+v", events[2])
	}
	if events[3].Segment == nil || events[3].Segment.Text != "Hello world." ||
		events[3].Segment.State != TranscriptStateStable || events[3].Segment.Revision != 3 {
		t.Fatalf("final = %+v", events[3])
	}
}
