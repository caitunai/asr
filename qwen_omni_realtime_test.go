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

func TestQwenOmniRealtimeSessionASRProtocol(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-key" ||
			request.Header.Get("OpenAI-Beta") != "" ||
			request.URL.Query().Get("model") != defaultQwenOmniRealtimeModel {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, upgradeErr := upgrader.Upgrade(response, request, nil)
		if upgradeErr != nil {
			serverErr <- errors.Join(ErrProviderResponse, upgradeErr)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := verifyQwenOmniSessionUpdate(conn); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{"type": qwenEventSessionUpdated}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyQwenOmniAudioAppend(conn, 320); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"type": qwenEventSpeechStarted, "item_id": "omni-item-1", "audio_start_ms": 0,
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"type":    "conversation.item.input_audio_transcription.delta",
			"item_id": "omni-item-1", "text": "实时", "stash": "识别",
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		paddingBytes := int((defaultQwenOmniVADSilence + qwenOmniFinishPadding).Seconds() * 16000 * 2)
		if err := verifyQwenOmniAudioAppend(conn, paddingBytes); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"type": qwenEventSpeechStopped, "item_id": "omni-item-1", "audio_end_ms": 10,
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := conn.WriteJSON(map[string]any{"type": "response.created"}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		_, err := verifyQwenOmniResponseCancel(conn)
		if err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"type": qwenEventError,
			"error": map[string]any{
				"type":    "invalid_request_error",
				"code":    "invalid_request_error",
				"message": "response already completed",
			},
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"type":    qwenEventTranscriptionCompleted,
			"item_id": "omni-item-1", "transcript": "实时识别。",
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		serverErr <- nil
	}))
	defer server.Close()

	provider, err := NewQwenOmniRealtimeProvider(QwenOmniRealtimeConfig{
		Endpoint: "ws" + strings.TrimPrefix(server.URL, "http"),
		APIKey:   "test-key",
	})
	if err != nil {
		t.Fatalf("new qwen omni provider: %v", err)
	}
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID:  "omni-session-1",
			Language:   "auto",
			SampleRate: 16000,
			Channels:   1,
			Format:     AudioFormatRawPCM16,
			ServerVAD:  provider.ServerVADEnabled(),
		},
	})
	if err != nil {
		t.Fatalf("new realtime session: %v", err)
	}
	defer session.Close()
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 160)}); err != nil {
		t.Fatalf("push omni audio: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish omni audio: %v", err)
	}

	events := make([]Event, 0, 4)
	for event := range session.Events() {
		events = append(events, event)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait omni session: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("mock omni server: %v", err)
	}
	assertQwenOmniEvents(t, events)
}

func TestQwenOmniRealtimeProviderDefaults(t *testing.T) {
	t.Parallel()
	provider, err := NewQwenOmniRealtimeProvider(QwenOmniRealtimeConfig{
		Endpoint: "wss://workspace.example.com/api-ws/v1/realtime",
		APIKey:   "test-key",
	})
	if err != nil {
		t.Fatalf("new qwen omni provider: %v", err)
	}
	if provider.Model() != defaultQwenOmniRealtimeModel || !provider.ServerVADEnabled() ||
		provider.core.omni.turnDetectionType != QwenOmniTurnDetectionSemantic ||
		provider.core.cfg.ServerVADThreshold != defaultQwenOmniVADThreshold ||
		provider.core.cfg.ServerVADSilenceDuration != defaultQwenOmniVADSilence {
		t.Fatalf("unexpected qwen omni defaults: %+v", provider.core)
	}
	capabilities := provider.StreamingCapabilities()
	if len(capabilities.SampleRates) != 1 || capabilities.SampleRates[0] != 16000 ||
		capabilities.SupportsPrompt || capabilities.SupportsTerms {
		t.Fatalf("capabilities = %+v", capabilities)
	}
}

func verifyQwenOmniSessionUpdate(conn *websocket.Conn) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	session, ok := event["session"].(map[string]any)
	if event["type"] != qwenEventSessionUpdate || !ok ||
		session["input_audio_format"] != qwenAudioFormatPCM {
		return ErrProviderRequest
	}
	modalities, ok := session["modalities"].([]any)
	if !ok || len(modalities) != 1 || modalities[0] != "text" {
		return ErrProviderRequest
	}
	transcription, ok := session["input_audio_transcription"].(map[string]any)
	if !ok || len(transcription) != 0 {
		return ErrProviderRequest
	}
	turnDetection, ok := session["turn_detection"].(map[string]any)
	if !ok || turnDetection["type"] != QwenOmniTurnDetectionSemantic ||
		turnDetection["threshold"] != defaultQwenOmniVADThreshold ||
		turnDetection["silence_duration_ms"] != float64(defaultQwenOmniVADSilence.Milliseconds()) {
		return ErrProviderRequest
	}
	if _, exists := session[realtimeFieldSampleRate]; exists {
		return ErrProviderRequest
	}
	return nil
}

func verifyQwenOmniAudioAppend(conn *websocket.Conn, expectedBytes int) error {
	var event struct {
		Type  string `json:"type"`
		Audio string `json:"audio"`
	}
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	audio, err := base64.StdEncoding.DecodeString(event.Audio)
	if err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	if event.Type != qwenEventAudioAppend || len(audio) != expectedBytes {
		return ErrProviderRequest
	}
	return nil
}

func verifyQwenOmniResponseCancel(conn *websocket.Conn) (string, error) {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return "", errors.Join(ErrProviderResponse, err)
	}
	if event["type"] != "response.cancel" {
		return "", ErrProviderRequest
	}
	eventID, ok := event["event_id"].(string)
	if !ok || eventID == "" {
		return "", ErrProviderRequest
	}
	return eventID, nil
}

func assertQwenOmniEvents(t *testing.T, events []Event) {
	t.Helper()
	if len(events) != 4 || events[0].Type != EventSessionReady || events[3].Type != EventCompleted {
		t.Fatalf("events = %+v", events)
	}
	if events[1].Segment == nil || events[1].Segment.Text != "实时识别" ||
		events[1].Segment.State != TranscriptStateProvisional {
		t.Fatalf("partial event = %+v", events[1])
	}
	if events[2].Segment == nil || events[2].Segment.Text != "实时识别。" ||
		events[2].Segment.State != TranscriptStateStable || events[2].Segment.Revision != 2 {
		t.Fatalf("final event = %+v", events[2])
	}
}
