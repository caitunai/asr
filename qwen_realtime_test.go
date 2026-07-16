package asr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestQwenRealtimeSessionProtocolAndEventMapping(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-key" ||
			request.Header.Get("OpenAI-Beta") != qwenRealtimeBetaHeader ||
			request.Header.Get("X-DashScope-WorkSpace") != "workspace-1" ||
			request.URL.Query().Get("model") != defaultQwenRealtimeModel {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		if err := verifyQwenSessionUpdate(conn); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{"type": qwenEventSessionUpdated}); err != nil {
			serverErr <- err
			return
		}
		if err := verifyQwenAudioAppend(conn); err != nil {
			serverErr <- err
			return
		}
		if err := verifyQwenFinish(conn); err != nil {
			serverErr <- err
			return
		}
		responses := []map[string]any{
			{"type": qwenEventSpeechStarted, "item_id": "item-1", "audio_start_ms": 10},
			{"type": "conversation.item.input_audio_transcription.text", "item_id": "item-1", "text": "今天天气", "stash": "怎么样？", "language": "zh"},
			{"type": qwenEventSpeechStopped, "item_id": "item-1", "audio_end_ms": 20},
			{"type": qwenEventTranscriptionCompleted, "item_id": "item-1", "transcript": "今天天气怎么样？", "language": "zh"},
			{"type": "session.finished"},
		}
		for _, event := range responses {
			if err := conn.WriteJSON(event); err != nil {
				serverErr <- err
				return
			}
		}
		serverErr <- nil
	}))
	defer server.Close()

	provider, err := NewQwenRealtimeProvider(QwenRealtimeConfig{
		Endpoint:                 "ws" + strings.TrimPrefix(server.URL, "http"),
		APIKey:                   "test-key",
		WorkspaceID:              "workspace-1",
		ServerVADSilenceDuration: 400 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new qwen provider: %v", err)
	}
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID: "session-1",
			Language:  "zh",
			Context: RecognitionContext{
				Prompt: "天气问答",
				Terms:  []string{"气象局", "气象局"},
			},
			SampleRate: 16000,
			Channels:   1,
			Format:     AudioFormatRawPCM16,
			ServerVAD:  true,
		},
	})
	if err != nil {
		t.Fatalf("new realtime session: %v", err)
	}
	defer session.Close()
	for range 4 {
		if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 320)}); err != nil {
			t.Fatalf("push audio: %v", err)
		}
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish audio: %v", err)
	}

	events := make([]Event, 0, 4)
	for event := range session.Events() {
		events = append(events, event)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait realtime session: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("mock qwen server: %v", err)
	}
	assertRealtimeEvents(t, events)
}

func TestQwenRealtimeProviderClassifiesUnauthorizedHandshake(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	provider, err := NewQwenRealtimeProvider(QwenRealtimeConfig{
		Endpoint: "ws" + strings.TrimPrefix(server.URL, "http"),
		APIKey:   "invalid-key",
	})
	if err != nil {
		t.Fatalf("new qwen provider: %v", err)
	}
	_, err = provider.Start(context.Background(), StreamingRequest{
		SessionID:  "session-unauthorized",
		Language:   "auto",
		SampleRate: 16000,
		Channels:   1,
		Format:     AudioFormatRawPCM16,
		ServerVAD:  true,
	})
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("error = %v, want unauthorized", err)
	}
}

func TestQwenRealtimeProviderAppliesServerVADDefaults(t *testing.T) {
	t.Parallel()
	provider, err := NewQwenRealtimeProvider(QwenRealtimeConfig{
		Endpoint: "wss://workspace.example.com/api-ws/v1/realtime",
		APIKey:   "test-key",
	})
	if err != nil {
		t.Fatalf("new qwen provider: %v", err)
	}
	if !provider.ServerVADEnabled() {
		t.Fatal("server VAD must be enabled by default")
	}
	if provider.cfg.ServerVADThreshold != defaultQwenRealtimeVADThreshold ||
		provider.cfg.ServerVADSilenceDuration != defaultQwenRealtimeSilence {
		t.Fatalf("server VAD defaults = threshold %v, silence %v",
			provider.cfg.ServerVADThreshold,
			provider.cfg.ServerVADSilenceDuration,
		)
	}

	disabled, err := NewQwenRealtimeProvider(QwenRealtimeConfig{
		Endpoint:         "wss://workspace.example.com/api-ws/v1/realtime",
		APIKey:           "test-key",
		DisableServerVAD: true,
	})
	if err != nil {
		t.Fatalf("new qwen provider with disabled VAD: %v", err)
	}
	if disabled.ServerVADEnabled() {
		t.Fatal("server VAD must honor explicit disable")
	}
}

func verifyQwenSessionUpdate(conn *websocket.Conn) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	if event["type"] != qwenEventSessionUpdate {
		return ErrProviderRequest
	}
	session, ok := event["session"].(map[string]any)
	if !ok || session["input_audio_format"] != qwenAudioFormatPCM ||
		session[realtimeFieldSampleRate] != float64(16000) {
		return ErrProviderRequest
	}
	transcription, ok := session["input_audio_transcription"].(map[string]any)
	if !ok || transcription["language"] != "zh" {
		return ErrProviderRequest
	}
	corpus, ok := transcription["corpus"].(map[string]any)
	if !ok || corpus["text"] != "天气问答\n气象局" {
		return ErrProviderRequest
	}
	turnDetection, ok := session["turn_detection"].(map[string]any)
	if !ok || turnDetection["type"] != "server_vad" || turnDetection["silence_duration_ms"] != float64(400) {
		return ErrProviderRequest
	}
	return nil
}

func verifyQwenAudioAppend(conn *websocket.Conn) error {
	_, payload, err := conn.ReadMessage()
	if err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	var event struct {
		Type  string `json:"type"`
		Audio string `json:"audio"`
	}
	if unmarshalErr := json.Unmarshal(payload, &event); unmarshalErr != nil {
		return errors.Join(ErrProviderResponse, unmarshalErr)
	}
	decoded, err := base64.StdEncoding.DecodeString(event.Audio)
	if err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	if event.Type != qwenEventAudioAppend || len(decoded) != 2560 {
		return ErrProviderRequest
	}
	return nil
}

func verifyQwenFinish(conn *websocket.Conn) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	if event["type"] != "session.finish" {
		return ErrProviderRequest
	}
	return nil
}

func assertRealtimeEvents(t *testing.T, events []Event) {
	t.Helper()
	if len(events) != 4 {
		t.Fatalf("events = %+v", events)
	}
	if events[0].Type != EventSessionReady || events[1].Segment == nil ||
		events[1].Segment.State != TranscriptStateProvisional ||
		events[1].Segment.Text != "今天天气怎么样？" {
		t.Fatalf("ready/partial events = %+v", events[:2])
	}
	if events[2].Segment == nil || events[2].Segment.State != TranscriptStateStable ||
		events[2].Segment.Revision != 2 ||
		events[2].Segment.FinalizationReason != FinalizationProviderFinal ||
		events[2].Segment.EvidenceQuality != EvidenceProviderFinal {
		t.Fatalf("final event = %+v", events[2])
	}
	if events[3].Type != EventCompleted {
		t.Fatalf("completed event = %+v", events[3])
	}
}
