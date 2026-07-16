package asr

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestVLLMRealtimeSessionProtocol(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != vllmRealtimeEndpointPath ||
			request.Header.Get("Authorization") != "Bearer test-key" {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := conn.WriteJSON(vllmRealtimeServerEvent{
			Type:    vllmEventSessionCreated,
			ID:      "sess-test",
			Created: time.Now().Unix(),
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyVLLMInitialization(conn); err != nil {
			serverErr <- err
			return
		}
		if err := verifyVLLMAudioChunk(conn, 3200); err != nil {
			serverErr <- err
			return
		}
		for _, delta := range []string{"Hello", " world"} {
			if err := conn.WriteJSON(vllmRealtimeServerEvent{
				Type:  vllmEventTranscriptionDelta,
				Delta: delta,
			}); err != nil {
				serverErr <- errors.Join(ErrProviderResponse, err)
				return
			}
		}
		if err := verifyVLLMFinalCommit(conn); err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(vllmRealtimeServerEvent{
			Type: vllmEventTranscriptionDone,
			Text: "Hello world",
		}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		serverErr <- nil
	}))
	defer server.Close()

	provider, err := NewVLLMRealtimeProvider(VLLMRealtimeConfig{
		Endpoint: vllmTestWebSocketURL(server.URL),
		Model:    "test/realtime-model",
		APIKey:   "test-key",
	})
	if err != nil {
		t.Fatalf("new vLLM provider: %v", err)
	}
	stream, err := provider.Start(context.Background(), vllmTestStreamingRequest())
	if err != nil {
		t.Fatalf("start vLLM stream: %v", err)
	}
	defer stream.Close()
	audio := make([]byte, 3200)
	if err := stream.WriteAudio(context.Background(), StreamingAudioChunk{
		Data:        audio,
		Sequence:    0,
		StartSample: 0,
		EndSample:   1600,
	}); err != nil {
		t.Fatalf("write vLLM audio: %v", err)
	}
	if err := stream.CloseInput(context.Background()); err != nil {
		t.Fatalf("close vLLM input: %v", err)
	}
	events := collectProviderEvents(stream.Events())
	if err := stream.Wait(context.Background()); err != nil {
		t.Fatalf("wait vLLM stream: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("event count = %d, events = %+v", len(events), events)
	}
	wantTexts := []string{"Hello", "Hello world", "Hello world"}
	for index, event := range events {
		if event.ResultID != "vllm-test-session" || event.Text != wantTexts[index] {
			t.Fatalf("event %d = %+v", index, event)
		}
	}
	if !events[0].Started || events[0].IsFinal || events[1].IsFinal || !events[2].IsFinal {
		t.Fatalf("unexpected event states: %+v", events)
	}
	if !slices.Equal([]string{events[0].ConfirmedText, events[1].ConfirmedText}, wantTexts[:2]) {
		t.Fatalf("provisional text = %q, %q", events[0].ConfirmedText, events[1].ConfirmedText)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("vLLM test server: %v", err)
	}
}

func TestVLLMRealtimeServerError(t *testing.T) {
	t.Parallel()
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.WriteJSON(vllmRealtimeServerEvent{Type: vllmEventSessionCreated, ID: "bad-model"})
		_ = verifyVLLMInitialization(conn)
		_ = conn.WriteJSON(map[string]any{
			"type":  vllmEventError,
			"error": "The model does not exist.",
			"code":  "model_not_found",
		})
		<-request.Context().Done()
	}))
	defer server.Close()
	provider, err := NewVLLMRealtimeProvider(VLLMRealtimeConfig{
		Endpoint: vllmTestWebSocketURL(server.URL),
	})
	if err != nil {
		t.Fatalf("new vLLM provider: %v", err)
	}
	stream, err := provider.Start(context.Background(), vllmTestStreamingRequest())
	if err != nil {
		t.Fatalf("start vLLM stream: %v", err)
	}
	defer stream.Close()
	events := collectProviderEvents(stream.Events())
	if err := stream.Wait(context.Background()); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("wait error = %v", err)
	}
	if len(events) != 1 || !events[0].IsFinal || !errors.Is(events[0].Err, ErrInvalidRequest) {
		t.Fatalf("events = %+v", events)
	}
}

func TestVLLMRealtimeFinishTimeout(t *testing.T) {
	t.Parallel()
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.WriteJSON(vllmRealtimeServerEvent{Type: vllmEventSessionCreated, ID: "timeout"})
		_ = verifyVLLMInitialization(conn)
		_ = verifyVLLMFinalCommit(conn)
		<-request.Context().Done()
	}))
	defer server.Close()
	provider, err := NewVLLMRealtimeProvider(VLLMRealtimeConfig{
		Endpoint:      vllmTestWebSocketURL(server.URL),
		FinishTimeout: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new vLLM provider: %v", err)
	}
	stream, err := provider.Start(context.Background(), vllmTestStreamingRequest())
	if err != nil {
		t.Fatalf("start vLLM stream: %v", err)
	}
	defer stream.Close()
	if err := stream.CloseInput(context.Background()); err != nil {
		t.Fatalf("close vLLM input: %v", err)
	}
	_ = collectProviderEvents(stream.Events())
	if err := stream.Wait(context.Background()); !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("wait error = %v", err)
	}
}

func TestVLLMRealtimeConfigAndRequestValidation(t *testing.T) {
	t.Parallel()
	provider, err := NewVLLMRealtimeProvider(VLLMRealtimeConfig{})
	if err != nil {
		t.Fatalf("new default vLLM provider: %v", err)
	}
	if provider.Name() != defaultVLLMRealtimeName || provider.Model() != defaultVLLMRealtimeModel ||
		provider.cfg.Endpoint != defaultVLLMRealtimeEndpoint || provider.ServerVADEnabled() {
		t.Fatalf("unexpected vLLM defaults: %+v", provider)
	}
	capabilities := provider.StreamingCapabilities()
	if !slices.Equal(capabilities.Formats, []AudioFormat{AudioFormatRawPCM16}) ||
		!slices.Equal(capabilities.SampleRates, []int{vllmRealtimeSampleRate}) ||
		capabilities.SupportsPrompt || capabilities.SupportsTerms ||
		capabilities.SupportsLanguageHints || capabilities.SupportsServerVAD {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	invalidConfigs := []VLLMRealtimeConfig{
		{Endpoint: "file:///tmp/vllm.sock"},
		{Endpoint: "ws://example.com/v1/realtime"},
		{Endpoint: "ws://127.0.0.1:8000/not-realtime"},
		{APIKey: "first\nsecond"},
	}
	for index, config := range invalidConfigs {
		if _, err := NewVLLMRealtimeProvider(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("config %d error = %v", index, err)
		}
	}
	request := vllmTestStreamingRequest()
	request.SampleRate = 24000
	if _, err := provider.Start(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("sample-rate error = %v", err)
	}
}

func TestVLLMRealtimeRejectsOutOfOrderAudio(t *testing.T) {
	t.Parallel()
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.WriteJSON(vllmRealtimeServerEvent{Type: vllmEventSessionCreated, ID: "sequence"})
		_ = verifyVLLMInitialization(conn)
		<-request.Context().Done()
	}))
	defer server.Close()
	provider, err := NewVLLMRealtimeProvider(VLLMRealtimeConfig{
		Endpoint: vllmTestWebSocketURL(server.URL),
	})
	if err != nil {
		t.Fatalf("new vLLM provider: %v", err)
	}
	stream, err := provider.Start(context.Background(), vllmTestStreamingRequest())
	if err != nil {
		t.Fatalf("start vLLM stream: %v", err)
	}
	defer stream.Close()
	err = stream.WriteAudio(context.Background(), StreamingAudioChunk{
		Data:        make([]byte, 3200),
		Sequence:    1,
		StartSample: 0,
		EndSample:   1600,
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("sequence error = %v", err)
	}
}

func verifyVLLMInitialization(conn *websocket.Conn) error {
	var update map[string]any
	if err := conn.ReadJSON(&update); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	if update["type"] != vllmEventSessionUpdate || update["model"] == "" {
		return ErrProviderRequest
	}
	var commit map[string]any
	if err := conn.ReadJSON(&commit); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	if commit["type"] != vllmEventAudioCommit || commit["final"] != nil {
		return ErrProviderRequest
	}
	return nil
}

func verifyVLLMAudioChunk(conn *websocket.Conn, expectedBytes int) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	if event["type"] != vllmEventAudioAppend {
		return ErrProviderRequest
	}
	encoded, ok := event["audio"].(string)
	if !ok {
		return ErrProviderRequest
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(decoded) != expectedBytes {
		return ErrProviderRequest
	}
	return nil
}

func verifyVLLMFinalCommit(conn *websocket.Conn) error {
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	if event["type"] != vllmEventAudioCommit || event["final"] != true {
		return ErrProviderRequest
	}
	return nil
}

func vllmTestStreamingRequest() StreamingRequest {
	return StreamingRequest{
		SessionID:  "test-session",
		Language:   "auto",
		SampleRate: vllmRealtimeSampleRate,
		Channels:   1,
		Format:     AudioFormatRawPCM16,
	}
}

func vllmTestWebSocketURL(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http") + vllmRealtimeEndpointPath
}

func collectProviderEvents(events <-chan ProviderStreamEvent) []ProviderStreamEvent {
	collected := make([]ProviderStreamEvent, 0, 4)
	for event := range events {
		collected = append(collected, event)
	}
	return collected
}
