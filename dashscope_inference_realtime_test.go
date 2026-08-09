package asr

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDashScopeInferenceRealtimeSessionProtocol(t *testing.T) {
	t.Parallel()
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != dashScopeInferencePath || request.URL.RawQuery != "" ||
			request.Header.Get("Authorization") != "Bearer test-key" ||
			request.Header.Get("X-DashScope-WorkSpace") != "workspace-1" ||
			request.Header.Get("User-Agent") != "asr-sdk-test" {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		defer func() { _ = conn.Close() }()
		taskID, err := verifyDashScopeRunTask(conn)
		if err != nil {
			serverErr <- err
			return
		}
		if err := conn.WriteJSON(dashScopeServerEvent{Header: dashScopeTaskHeader{
			TaskID: taskID,
			Event:  dashScopeEventTaskStarted,
		}}); err != nil {
			serverErr <- errors.Join(ErrProviderResponse, err)
			return
		}
		if err := verifyDashScopeBinaryAudio(conn, 3200); err != nil {
			serverErr <- err
			return
		}
		responses := []dashScopeServerEvent{
			{
				Header: dashScopeTaskHeader{TaskID: taskID, Event: dashScopeEventResultGenerated},
				Payload: dashScopeServerPayload{Output: dashScopeServerOutput{Sentence: &dashScopeSentence{
					Heartbeat: true,
				}}},
			},
			{
				Header: dashScopeTaskHeader{TaskID: taskID, Event: dashScopeEventResultGenerated},
				Payload: dashScopeServerPayload{Output: dashScopeServerOutput{Sentence: &dashScopeSentence{
					BeginTime: 100, EndTime: 800, Text: "今天天气", SentenceID: 1,
				}}},
			},
		}
		for _, event := range responses {
			if err := conn.WriteJSON(event); err != nil {
				serverErr <- errors.Join(ErrProviderResponse, err)
				return
			}
		}
		if err := verifyDashScopeFinishTask(conn, taskID); err != nil {
			serverErr <- err
			return
		}
		finalEvents := []dashScopeServerEvent{
			{
				Header: dashScopeTaskHeader{TaskID: taskID, Event: dashScopeEventResultGenerated},
				Payload: dashScopeServerPayload{Output: dashScopeServerOutput{Sentence: &dashScopeSentence{
					BeginTime: 100, EndTime: 1000, Text: "今天天气怎么样？", SentenceEnd: true, SentenceID: 1,
				}}},
			},
			{Header: dashScopeTaskHeader{TaskID: taskID, Event: dashScopeEventTaskFinished}},
		}
		for _, event := range finalEvents {
			if err := conn.WriteJSON(event); err != nil {
				serverErr <- errors.Join(ErrProviderResponse, err)
				return
			}
		}
		serverErr <- nil
	}))
	defer server.Close()

	threshold := 0.2
	provider, err := NewDashScopeInferenceRealtimeProvider(DashScopeInferenceRealtimeConfig{
		Endpoint:                   "ws" + strings.TrimPrefix(server.URL, "http") + dashScopeInferencePath,
		APIKey:                     "test-key",
		WorkspaceID:                "workspace-1",
		UserAgent:                  "asr-sdk-test",
		VocabularyWeight:           5,
		SemanticPunctuationEnabled: true,
		MaxSentenceSilence:         700 * time.Millisecond,
		MultiThresholdModeEnabled:  true,
		Heartbeat:                  true,
		SpeechNoiseThreshold:       &threshold,
		FinishTimeout:              2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new DashScope provider: %v", err)
	}
	session, err := NewRealtimeSession(context.Background(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID:     "dashscope-client-session",
			Language:      "zh-CN",
			LanguageHints: []string{"en-US"},
			Context: RecognitionContext{
				Prompt: "天气问答",
				Terms:  []string{"气象局", "百炼", "气象局"},
			},
			SampleRate: 16000,
			Channels:   1,
			Format:     AudioFormatRawPCM16,
			ServerVAD:  true,
		},
		ChunkDuration: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new DashScope realtime session: %v", err)
	}
	defer session.Close()
	if err := session.Push(context.Background(), AudioChunk{Samples: make([]float32, 1600)}); err != nil {
		t.Fatalf("push DashScope audio: %v", err)
	}
	if err := session.Finish(context.Background(), FinalAudioChunk{}); err != nil {
		t.Fatalf("finish DashScope audio: %v", err)
	}
	events := make([]Event, 0, 4)
	for event := range session.Events() {
		events = append(events, event)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Wait(waitCtx); err != nil {
		t.Fatalf("wait DashScope session: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("mock DashScope server: %v", err)
	}
	assertDashScopeRealtimeEvents(t, events)
}

func TestDashScopeInferenceRealtimeDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	provider, err := NewDashScopeInferenceRealtimeProvider(DashScopeInferenceRealtimeConfig{
		Endpoint: "wss://workspace.example.com" + dashScopeInferencePath,
		APIKey:   "test-key",
	})
	if err != nil {
		t.Fatalf("new provider defaults: %v", err)
	}
	if provider.Name() != defaultDashScopeInferenceRealtimeName ||
		provider.Model() != defaultDashScopeInferenceRealtimeModel ||
		provider.cfg.MaxSentenceSilence != defaultDashScopeInferenceSentenceSilence ||
		provider.cfg.VocabularyWeight != defaultDashScopeInferenceVocabularyWeight ||
		!provider.ServerVADEnabled() {
		t.Fatalf("unexpected defaults: %+v", provider.cfg)
	}
	capabilities := provider.StreamingCapabilities()
	if !capabilities.SupportsPrompt || !capabilities.SupportsTerms ||
		!capabilities.SupportsLanguageHints || !capabilities.SupportsServerVAD {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	belowThreshold := -1.1
	invalid := []DashScopeInferenceRealtimeConfig{
		{},
		{Endpoint: "wss://workspace.example.com/v1/realtime", APIKey: "test-key"},
		{Endpoint: "wss://workspace.example.com" + dashScopeInferencePath + "?model=x", APIKey: "test-key"},
		{Endpoint: "file:///tmp/asr.sock", APIKey: "test-key"},
		{Endpoint: "wss://workspace.example.com" + dashScopeInferencePath, APIKey: "bad\nkey"},
		{Endpoint: "wss://workspace.example.com" + dashScopeInferencePath, APIKey: "test-key", VocabularyWeight: 6},
		{Endpoint: "wss://workspace.example.com" + dashScopeInferencePath, APIKey: "test-key", MaxSentenceSilence: 100 * time.Millisecond},
		{Endpoint: "wss://workspace.example.com" + dashScopeInferencePath, APIKey: "test-key", SpeechNoiseThreshold: &belowThreshold},
	}
	for index, config := range invalid {
		if _, err := NewDashScopeInferenceRealtimeProvider(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid config %d error = %v", index, err)
		}
	}
}

func TestDashScopeInferenceRealtimeClassifiesTaskFailure(t *testing.T) {
	t.Parallel()
	upgrader := websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		var runTask dashScopeClientEvent
		if err := conn.ReadJSON(&runTask); err != nil {
			return
		}
		_ = conn.WriteJSON(dashScopeServerEvent{Header: dashScopeTaskHeader{
			TaskID:    runTask.Header.TaskID,
			Event:     dashScopeEventTaskFailed,
			ErrorCode: "RATE_LIMIT_EXCEEDED",
			ErrorText: "quota limit reached",
		}})
	}))
	defer server.Close()
	provider, err := NewDashScopeInferenceRealtimeProvider(DashScopeInferenceRealtimeConfig{
		Endpoint: "ws" + strings.TrimPrefix(server.URL, "http") + dashScopeInferencePath,
		APIKey:   "test-key",
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	_, err = provider.Start(context.Background(), StreamingRequest{
		SessionID:  "dashscope-rate-limit",
		Language:   "auto",
		SampleRate: 16000,
		Channels:   1,
		Format:     AudioFormatRawPCM16,
	})
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("error = %v, want ErrRateLimited", err)
	}
}

func verifyDashScopeRunTask(conn *websocket.Conn) (string, error) {
	var event dashScopeClientEvent
	if err := conn.ReadJSON(&event); err != nil {
		return "", errors.Join(ErrProviderResponse, err)
	}
	parameters := event.Payload.Parameters
	if event.Header.Action != dashScopeActionRunTask || event.Header.Streaming != dashScopeStreamingDuplex ||
		len(event.Header.TaskID) != 36 || event.Payload.TaskGroup != dashScopeTaskGroupAudio ||
		event.Payload.Task != dashScopeTaskASR || event.Payload.Function != dashScopeFunctionRecognition ||
		event.Payload.Model != defaultDashScopeInferenceRealtimeModel || parameters == nil ||
		parameters.Format != dashScopeAudioFormatPCM || parameters.SampleRate != 16000 ||
		parameters.MaxSentenceSilence != 700 || !parameters.SemanticPunctuationEnabled ||
		!parameters.MultiThresholdModeEnabled || !parameters.Heartbeat ||
		parameters.SpeechNoiseThreshold == nil || *parameters.SpeechNoiseThreshold != 0.2 ||
		len(parameters.LanguageHints) != 1 || parameters.LanguageHints[0] != "zh" ||
		parameters.Vocabulary["气象局"] != 5 || parameters.Vocabulary["百炼"] != 5 {
		return "", ErrProviderRequest
	}
	if len(event.Payload.Input.Context) != 1 ||
		len(event.Payload.Input.Context[0].Content) != 1 ||
		event.Payload.Input.Context[0].Content[0].Text != "天气问答" {
		return "", ErrProviderRequest
	}
	return event.Header.TaskID, nil
}

func verifyDashScopeBinaryAudio(conn *websocket.Conn, wantBytes int) error {
	messageType, payload, err := conn.ReadMessage()
	if err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	if messageType != websocket.BinaryMessage || len(payload) != wantBytes {
		return ErrProviderRequest
	}
	return nil
}

func verifyDashScopeFinishTask(conn *websocket.Conn, taskID string) error {
	var event dashScopeClientEvent
	if err := conn.ReadJSON(&event); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	if event.Header.Action != dashScopeActionFinishTask || event.Header.TaskID != taskID ||
		event.Header.Streaming != dashScopeStreamingDuplex || event.Payload.Input.Context != nil {
		return ErrProviderRequest
	}
	return nil
}

func assertDashScopeRealtimeEvents(t *testing.T, events []Event) {
	t.Helper()
	if len(events) != 4 || events[0].Type != EventSessionReady {
		t.Fatalf("events = %+v", events)
	}
	partial := events[1].Segment
	final := events[2].Segment
	if partial == nil || partial.SegmentIndex != 0 || partial.Revision != 1 ||
		partial.State != TranscriptStateProvisional || partial.Text != "今天天气" {
		t.Fatalf("partial = %+v", partial)
	}
	if final == nil || final.SegmentIndex != partial.SegmentIndex || final.Revision != 2 ||
		final.State != TranscriptStateStable || final.Text != "今天天气怎么样？" ||
		final.FinalizationReason != FinalizationProviderFinal || final.EvidenceQuality != EvidenceProviderFinal {
		t.Fatalf("final = %+v", final)
	}
	if events[3].Type != EventCompleted {
		t.Fatalf("completed = %+v", events[3])
	}
}
