package asr

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDashScopeMessageProtocol(t *testing.T) {
	t.Parallel()
	for _, model := range []string{QwenASRMessageModel, QwenASRStreamingModel} {
		t.Run(model, func(t *testing.T) { testDashScope31Protocol(t, model) })
	}
}

func testDashScope31Protocol(t *testing.T, model string) {
	t.Helper()
	messageModel := isQwenMessageModel(model)
	sampleRate := 24000
	if messageModel {
		sampleRate = 16000
	}
	chunkSamples := sampleRate / 10
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		var run dashScopeClientEvent
		if err := conn.ReadJSON(&run); err != nil {
			t.Error(err)
			return
		}
		p := run.Payload.Parameters
		if p == nil || run.Header.Action != dashScopeActionRunTask || run.Payload.Model != model ||
			p.SampleRate != sampleRate || p.Format != "pcm" || p.MaxSentenceSilence != 800 ||
			p.VADModel != defaultQwenMessageVAD || p.KeepDialect == nil || !*p.KeepDialect ||
			!slices.Equal(p.LanguageHints, []string{"zh", "en"}) || p.Vocabulary["百炼"] != 5 {
			t.Errorf("run task = %+v parameters=%+v", run, p)
			return
		}
		if messageModel {
			if p.IntermediateResultEnabled == nil || !*p.IntermediateResultEnabled ||
				p.DisfluencyRemovalEnabled == nil || *p.DisfluencyRemovalEnabled {
				t.Error("missing Message parameters")
				return
			}
		} else if p.IntermediateResultEnabled != nil || p.DisfluencyRemovalEnabled != nil ||
			!p.MultiThresholdModeEnabled || p.SpecialWordFilter != "{}" || p.VocabularyID != "vocab-test" {
			t.Error("incorrect Streaming parameters")
			return
		}
		if len(run.Payload.Input.Context) != 2 || run.Payload.Input.Context[1].Role != RecognitionContextRoleAssistant {
			t.Errorf("context = %+v", run.Payload.Input)
			return
		}
		id := run.Header.TaskID
		send := func(event string, sentence *dashScopeSentence, usage *RecognitionUsage) bool {
			err := conn.WriteJSON(dashScopeServerEvent{
				Header:  dashScopeTaskHeader{TaskID: id, Event: event},
				Payload: dashScopeServerPayload{Output: dashScopeServerOutput{Sentence: sentence}, Usage: usage},
			})
			if err != nil {
				t.Error(err)
			}
			return err == nil
		}
		if !send(dashScopeEventTaskStarted, nil, nil) {
			return
		}
		if err := verifyDashScopeBinaryAudio(conn, chunkSamples*2); err != nil {
			t.Error(err)
			return
		}
		for _, sentence := range []*dashScopeSentence{
			{Heartbeat: true},
			{SentenceID: 1}, // Empty start must not create a visible result.
			{SentenceID: 1, Text: "你好"},
			{SentenceID: 1, Text: "你好 world", EndTime: 100},
			{
				SentenceID: 1, Text: "你好 world。", EndTime: 100, SentenceEnd: true,
				Words: []dashScopeWord{{Text: "你好", EndTime: 50, Fixed: false}},
			},
			{SentenceID: 1, Text: "你好 world。", SentenceEnd: true}, // Duplicate final.
			{SentenceID: 1, Text: "late draft"},
		} {
			if !send(dashScopeEventResultGenerated, sentence, &RecognitionUsage{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}) {
				return
			}
		}
		if messageModel {
			var update dashScopeClientEvent
			if err := conn.ReadJSON(&update); err != nil {
				t.Error(err)
				return
			}
			if update.Header.Action != dashScopeActionContinueTask || update.Header.TaskID != id ||
				len(update.Payload.Input.Context) != 3 || update.Payload.Input.Context[2].Content[0].Text != "你好 world。" {
				t.Errorf("update = %+v", update)
				return
			}
		}
		// Streaming must send PCM next, never an undocumented continue-task.
		if err := verifyDashScopeBinaryAudio(conn, chunkSamples*2); err != nil {
			t.Error(err)
			return
		}
		if err := verifyDashScopeFinishTask(conn, id); err != nil {
			t.Error(err)
			return
		}
		send(dashScopeEventTaskFinished, nil, &RecognitionUsage{InputTokens: 20, OutputTokens: 3, TotalTokens: 23})
	}))
	defer server.Close()
	cfg := DashScopeInferenceRealtimeConfig{
		Endpoint: "ws" + strings.TrimPrefix(server.URL, "http") + dashScopeInferencePath,
		APIKey:   "test-key", Model: model, KeepDialect: new(true),
	}
	if messageModel {
		cfg.DisfluencyRemovalEnabled = new(false)
	} else {
		cfg.MultiThresholdModeEnabled = true
		cfg.SpecialWordFilter = "{}"
		cfg.VocabularyID = "vocab-test"
	}
	provider, err := NewDashScopeInferenceRealtimeProvider(cfg)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewRealtimeSession(t.Context(), provider, RealtimeSessionConfig{
		Request: StreamingRequest{
			SessionID: "qwen31", SampleRate: sampleRate, Channels: 1, Format: AudioFormatRawPCM16,
			Language: "auto", LanguageHints: []string{"zh", "en"},
			Context: RecognitionContext{Terms: []string{"百炼"}, Messages: []RecognitionContextMessage{
				{Role: RecognitionContextRoleUser, Text: "前轮问题"},
				{Role: RecognitionContextRoleAssistant, Text: "前轮回复"},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Close)
	if !messageModel {
		if provider.StreamingCapabilities().SupportsContextUpdate {
			t.Fatal("Streaming must not advertise dynamic context")
		}
		if err := session.UpdateContext(t.Context(), RecognitionContext{Prompt: "new"}); !errors.Is(err, ErrContextUpdateUnsupported) {
			t.Fatalf("context error = %v", err)
		}
	}
	if err := session.Push(t.Context(), AudioChunk{Samples: make([]float32, chunkSamples)}); err != nil {
		t.Fatal(err)
	}
	var segments []SegmentResult
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-session.Events():
			if !ok {
				t.Fatal("closed before completed")
			}
			if event.Error != nil {
				t.Fatalf("event error: %+v", event.Error)
			}
			if event.Segment != nil {
				segments = append(segments, *event.Segment)
				if event.Segment.State == TranscriptStateStable {
					if err := session.Push(t.Context(), AudioChunk{Samples: make([]float32, chunkSamples)}); err != nil {
						t.Fatal(err)
					}
					if err := session.Finish(t.Context(), FinalAudioChunk{}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if event.Type == EventCompleted {
				if event.Usage == nil || event.Usage.TotalTokens != 23 {
					t.Fatalf("usage=%+v", event.Usage)
				}
				if len(segments) != 3 || segments[0].State != TranscriptStatePreview || segments[1].State != TranscriptStatePreview ||
					segments[2].State != TranscriptStateStable || segments[2].Revision != 3 || segments[0].SegmentIndex != segments[2].SegmentIndex {
					t.Fatalf("segments=%+v", segments)
				}
				if len(segments[2].Sentences) != 1 || len(segments[2].Sentences[0].Words) != 1 || segments[2].Sentences[0].Words[0].Fixed {
					t.Fatalf("details=%+v", segments[2].Sentences)
				}
				return
			}
		case <-timer.C:
			t.Fatal("message protocol timeout")
		}
	}
}

func TestDashScopeMessageConfigAndContext(t *testing.T) {
	t.Parallel()
	base := DashScopeInferenceRealtimeConfig{Model: QwenASRMessageModel, Endpoint: "wss://example.com" + dashScopeInferencePath, APIKey: "key"}
	for _, modify := range []func(*DashScopeInferenceRealtimeConfig){
		func(c *DashScopeInferenceRealtimeConfig) { c.VADModel = "unknown" },
		func(c *DashScopeInferenceRealtimeConfig) { c.SemanticPunctuationEnabled = true },
		func(c *DashScopeInferenceRealtimeConfig) { c.MaxSentenceSilence = 100 * time.Millisecond },
		func(c *DashScopeInferenceRealtimeConfig) {
			c.Model = defaultDashScopeInferenceRealtimeModel
			c.KeepDialect = new(true)
		},
	} {
		cfg := base
		modify(&cfg)
		if _, err := NewDashScopeInferenceRealtimeProvider(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("config error=%v", err)
		}
	}
	base.IntermediateResultEnabled = new(false)
	provider, err := NewDashScopeInferenceRealtimeProvider(base)
	if err != nil {
		t.Fatal(err)
	}
	if *provider.cfg.IntermediateResultEnabled || !slices.Equal(provider.StreamingCapabilities().SampleRates, []int{16000}) {
		t.Fatal("invalid defaults")
	}
	_, err = provider.Start(t.Context(), StreamingRequest{SessionID: "bad-rate", SampleRate: 24000, Channels: 1, Format: AudioFormatRawPCM16})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("sample-rate error=%v", err)
	}
	input, err := dashScopeDialogueInput(RecognitionContext{Messages: []RecognitionContextMessage{
		{Role: RecognitionContextRoleUser, Text: strings.Repeat("中", 400)},
		{Role: RecognitionContextRoleAssistant, Text: "must be truncated"},
	}}, []string{"latest"})
	if err != nil || len(input.Context) != 2 || input.Context[1].Content[0].Text != "latest" {
		t.Fatalf("context=%+v err=%v", input, err)
	}
}

func TestDashScopeStreamingConfig(t *testing.T) {
	t.Parallel()
	base := DashScopeInferenceRealtimeConfig{
		Model: QwenASRStreamingModel, Endpoint: "wss://example.com" + dashScopeInferencePath, APIKey: "key",
	}
	for _, modify := range []func(*DashScopeInferenceRealtimeConfig){
		func(c *DashScopeInferenceRealtimeConfig) { c.IntermediateResultEnabled = new(false) },
		func(c *DashScopeInferenceRealtimeConfig) { c.DisfluencyRemovalEnabled = new(false) },
		func(c *DashScopeInferenceRealtimeConfig) { c.SemanticPunctuationEnabled = true },
		func(c *DashScopeInferenceRealtimeConfig) { c.VADModel = "unknown" },
	} {
		cfg := base
		modify(&cfg)
		if _, err := NewDashScopeInferenceRealtimeProvider(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("config error=%v", err)
		}
	}
	base.KeepDialect = new(false)
	base.VADModel = "near_meeting_16k"
	provider, err := NewDashScopeInferenceRealtimeProvider(base)
	if err != nil {
		t.Fatal(err)
	}
	if provider.cfg.IntermediateResultEnabled != nil || *provider.cfg.KeepDialect ||
		provider.cfg.MaxSentenceSilence != 800*time.Millisecond {
		t.Fatalf("config=%+v", provider.cfg)
	}
}
