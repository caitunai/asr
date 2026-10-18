package asr

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testQwen31Model = "qwen-audio-3.1-asr-flash"

func TestQwenHTTP31Parameters(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		diarization *bool
		dialect     *bool
	}{
		{name: "defaults"},
		{name: "dialect", dialect: new(true)},
		{name: "explicit_false", diarization: new(false), dialect: new(false)},
		{name: "diarization", diarization: new(true), dialect: new(true)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Model      string                     `json:"model"`
					Parameters map[string]json.RawMessage `json:"parameters"`
				}
				if err := readJSONBody(r, &payload); err != nil {
					t.Errorf("decode request: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if payload.Model != testQwen31Model || string(payload.Parameters["sample_rate"]) != `"16000"` ||
					r.Header.Get("X-DashScope-SSE") != "disable" {
					t.Errorf("unexpected request: %+v", payload)
				}
				assertQwenOptionalBool(t, payload.Parameters, "speaker_diarization_enabled", test.diarization)
				if test.diarization != nil && *test.diarization {
					assertQwenOptionalBool(t, payload.Parameters, "keep_dialect", nil)
					if _, exists := payload.Parameters["language_hints"]; exists {
						t.Error("diarization must omit ineffective language hints")
					}
				} else {
					assertQwenOptionalBool(t, payload.Parameters, "keep_dialect", test.dialect)
					if string(payload.Parameters["language_hints"]) != `["zh"]` {
						t.Errorf("language hints = %s", payload.Parameters["language_hints"])
					}
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"output":{"sentence":{"text":"你好。","sentence_id":1,"sentence_end":true}}}`)
			}))
			defer server.Close()
			provider, err := NewQwenHTTPProvider(QwenHTTPConfig{
				BaseURL: server.URL, APIKey: "test-key", Model: testQwen31Model,
				SpeakerDiarizationEnabled: test.diarization, KeepDialect: test.dialect,
			})
			if err != nil {
				t.Fatal(err)
			}
			result := transcribeWithProvider(t, provider, TranscriptionRequest{
				RequestID: "qwen31", SessionID: "session", Language: "zh-CN",
				Samples: make([]float32, 1600), SampleRate: 16000, Channels: 1,
			})
			if result.Text != "你好。" || len(result.Sentences) != 1 || !result.Sentences[0].Final {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func assertQwenOptionalBool(t *testing.T, parameters map[string]json.RawMessage, key string, want *bool) {
	t.Helper()
	value, exists := parameters[key]
	if want == nil {
		if exists {
			t.Errorf("%s must be omitted, got %s", key, value)
		}
		return
	}
	var actual bool
	if !exists || json.Unmarshal(value, &actual) != nil || actual != *want {
		t.Errorf("%s = %s, want %v", key, value, *want)
	}
}

func TestQwenHTTP31DiarizationResponse(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"output":{"sentences":[
				{"sentence_id":1,"speaker_id":0,"text":"Hello.","begin_time":100,"end_time":700,"sentence_end":true,
				 "words":[{"text":"Hello","punctuation":".","begin_time":100,"end_time":700,"speaker_id":0,"fixed":true}]},
				{"sentence_id":2,"speaker_id":1,"text":" Hi.","begin_time":900,"end_time":1300,"sentence_end":true}
			],"sentence":{"text":" Hi."}},
			"usage":{"duration":2,"input_tokens":10,"output_tokens":5,"total_tokens":15}
		}`)
	}))
	defer server.Close()
	provider, err := NewQwenHTTPProvider(QwenHTTPConfig{
		BaseURL: server.URL, APIKey: "test-key", Model: testQwen31Model,
		SpeakerDiarizationEnabled: new(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	result := transcribeWithProvider(t, provider, TranscriptionRequest{
		RequestID: "speakers", SessionID: "session", Language: "auto",
		Samples: make([]float32, 32000), SampleRate: 16000, Channels: 1,
	})
	if result.Text != "Hello. Hi." || len(result.Sentences) != 2 || result.Usage == nil ||
		result.Usage.TotalTokens != 15 || result.Usage.Duration != 2 {
		t.Fatalf("result = %+v", result)
	}
	first := result.Sentences[0]
	if first.SpeakerID == nil || *first.SpeakerID != 0 || first.StartAt != 0.1 || first.EndAt != 0.7 ||
		len(first.Words) != 1 || !first.Words[0].Fixed || first.Words[0].Punctuation != "." ||
		first.Words[0].SpeakerID == nil || *first.Words[0].SpeakerID != 0 {
		t.Fatalf("sentence = %+v", first)
	}
	// The current sentence may cover only part of cumulative output.text. Keep
	// detailed timestamps out of alignment's complete-window Words evidence.
	if len(result.Words) != 0 {
		t.Fatal("partial sentence timestamps must not be used for full-window alignment")
	}
}

func TestQwenHTTPRejects31OptionsOnOlderModel(t *testing.T) {
	t.Parallel()
	_, err := NewQwenHTTPProvider(QwenHTTPConfig{
		BaseURL: "https://example.com", APIKey: "test-key", Model: "qwen-audio-3.0-asr-flash",
		KeepDialect: new(true),
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v", err)
	}
}

func TestSessionPreservesProviderDetails(t *testing.T) {
	t.Parallel()
	for _, strategy := range []SegmentRecognitionStrategy{SegmentRecognitionStrategyContextual, SegmentRecognitionStrategySingle} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()
			recognizer := &recordingRecognizer{transcribe: func(request TranscriptionRequest) (TranscriptionResult, error) {
				result := testTranscriptionResult(request, "你好")
				result.Sentences = []TranscriptSentence{{Text: "你好", SpeakerID: new(0)}}
				result.Usage = &RecognitionUsage{TotalTokens: 15}
				return result, nil
			}}
			session, err := NewSession(recognizer, nil, SessionConfig{
				SessionID: "metadata", SegmentStrategy: strategy, SampleRate: 1000, Channels: 1,
				MaxWindowDuration: 3 * time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(session.Close)
			if err := session.AddSegment(t.Context(), testSegment(1)); err != nil {
				t.Fatal(err)
			}
			if err := session.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			for _, event := range collectUntilCompleted(t, session.Events()) {
				var sentences []TranscriptSentence
				var usage *RecognitionUsage
				if event.Window != nil {
					sentences, usage = event.Window.Sentences, event.Window.Usage
				} else if strategy == SegmentRecognitionStrategySingle && event.Segment != nil {
					sentences, usage = event.Segment.Sentences, event.Segment.Usage
				}
				if len(sentences) == 1 && sentences[0].SpeakerID != nil && *sentences[0].SpeakerID == 0 &&
					usage != nil && usage.TotalTokens == 15 {
					return
				}
			}
			t.Fatal("provider details missing from session events")
		})
	}
}
