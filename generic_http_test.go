package asr

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestGenericHTTPProviderSendsContextAndParsesResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
		}
		if request.Header.Get("X-Request-ID") != "request-1" {
			t.Errorf("X-Request-ID = %q", request.Header.Get("X-Request-ID"))
		}
		if err := request.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("parse multipart form: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		checks := map[string]string{
			"model":           "multilingual-asr",
			"language":        "zh-Hans",
			"prompt":          "星河系统发布会",
			"hotwords":        `["星河系统","Qwen ASR"]`,
			"language_hints":  `["zh-Hans","en"]`,
			"response_format": "json",
		}
		for field, want := range checks {
			if got := request.FormValue(field); got != want {
				t.Errorf("form field %s = %q, want %q", field, got, want)
			}
		}
		file, _, err := request.FormFile("audio")
		if err != nil {
			t.Errorf("read audio form file: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		defer func() {
			if closeErr := file.Close(); closeErr != nil {
				t.Errorf("close audio form file: %v", closeErr)
			}
		}()
		data, err := io.ReadAll(file)
		if err != nil {
			t.Errorf("read audio data: %v", err)
		}
		if len(data) < 44 || string(data[:4]) != "RIFF" {
			t.Errorf("audio is not PCM WAV: %q", data)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"text":"星河系统 supports العربية","language":"zh","usage":{"type":"duration","seconds":7},"words":[{"word":"星河系统","start":0.1,"end":0.8,"confidence":0.95}]}`))
	}))
	defer server.Close()

	provider, err := NewGenericHTTPProvider(GenericHTTPConfig{
		Name:                  "generic",
		Model:                 "multilingual-asr",
		BaseURL:               server.URL,
		Path:                  "/transcribe",
		APIKey:                "secret",
		FileField:             "audio",
		AllowInsecureHTTP:     true,
		SupportsLanguageHints: true,
		AudioFormat:           AudioFormatWAVPCM16,
		ExtraFields: map[string]string{
			"response_format": "json",
		},
	})
	if err != nil {
		t.Fatalf("new generic provider: %v", err)
	}
	client, err := NewClient(provider, ClientConfig{AudioFormat: AudioFormatWAVPCM16})
	if err != nil {
		t.Fatalf("new ASR client: %v", err)
	}
	result, err := client.Transcribe(context.Background(), TranscriptionRequest{
		RequestID:     "request-1",
		SessionID:     "session-1",
		Language:      "zh-Hans",
		LanguageHints: []string{"zh-Hans", "en"},
		Context: RecognitionContext{
			Prompt: "星河系统发布会",
			Terms:  []string{"星河系统", "Qwen ASR"},
		},
		Samples:    make([]float32, 320),
		SampleRate: 16_000,
		Channels:   1,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Text != "星河系统 supports العربية" || len(result.Words) != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestClientRetriesSameProviderOnce(t *testing.T) {
	var attempts atomic.Int32
	provider := &testProvider{
		transcribe: func(_ context.Context, _ ProviderRequest) (ProviderResult, error) {
			if attempts.Add(1) == 1 {
				return ProviderResult{}, ErrProviderUnavailable
			}
			return ProviderResult{Text: "ok", Provider: "test", Model: "model"}, nil
		},
	}
	client, err := NewClient(provider, ClientConfig{
		RequestTimeout: time.Second,
		RetryCount:     1,
		MaxConcurrency: 1,
		AudioFormat:    AudioFormatRawPCM16,
	})
	if err != nil {
		t.Fatalf("new ASR client: %v", err)
	}
	result, err := client.Transcribe(context.Background(), TranscriptionRequest{
		RequestID: "request", SessionID: "session", Samples: []float32{0}, SampleRate: 16_000, Channels: 1,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Text != "ok" || attempts.Load() != 2 {
		t.Fatalf("result = %+v, attempts = %d", result, attempts.Load())
	}
}

func TestClientClassifiesEmptyPreviewAsNoSpeechWithoutRetry(t *testing.T) {
	var attempts atomic.Int32
	provider := &testProvider{
		transcribe: func(_ context.Context, _ ProviderRequest) (ProviderResult, error) {
			attempts.Add(1)
			return ProviderResult{}, ErrNoSpeech
		},
	}
	client, err := NewClient(provider, ClientConfig{
		RequestTimeout: time.Second,
		RetryCount:     1,
		MaxConcurrency: 1,
		AudioFormat:    AudioFormatRawPCM16,
	})
	if err != nil {
		t.Fatalf("new ASR client: %v", err)
	}
	_, err = client.Transcribe(context.Background(), TranscriptionRequest{
		RequestID: "empty-preview", SessionID: "session", Samples: []float32{0}, SampleRate: 16_000, Channels: 1,
	})
	if !errors.Is(err, ErrNoSpeech) || errors.Is(err, ErrProviderRequest) {
		t.Fatalf("empty preview error = %v, want only ErrNoSpeech", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("empty preview attempts = %d, want 1", attempts.Load())
	}
}

func TestGenericHTTPProviderRejectsUnsafeBaseURL(t *testing.T) {
	_, err := NewGenericHTTPProvider(GenericHTTPConfig{Name: "test", Model: "model", BaseURL: "file:///etc", Path: "/asr", AllowInsecureHTTP: true})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v, want ErrInvalidConfig", err)
	}
}

func TestGenericHTTPProviderRequiresConfiguredAPIKey(t *testing.T) {
	_, err := NewGenericHTTPProvider(GenericHTTPConfig{
		Name: "test", Model: "model", BaseURL: "https://asr.example.com", Path: "/asr", RequireAPIKey: true,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v, want ErrInvalidConfig", err)
	}
}

func TestStripLeadingLanguageLabel(t *testing.T) {
	tests := map[string]string{
		"[English] Hello.":   "Hello.",
		"[zh-CN] 你好":         "你好",
		"[日本語] こんにちは":        "こんにちは",
		"[not:a-label] keep": "[not:a-label] keep",
		"spoken text":        "spoken text",
	}
	for input, want := range tests {
		if got := stripLeadingLanguageLabel(input); got != want {
			t.Fatalf("strip label from %q = %q, want %q", input, got, want)
		}
	}
}

type testProvider struct {
	transcribe func(context.Context, ProviderRequest) (ProviderResult, error)
}

func (p *testProvider) Name() string { return "test" }

func (p *testProvider) Model() string { return "model" }

func (p *testProvider) Capabilities() Capabilities {
	return Capabilities{Formats: []AudioFormat{AudioFormatRawPCM16}}
}

func (p *testProvider) Transcribe(ctx context.Context, request ProviderRequest) (ProviderResult, error) {
	return p.transcribe(ctx, request)
}
