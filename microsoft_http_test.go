package asr

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMicrosoftHTTPProviderSendsWAVAndParsesDetailedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != defaultMicrosoftHTTPPath {
			t.Errorf("path = %q", request.URL.Path)
		}
		if request.URL.Query().Get("language") != "zh-CN" || request.URL.Query().Get("format") != "detailed" {
			t.Errorf("query = %v", request.URL.Query())
		}
		if request.Header.Get("Ocp-Apim-Subscription-Key") != "subscription-key" {
			t.Errorf("subscription key = %q", request.Header.Get("Ocp-Apim-Subscription-Key"))
		}
		if request.Header.Get("Authorization") != "" {
			t.Errorf("unexpected Authorization = %q", request.Header.Get("Authorization"))
		}
		if request.Header.Get("Content-Type") != "audio/wav; codecs=audio/pcm; samplerate=16000" {
			t.Errorf("Content-Type = %q", request.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if len(body) < 44 || string(body[:4]) != "RIFF" {
			t.Errorf("body is not WAV: %q", body)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{
			"RecognitionStatus":"Success",
			"DisplayText":"今天天气怎么样？",
			"NBest":[{"Confidence":0.95,"Display":"今天天气怎么样？","Lexical":"今天天气怎么样", "Words":[
				{"Word":"今天","Offset":1000000,"Duration":2000000},
				{"Word":"天气","Offset":3000000,"Duration":2500000}
			]}]
		}`)
	}))
	defer server.Close()

	provider, err := NewMicrosoftHTTPProvider(MicrosoftHTTPConfig{
		Endpoint: server.URL,
		APIKey:   "subscription-key",
	})
	if err != nil {
		t.Fatalf("new Microsoft provider: %v", err)
	}
	client, err := NewClient(provider, ClientConfig{AudioFormat: AudioFormatWAVPCM16})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	result, err := client.Transcribe(context.Background(), TranscriptionRequest{
		RequestID:  "request-1",
		SessionID:  "session-1",
		Language:   "zh-Hans",
		Samples:    make([]float32, 320),
		SampleRate: 16_000,
		Channels:   1,
	})
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if result.Text != "今天天气怎么样？" || result.Provider != defaultMicrosoftHTTPName ||
		result.Model != defaultMicrosoftHTTPModel || len(result.Words) != 2 {
		t.Fatalf("result = %+v", result)
	}
	if math.Abs(result.Words[0].StartAt-0.1) > 1e-9 || math.Abs(result.Words[0].EndAt-0.3) > 1e-9 ||
		result.Words[0].Confidence != 0.95 {
		t.Fatalf("first word = %+v", result.Words[0])
	}
}

func TestMicrosoftHTTPProviderUsesBearerAndDefaultLanguage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer token-value" {
			t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
		}
		if request.Header.Get("Ocp-Apim-Subscription-Key") != "" {
			t.Errorf("unexpected subscription key")
		}
		if request.URL.Query().Get("language") != "en-GB" {
			t.Errorf("language = %q", request.URL.Query().Get("language"))
		}
		_, _ = io.WriteString(response, `{"RecognitionStatus":"Success","NBest":[{"Display":"Where are you going?"}]}`)
	}))
	defer server.Close()

	provider, err := NewMicrosoftHTTPProvider(MicrosoftHTTPConfig{
		Endpoint:        strings.Replace(server.URL, "http://", "ws://", 1),
		APIKey:          "Bearer token-value",
		DefaultLanguage: "en-GB",
	})
	if err != nil {
		t.Fatalf("new Microsoft provider: %v", err)
	}
	audio, err := EncodeAudio(make([]float32, 160), 16_000, 1, AudioFormatWAVPCM16)
	if err != nil {
		t.Fatalf("encode audio: %v", err)
	}
	result, err := provider.Transcribe(context.Background(), ProviderRequest{
		RequestID: "request-2",
		Language:  "auto",
		Audio:     audio,
	})
	if err != nil || result.Text != "Where are you going?" {
		t.Fatalf("result = %+v, error = %v", result, err)
	}
}

func TestMicrosoftHTTPProviderClassifiesNoSpeech(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, `{"RecognitionStatus":"NoMatch"}`)
	}))
	defer server.Close()
	provider, err := NewMicrosoftHTTPProvider(MicrosoftHTTPConfig{Endpoint: server.URL, APIKey: "key"})
	if err != nil {
		t.Fatalf("new Microsoft provider: %v", err)
	}
	audio, err := EncodeAudio(make([]float32, 160), 16_000, 1, AudioFormatWAVPCM16)
	if err != nil {
		t.Fatalf("encode audio: %v", err)
	}
	_, err = provider.Transcribe(context.Background(), ProviderRequest{RequestID: "request", Language: "en", Audio: audio})
	if !errors.Is(err, ErrNoSpeech) {
		t.Fatalf("error = %v, want ErrNoSpeech", err)
	}
}

func TestMicrosoftHTTPProviderValidatesConfigAndResponse(t *testing.T) {
	tests := []MicrosoftHTTPConfig{
		{Endpoint: "https://speech.example.com", APIKey: ""},
		{Endpoint: "file:///etc/passwd", APIKey: "key"},
		{Endpoint: "https://user:secret@speech.example.com", APIKey: "key"},
		{Endpoint: "https://speech.example.com?key=value", APIKey: "key"},
		{Endpoint: "https://speech.example.com", APIKey: "key", AuthMode: "unknown"},
		{Endpoint: "https://speech.example.com", APIKey: "key", DefaultLanguage: "auto"},
	}
	for _, cfg := range tests {
		if _, err := NewMicrosoftHTTPProvider(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("config %+v error = %v", cfg, err)
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, `{"RecognitionStatus":"Error"}`)
	}))
	defer server.Close()
	provider, err := NewMicrosoftHTTPProvider(MicrosoftHTTPConfig{Endpoint: server.URL, APIKey: "key"})
	if err != nil {
		t.Fatalf("new Microsoft provider: %v", err)
	}
	audio, err := EncodeAudio(make([]float32, 160), 16_000, 1, AudioFormatWAVPCM16)
	if err != nil {
		t.Fatalf("encode audio: %v", err)
	}
	_, err = provider.Transcribe(context.Background(), ProviderRequest{RequestID: "request", Language: "en", Audio: audio})
	if !errors.Is(err, ErrProviderResponse) {
		t.Fatalf("error = %v, want ErrProviderResponse", err)
	}
}

func TestMicrosoftHTTPProviderNormalizesRegionalAndResourceEndpoints(t *testing.T) {
	tests := map[string]string{
		"https://eastus.stt.speech.microsoft.com":                                                    "https://eastus.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1",
		"https://speech-resource.cognitiveservices.azure.com":                                        "https://speech-resource.cognitiveservices.azure.com/stt/speech/recognition/conversation/cognitiveservices/v1",
		"wss://eastus.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1": "https://eastus.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1",
	}
	for endpoint, want := range tests {
		provider, err := NewMicrosoftHTTPProvider(MicrosoftHTTPConfig{Endpoint: endpoint, APIKey: "key"})
		if err != nil {
			t.Fatalf("normalize %q: %v", endpoint, err)
		}
		if provider.cfg.Endpoint != want {
			t.Fatalf("endpoint %q = %q, want %q", endpoint, provider.cfg.Endpoint, want)
		}
	}
}

func TestMicrosoftHTTPProviderRejectsUnsupportedAudio(t *testing.T) {
	provider, err := NewMicrosoftHTTPProvider(MicrosoftHTTPConfig{
		Endpoint: "https://eastus.stt.speech.microsoft.com",
		APIKey:   "key",
	})
	if err != nil {
		t.Fatalf("new Microsoft provider: %v", err)
	}
	audio8k, err := EncodeAudio(make([]float32, 80), 8_000, 1, AudioFormatWAVPCM16)
	if err != nil {
		t.Fatalf("encode 8k audio: %v", err)
	}
	_, err = provider.Transcribe(context.Background(), ProviderRequest{
		RequestID: "unsupported-rate",
		Language:  defaultMicrosoftLanguage,
		Audio:     audio8k,
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("8k error = %v, want ErrInvalidRequest", err)
	}

	tooLong := AudioPayload{
		Data:       make([]byte, microsoftWAVHeaderSize+microsoftHTTPAudioSampleRate*2*61),
		Format:     AudioFormatWAVPCM16,
		SampleRate: microsoftHTTPAudioSampleRate,
		Channels:   1,
	}
	_, err = provider.Transcribe(context.Background(), ProviderRequest{
		RequestID: "too-long",
		Language:  defaultMicrosoftLanguage,
		Audio:     tooLong,
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("long audio error = %v, want ErrInvalidRequest", err)
	}
}

func TestMicrosoftSpeechLocale(t *testing.T) {
	tests := map[string]string{
		"auto":       "en-US",
		"zh-Hans":    "zh-CN",
		"zh-Hant":    "zh-TW",
		"zh-Hant-HK": "zh-HK",
		"en":         "en-US",
		"pt":         "pt-BR",
		"fr-CA":      "fr-CA",
	}
	for input, want := range tests {
		got, err := microsoftSpeechLocale(input, defaultMicrosoftLanguage)
		if err != nil || got != want {
			t.Fatalf("locale %q = %q, %v; want %q", input, got, err, want)
		}
	}
}
