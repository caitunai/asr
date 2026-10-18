package asr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestQwenHTTPDashScopeMultimodalProtocol(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != defaultQwenHTTPNativePath ||
			request.Header.Get("Authorization") != "Bearer test-key" ||
			request.Header.Get("X-DashScope-SSE") != "disable" ||
			request.Header.Get("X-Request-ID") != "native-request" {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		var payload qwenHTTPNativeRequest
		if err := readJSONBody(request, &payload); err != nil {
			t.Errorf("decode native request: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		assertQwenHTTPNativeRequest(t, payload)
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"output":{"output":{"sentence":{"text":"今天天气怎么样？"}}}}`)
	}))
	defer server.Close()

	provider, err := NewQwenHTTPProvider(QwenHTTPConfig{
		Name:             "qwenHTTP",
		BaseURL:          server.URL,
		APIKey:           "test-key",
		VocabularyID:     "vocabulary-1",
		VocabularyWeight: 5,
	})
	if err != nil {
		t.Fatalf("new native provider: %v", err)
	}
	result := transcribeWithProvider(t, provider, TranscriptionRequest{
		RequestID:     "native-request",
		SessionID:     "native-session",
		Language:      "zh-CN",
		LanguageHints: []string{"en-US"},
		Context: RecognitionContext{
			Prompt: "气象领域",
			Terms:  []string{"中央气象台", "副热带高压"},
			Messages: []RecognitionContextMessage{
				{Role: RecognitionContextRoleUser, Text: "上一轮用户语音"},
				{Role: RecognitionContextRoleAssistant, Text: "上一轮模型回复"},
			},
		},
		Samples:    make([]float32, 1600),
		SampleRate: 16000,
		Channels:   1,
	})
	if result.Text != "今天天气怎么样？" || result.Provider != "qwenHTTP" ||
		result.Model != defaultQwenHTTPModel {
		t.Fatalf("result = %+v", result)
	}
}

func TestChatCompletionsProtocolAndLanguage(t *testing.T) {
	t.Parallel()
	enableITN := false
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/compatible-mode"+defaultChatCompletionsPath ||
			request.Header.Get("Authorization") != "Bearer test-key" ||
			request.Header.Get("X-DashScope-SSE") != "" {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		var payload qwenHTTPOpenAIRequest
		if err := readJSONBody(request, &payload); err != nil {
			t.Errorf("decode OpenAI request: %v", err)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		assertQwenHTTPOpenAIRequest(t, payload)
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"choices":[{"message":{"content":"Where are you going?"}}]}`)
	}))
	defer server.Close()

	provider, err := NewChatCompletionsProvider(ChatCompletionsConfig{
		Name:      "chatCompletions",
		Model:     "qwen3-asr-flash",
		BaseURL:   server.URL + "/compatible-mode",
		APIKey:    "test-key",
		EnableITN: &enableITN,
	})
	if err != nil {
		t.Fatalf("new OpenAI provider: %v", err)
	}
	result := transcribeWithProvider(t, provider, TranscriptionRequest{
		RequestID:     "openai-request",
		SessionID:     "openai-session",
		Language:      "auto",
		LanguageHints: []string{"en-US", "zh-CN"},
		Context: RecognitionContext{
			Prompt: "This is a weather interview.",
			Terms:  []string{"meteorological observatory"},
		},
		Samples:    make([]float32, 800),
		SampleRate: 16000,
		Channels:   1,
	})
	if result.Text != "Where are you going?" || result.Model != "qwen3-asr-flash" {
		t.Fatalf("result = %+v", result)
	}
}

func TestQwenHTTPValidationAndResponseContent(t *testing.T) {
	t.Parallel()
	invalid := []QwenHTTPConfig{
		{},
		{BaseURL: "https://example.com", APIKey: "bad\nkey"},
		{BaseURL: "http://example.com", APIKey: "test-key"},
		{BaseURL: "https://example.com?secret=x", APIKey: "test-key"},
		{BaseURL: "https://example.com", APIKey: "test-key", Path: "relative"},
		{BaseURL: "https://example.com", APIKey: "test-key", VocabularyWeight: 6},
	}
	for index, cfg := range invalid {
		if _, err := NewQwenHTTPProvider(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid config %d error = %v", index, err)
		}
	}
	content := qwenHTTPChoiceText([]byte(`[{"text":"Hello "},{"text":"world"}]`))
	if content != "Hello world" {
		t.Fatalf("choice content = %q", content)
	}
	if _, err := qwenHTTPContextMessages(RecognitionContext{
		Messages: []RecognitionContextMessage{{Role: "system", Text: "invalid"}},
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid context error = %v", err)
	}
	if _, err := NewChatCompletionsProvider(ChatCompletionsConfig{
		BaseURL: "https://example.com",
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing chat model error = %v", err)
	}
}

func transcribeWithProvider(
	t *testing.T,
	provider Provider,
	request TranscriptionRequest,
) TranscriptionResult {
	t.Helper()
	client, err := NewClient(provider, ClientConfig{
		RequestTimeout: 2 * time.Second,
		AudioFormat:    AudioFormatWAVPCM16,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	result, err := client.Transcribe(context.Background(), request)
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	return result
}

func assertQwenHTTPNativeRequest(t *testing.T, payload qwenHTTPNativeRequest) {
	t.Helper()
	if payload.Model != defaultQwenHTTPModel || len(payload.Input.Messages) != 4 ||
		payload.Parameters.Format != "wav" || payload.Parameters.SampleRate != "16000" ||
		len(payload.Parameters.LanguageHints) != 1 || payload.Parameters.LanguageHints[0] != "zh" ||
		payload.Parameters.VocabularyID != "vocabulary-1" ||
		payload.Parameters.Vocabulary["中央气象台"] != 5 {
		t.Fatalf("native request = %+v", payload)
	}
	if content, ok := payload.Input.Messages[0].Content.([]any); !ok || len(content) != 1 {
		// JSON decoding into an interface uses []any; typed construction is tested
		// through the remaining protocol assertions and audio decoder below.
		t.Fatalf("native prompt content = %#v", payload.Input.Messages[0].Content)
	}
	assertQwenHTTPAudioData(t, payload.Input.Messages[len(payload.Input.Messages)-1])
}

func assertQwenHTTPOpenAIRequest(t *testing.T, payload qwenHTTPOpenAIRequest) {
	t.Helper()
	if payload.Model != "qwen3-asr-flash" || payload.Stream || payload.ASROptions == nil ||
		payload.ASROptions.Language != "en" || payload.ASROptions.EnableITN == nil ||
		*payload.ASROptions.EnableITN || len(payload.Messages) != 3 ||
		payload.Messages[0].Content != "This is a weather interview." ||
		payload.Messages[1].Content != "Vocabulary: meteorological observatory" {
		t.Fatalf("OpenAI request = %+v", payload)
	}
	assertQwenHTTPAudioData(t, payload.Messages[len(payload.Messages)-1])
}

func assertQwenHTTPAudioData(t *testing.T, message qwenHTTPMessage) {
	t.Helper()
	content, ok := message.Content.([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("audio content = %#v", message.Content)
	}
	item, ok := content[0].(map[string]any)
	if !ok || item["type"] != "input_audio" {
		t.Fatalf("audio item = %#v", content[0])
	}
	inputAudio, ok := item["input_audio"].(map[string]any)
	if !ok {
		t.Fatalf("input_audio = %#v", item["input_audio"])
	}
	data, _ := inputAudio["data"].(string)
	const prefix = "data:audio/wav;base64,"
	if !strings.HasPrefix(data, prefix) {
		t.Fatalf("audio data prefix is missing")
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(data, prefix))
	if err != nil || len(decoded) < 44 || string(decoded[:4]) != "RIFF" {
		t.Fatalf("audio data is not WAV: %v", err)
	}
}

func readJSONBody(request *http.Request, target any) error {
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(target); err != nil {
		return errors.Join(ErrProviderResponse, err)
	}
	return nil
}
