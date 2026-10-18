package asr

import (
	"context"
	"strings"
)

const (
	defaultChatCompletionsName = "chat-completions"
	defaultChatCompletionsPath = "/v1/chat/completions"
)

// ChatCompletionsConfig configures a provider-neutral OpenAI-compatible
// /chat/completions endpoint that accepts input_audio message content.
type ChatCompletionsConfig struct {
	Name              string
	Model             string
	BaseURL           string
	Path              string
	APIKey            string
	EnableITN         *bool
	ResponseBodyLimit int
	AllowInsecureHTTP bool
}

type ChatCompletionsProvider struct {
	inner *jsonAudioHTTPProvider
}

func NewChatCompletionsProvider(cfg ChatCompletionsConfig) (*ChatCompletionsProvider, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultChatCompletionsName
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		return nil, ErrInvalidConfig
	}
	cfg.Path = strings.TrimSpace(cfg.Path)
	if cfg.Path == "" {
		cfg.Path = defaultChatCompletionsPath
	}
	inner, err := newJSONAudioHTTPProvider(jsonAudioHTTPConfig{
		Name:              cfg.Name,
		Model:             cfg.Model,
		BaseURL:           cfg.BaseURL,
		Path:              cfg.Path,
		APIKey:            cfg.APIKey,
		ResponseBodyLimit: cfg.ResponseBodyLimit,
		AllowInsecureHTTP: cfg.AllowInsecureHTTP,
		protocol:          chatCompletionsProtocol,
		enableITN:         cfg.EnableITN,
	})
	if err != nil {
		return nil, err
	}
	return &ChatCompletionsProvider{inner: inner}, nil
}

func chatCompletionsContextMessages(context RecognitionContext) ([]qwenHTTPMessage, error) {
	messages := make([]RecognitionContextMessage, 0, len(context.Messages)+2)
	if prompt := strings.TrimSpace(context.Prompt); prompt != "" {
		messages = append(messages, RecognitionContextMessage{
			Role: RecognitionContextRoleUser,
			Text: prompt,
		})
	}
	messages = append(messages, context.Messages...)
	if len(context.Terms) > 0 {
		messages = append(messages, RecognitionContextMessage{
			Role: RecognitionContextRoleUser,
			Text: "Vocabulary: " + strings.Join(context.Terms, ", "),
		})
	}
	result := make([]qwenHTTPMessage, 0, len(messages))
	for _, message := range messages {
		role := strings.ToLower(strings.TrimSpace(message.Role))
		content := strings.TrimSpace(message.Text)
		if content == "" {
			continue
		}
		if role != RecognitionContextRoleUser && role != RecognitionContextRoleAssistant {
			return nil, ErrInvalidRequest
		}
		result = append(result, qwenHTTPMessage{Role: role, Content: content})
	}
	return result, nil
}

func (p *ChatCompletionsProvider) Name() string {
	if p == nil || p.inner == nil {
		return ""
	}
	return p.inner.Name()
}

func (p *ChatCompletionsProvider) Model() string {
	if p == nil || p.inner == nil {
		return ""
	}
	return p.inner.Model()
}

func (p *ChatCompletionsProvider) Capabilities() Capabilities {
	if p == nil || p.inner == nil {
		return Capabilities{}
	}
	return p.inner.Capabilities()
}

func (p *ChatCompletionsProvider) Transcribe(ctx context.Context, request ProviderRequest) (ProviderResult, error) {
	if p == nil || p.inner == nil {
		return ProviderResult{}, ErrInvalidRequest
	}
	return p.inner.Transcribe(ctx, request)
}

var _ Provider = (*ChatCompletionsProvider)(nil)
