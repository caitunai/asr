package asr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-resty/resty/v2"
)

const (
	qwenHTTPProtocolDashScopeMultimodal = "dashscope_multimodal"
	chatCompletionsProtocol             = "chat_completions"

	defaultQwenHTTPName             = "qwen-http"
	defaultQwenHTTPModel            = "qwen-audio-3.0-asr-flash"
	defaultQwenHTTPNativePath       = "/api/v1/services/aigc/multimodal-generation/generation"
	defaultQwenHTTPResponseLimit    = 2 * 1024 * 1024
	defaultQwenHTTPVocabularyWeight = 5
	qwenHTTPMaxContextRounds        = 5
	qwenHTTPMaxContextRunes         = 400
	qwenHTTPMaxLanguageHints        = 4
	qwenHTTPMaxSuperVocabulary      = 50
)

type QwenHTTPConfig struct {
	Name                      string
	Model                     string
	BaseURL                   string
	Path                      string
	APIKey                    string
	VocabularyID              string
	VocabularyWeight          int
	ResponseBodyLimit         int
	AllowInsecureHTTP         bool
	SpeakerDiarizationEnabled *bool
	KeepDialect               *bool
}

type QwenHTTPProvider struct {
	inner *jsonAudioHTTPProvider
}

type jsonAudioHTTPConfig struct {
	Name                      string
	Model                     string
	BaseURL                   string
	Path                      string
	APIKey                    string
	VocabularyID              string
	VocabularyWeight          int
	ResponseBodyLimit         int
	AllowInsecureHTTP         bool
	SpeakerDiarizationEnabled *bool
	KeepDialect               *bool
	protocol                  string
	enableITN                 *bool
}

type jsonAudioHTTPProvider struct {
	client *resty.Client
	cfg    jsonAudioHTTPConfig
}

type qwenHTTPMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type qwenHTTPContent struct {
	Type       string              `json:"type"`
	Text       string              `json:"text,omitempty"`
	InputAudio *qwenHTTPInputAudio `json:"input_audio,omitempty"`
}

type qwenHTTPInputAudio struct {
	Data string `json:"data"`
}

type qwenHTTPNativeRequest struct {
	Model      string                   `json:"model"`
	Input      qwenHTTPNativeInput      `json:"input"`
	Parameters qwenHTTPNativeParameters `json:"parameters"`
}

type qwenHTTPNativeInput struct {
	Messages []qwenHTTPMessage `json:"messages"`
}

type qwenHTTPNativeParameters struct {
	Format                    string         `json:"format"`
	SampleRate                string         `json:"sample_rate"`
	LanguageHints             []string       `json:"language_hints,omitempty"`
	VocabularyID              string         `json:"vocabulary_id,omitempty"`
	Vocabulary                map[string]int `json:"vocabulary,omitempty"`
	SpeakerDiarizationEnabled *bool          `json:"speaker_diarization_enabled,omitempty"`
	KeepDialect               *bool          `json:"keep_dialect,omitempty"`
}

type qwenHTTPOpenAIRequest struct {
	Model      string              `json:"model"`
	Messages   []qwenHTTPMessage   `json:"messages"`
	Stream     bool                `json:"stream"`
	ASROptions *qwenHTTPASROptions `json:"asr_options,omitempty"`
}

type qwenHTTPASROptions struct {
	Language  string `json:"language,omitempty"`
	EnableITN *bool  `json:"enable_itn,omitempty"`
}

type qwenHTTPResponse struct {
	Output  qwenHTTPNativeOutput `json:"output"`
	Choices []qwenHTTPChoice     `json:"choices"`
	Usage   *RecognitionUsage    `json:"usage"`
}

type qwenHTTPNativeOutput struct {
	Text      string                     `json:"text"`
	Output    qwenHTTPNestedNativeOutput `json:"output"`
	Sentence  qwenHTTPNativeSentence     `json:"sentence"`
	Sentences []qwenHTTPNativeSentence   `json:"sentences"`
}

type qwenHTTPNestedNativeOutput struct {
	Sentence qwenHTTPNativeSentence `json:"sentence"`
}

type qwenHTTPNativeSentence struct {
	Text        string               `json:"text"`
	SpeakerID   *int                 `json:"speaker_id"`
	SentenceID  int                  `json:"sentence_id"`
	SentenceEnd bool                 `json:"sentence_end"`
	BeginTime   int64                `json:"begin_time"`
	EndTime     int64                `json:"end_time"`
	ChannelID   int                  `json:"channel_id"`
	Words       []qwenHTTPNativeWord `json:"words"`
}

type qwenHTTPNativeWord struct {
	Text        string `json:"text"`
	Punctuation string `json:"punctuation"`
	BeginTime   int64  `json:"begin_time"`
	EndTime     int64  `json:"end_time"`
	SpeakerID   *int   `json:"speaker_id"`
	Fixed       bool   `json:"fixed"`
}

type qwenHTTPChoice struct {
	Message qwenHTTPChoiceMessage `json:"message"`
}

type qwenHTTPChoiceMessage struct {
	Content json.RawMessage `json:"content"`
}

type qwenHTTPChoiceContent struct {
	Text string `json:"text"`
}

func NewQwenHTTPProvider(cfg QwenHTTPConfig) (*QwenHTTPProvider, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, ErrInvalidConfig
	}
	model := strings.TrimSpace(cfg.Model)
	if (cfg.SpeakerDiarizationEnabled != nil || cfg.KeepDialect != nil) &&
		model != "qwen-audio-3.1-asr-flash" && !strings.HasPrefix(model, "qwen-audio-3.1-asr-flash-") {
		return nil, ErrInvalidConfig
	}
	inner, err := newJSONAudioHTTPProvider(jsonAudioHTTPConfig{
		Name:                      cfg.Name,
		Model:                     cfg.Model,
		BaseURL:                   cfg.BaseURL,
		Path:                      cfg.Path,
		APIKey:                    cfg.APIKey,
		VocabularyID:              cfg.VocabularyID,
		VocabularyWeight:          cfg.VocabularyWeight,
		ResponseBodyLimit:         cfg.ResponseBodyLimit,
		AllowInsecureHTTP:         cfg.AllowInsecureHTTP,
		SpeakerDiarizationEnabled: cloneOptionalBool(cfg.SpeakerDiarizationEnabled),
		KeepDialect:               cloneOptionalBool(cfg.KeepDialect),
		protocol:                  qwenHTTPProtocolDashScopeMultimodal,
	})
	if err != nil {
		return nil, err
	}
	return &QwenHTTPProvider{inner: inner}, nil
}

func newJSONAudioHTTPProvider(cfg jsonAudioHTTPConfig) (*jsonAudioHTTPProvider, error) {
	normalized, err := normalizeJSONAudioHTTPConfig(cfg)
	if err != nil {
		return nil, err
	}
	client := resty.New().
		SetBaseURL(normalized.BaseURL).
		SetResponseBodyLimit(normalized.ResponseBodyLimit).
		SetRedirectPolicy(resty.NoRedirectPolicy())
	return &jsonAudioHTTPProvider{client: client, cfg: normalized}, nil
}

func (p *QwenHTTPProvider) Name() string {
	if p == nil || p.inner == nil {
		return ""
	}
	return p.inner.Name()
}

func (p *QwenHTTPProvider) Model() string {
	if p == nil || p.inner == nil {
		return ""
	}
	return p.inner.Model()
}

func (p *QwenHTTPProvider) Capabilities() Capabilities {
	if p == nil || p.inner == nil {
		return Capabilities{}
	}
	return p.inner.Capabilities()
}

func (p *QwenHTTPProvider) Transcribe(ctx context.Context, request ProviderRequest) (ProviderResult, error) {
	if p == nil || p.inner == nil {
		return ProviderResult{}, ErrInvalidRequest
	}
	return p.inner.Transcribe(ctx, request)
}

func (p *jsonAudioHTTPProvider) Name() string {
	if p == nil {
		return ""
	}
	return p.cfg.Name
}

func (p *jsonAudioHTTPProvider) Model() string {
	if p == nil {
		return ""
	}
	return p.cfg.Model
}

func (p *jsonAudioHTTPProvider) Capabilities() Capabilities {
	if p == nil {
		return Capabilities{}
	}
	return Capabilities{
		Formats:               []AudioFormat{AudioFormatWAVPCM16},
		SupportsPrompt:        true,
		SupportsTerms:         true,
		SupportsAutoLanguage:  true,
		SupportsLanguageHints: true,
	}
}

func (p *jsonAudioHTTPProvider) Transcribe(ctx context.Context, request ProviderRequest) (ProviderResult, error) {
	ctx, cancel := httpRequestContext(ctx)
	defer cancel()
	if p == nil || p.client == nil || ctx == nil || request.RequestID == "" ||
		len(request.Audio.Data) == 0 || request.Audio.Format != AudioFormatWAVPCM16 ||
		request.Audio.SampleRate <= 0 || request.Audio.Channels != 1 {
		return ProviderResult{}, ErrInvalidRequest
	}
	var messages []qwenHTTPMessage
	var err error
	if p.cfg.protocol == chatCompletionsProtocol {
		messages, err = chatCompletionsContextMessages(request.Context)
	} else {
		messages, err = qwenHTTPContextMessages(request.Context)
	}
	if err != nil {
		return ProviderResult{}, err
	}
	audioData := "data:audio/wav;base64," + base64.StdEncoding.EncodeToString(request.Audio.Data)
	messages = append(messages, qwenHTTPAudioMessage(audioData))

	responseBody := qwenHTTPResponse{}
	httpRequest := p.client.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetHeader("X-Request-ID", request.RequestID).
		SetResult(&responseBody)
	if p.cfg.APIKey != "" {
		httpRequest.SetHeader("Authorization", "Bearer "+p.cfg.APIKey)
	}
	switch p.cfg.protocol {
	case qwenHTTPProtocolDashScopeMultimodal:
		httpRequest.SetHeader("X-DashScope-SSE", "disable").SetBody(qwenHTTPNativeRequest{
			Model:      p.cfg.Model,
			Input:      qwenHTTPNativeInput{Messages: messages},
			Parameters: p.nativeParameters(request),
		})
	case chatCompletionsProtocol:
		httpRequest.SetBody(qwenHTTPOpenAIRequest{
			Model:      p.cfg.Model,
			Messages:   messages,
			Stream:     false,
			ASROptions: qwenHTTPOptions(request, p.cfg.enableITN),
		})
	default:
		return ProviderResult{}, ErrInvalidConfig
	}

	startedAt := time.Now()
	httpResponse, err := httpRequest.Post(p.cfg.Path)
	if err != nil {
		if ctx.Err() != nil {
			return ProviderResult{}, errors.Join(ErrRequestTimeout, ctx.Err(), err)
		}
		return ProviderResult{}, errors.Join(ErrProviderUnavailable, err)
	}
	if err := classifyHTTPStatus(httpResponse.StatusCode()); err != nil {
		return ProviderResult{}, err
	}
	text := strings.TrimSpace(qwenHTTPResponseText(responseBody))
	if text == "" {
		return ProviderResult{}, ErrNoSpeech
	}
	return ProviderResult{
		Text:      text,
		Provider:  p.cfg.Name,
		Model:     p.cfg.Model,
		Duration:  time.Since(startedAt),
		Sentences: qwenHTTPResultSentences(responseBody.Output),
		Usage:     responseBody.Usage,
	}, nil
}

func cloneOptionalBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}

func (p *jsonAudioHTTPProvider) nativeParameters(request ProviderRequest) qwenHTTPNativeParameters {
	parameters := qwenHTTPNativeParameters{
		Format:                    "wav",
		SampleRate:                strconv.Itoa(request.Audio.SampleRate),
		LanguageHints:             qwenHTTPLanguageHints(request),
		VocabularyID:              p.cfg.VocabularyID,
		Vocabulary:                qwenHTTPVocabulary(request.Context.Terms, p.cfg.VocabularyWeight),
		SpeakerDiarizationEnabled: p.cfg.SpeakerDiarizationEnabled,
		KeepDialect:               p.cfg.KeepDialect,
	}
	if p.cfg.SpeakerDiarizationEnabled != nil && *p.cfg.SpeakerDiarizationEnabled {
		parameters.LanguageHints = nil
		parameters.KeepDialect = nil
	}
	return parameters
}

func normalizeJSONAudioHTTPConfig(cfg jsonAudioHTTPConfig) (jsonAudioHTTPConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultQwenHTTPName
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = defaultQwenHTTPModel
	}
	cfg.BaseURL = strings.TrimSpace(cfg.BaseURL)
	cfg.Path = strings.TrimSpace(cfg.Path)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.VocabularyID = strings.TrimSpace(cfg.VocabularyID)
	if cfg.BaseURL == "" || strings.ContainsAny(cfg.APIKey, "\r\n") {
		return cfg, ErrInvalidConfig
	}
	parsed, err := url.Parse(cfg.BaseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != httpSchemeSecure &&
			(parsed.Scheme != httpSchemeInsecure || (!cfg.AllowInsecureHTTP && !isLoopbackHost(parsed.Hostname())))) {
		if err != nil {
			return cfg, errors.Join(ErrInvalidConfig, err)
		}
		return cfg, ErrInvalidConfig
	}
	if cfg.protocol != qwenHTTPProtocolDashScopeMultimodal && cfg.protocol != chatCompletionsProtocol {
		return cfg, ErrInvalidConfig
	}
	if cfg.Path == "" {
		if cfg.protocol == chatCompletionsProtocol {
			cfg.Path = defaultChatCompletionsPath
		} else {
			cfg.Path = defaultQwenHTTPNativePath
		}
	}
	pathURL, err := url.Parse(cfg.Path)
	if err != nil || !strings.HasPrefix(cfg.Path, "/") || pathURL.IsAbs() || pathURL.RawQuery != "" || pathURL.Fragment != "" {
		return cfg, ErrInvalidConfig
	}
	if cfg.VocabularyWeight == 0 {
		cfg.VocabularyWeight = defaultQwenHTTPVocabularyWeight
	}
	if !validQwenHTTPVocabularyWeight(cfg.VocabularyWeight) {
		return cfg, ErrInvalidConfig
	}
	if cfg.ResponseBodyLimit <= 0 {
		cfg.ResponseBodyLimit = defaultQwenHTTPResponseLimit
	}
	return cfg, nil
}

func qwenHTTPContextMessages(context RecognitionContext) ([]qwenHTTPMessage, error) {
	messages := make([]RecognitionContextMessage, 0, len(context.Messages)+1)
	if prompt := strings.TrimSpace(context.Prompt); prompt != "" {
		messages = append(messages, RecognitionContextMessage{Role: RecognitionContextRoleUser, Text: prompt})
	}
	messages = append(messages, context.Messages...)
	rounds, err := qwenHTTPContextRounds(messages)
	if err != nil {
		return nil, err
	}
	result := make([]qwenHTTPMessage, 0, len(rounds)*2)
	for _, round := range rounds {
		remaining := qwenHTTPMaxContextRunes
		for _, message := range round {
			text := truncateRunes(strings.TrimSpace(message.Text), remaining)
			remaining -= utf8.RuneCountInString(text)
			contentType := "input_text"
			if message.Role == RecognitionContextRoleAssistant {
				contentType = "text"
			}
			result = append(result, qwenHTTPMessage{
				Role: message.Role,
				Content: []qwenHTTPContent{{
					Type: contentType,
					Text: text,
				}},
			})
		}
	}
	return result, nil
}

func qwenHTTPContextRounds(messages []RecognitionContextMessage) ([][]RecognitionContextMessage, error) {
	rounds := make([][]RecognitionContextMessage, 0, qwenHTTPMaxContextRounds)
	for _, message := range messages {
		message.Role = strings.ToLower(strings.TrimSpace(message.Role))
		message.Text = strings.TrimSpace(message.Text)
		if message.Text == "" {
			continue
		}
		switch message.Role {
		case RecognitionContextRoleUser:
			rounds = append(rounds, []RecognitionContextMessage{message})
		case RecognitionContextRoleAssistant:
			if len(rounds) == 0 || len(rounds[len(rounds)-1]) != 1 {
				return nil, ErrInvalidRequest
			}
			rounds[len(rounds)-1] = append(rounds[len(rounds)-1], message)
		default:
			return nil, ErrInvalidRequest
		}
	}
	if len(rounds) > qwenHTTPMaxContextRounds {
		rounds = rounds[len(rounds)-qwenHTTPMaxContextRounds:]
	}
	return rounds, nil
}

func qwenHTTPAudioMessage(data string) qwenHTTPMessage {
	return qwenHTTPMessage{
		Role: RecognitionContextRoleUser,
		Content: []qwenHTTPContent{{
			Type: "input_audio",
			InputAudio: &qwenHTTPInputAudio{
				Data: data,
			},
		}},
	}
}

func qwenHTTPLanguageHints(request ProviderRequest) []string {
	if language := qwenHTTPPrimaryLanguage(request.Language); language != "" {
		return []string{language}
	}
	hints := make([]string, 0, min(len(request.LanguageHints), qwenHTTPMaxLanguageHints))
	seen := make(map[string]struct{}, len(request.LanguageHints))
	for _, value := range request.LanguageHints {
		language := qwenHTTPPrimaryLanguage(value)
		if language == "" {
			continue
		}
		if _, exists := seen[language]; exists {
			continue
		}
		seen[language] = struct{}{}
		hints = append(hints, language)
		if len(hints) == qwenHTTPMaxLanguageHints {
			break
		}
	}
	return hints
}

func qwenHTTPOptions(request ProviderRequest, enableITN *bool) *qwenHTTPASROptions {
	language := qwenHTTPPrimaryLanguage(request.Language)
	if language == "" && len(request.LanguageHints) > 0 {
		language = qwenHTTPPrimaryLanguage(request.LanguageHints[0])
	}
	if language == "" && enableITN == nil {
		return nil
	}
	return &qwenHTTPASROptions{Language: language, EnableITN: enableITN}
}

func qwenHTTPPrimaryLanguage(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == automaticLanguage {
		return ""
	}
	return strings.ToLower(strings.SplitN(value, "-", 2)[0])
}

func qwenHTTPVocabulary(terms []string, weight int) map[string]int {
	limit := len(terms)
	if weight == 50 {
		limit = min(limit, qwenHTTPMaxSuperVocabulary)
	}
	vocabulary := make(map[string]int, limit)
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" || !validQwenHTTPVocabularyTerm(term) {
			continue
		}
		vocabulary[term] = weight
		if len(vocabulary) == limit {
			break
		}
	}
	if len(vocabulary) == 0 {
		return nil
	}
	return vocabulary
}

func validQwenHTTPVocabularyTerm(term string) bool {
	for _, character := range term {
		if character > 127 {
			return utf8.RuneCountInString(term) <= 15
		}
	}
	return len(strings.Fields(term)) <= 7
}

func validQwenHTTPVocabularyWeight(value int) bool {
	return value >= 1 && value <= 5 || value == 50
}

func qwenHTTPResponseText(response qwenHTTPResponse) string {
	if text := strings.TrimSpace(response.Output.Text); text != "" {
		return text
	}
	if len(response.Output.Sentences) > 0 {
		var text strings.Builder
		for _, sentence := range response.Output.Sentences {
			text.WriteString(sentence.Text)
		}
		if result := strings.TrimSpace(text.String()); result != "" {
			return result
		}
	}
	if text := strings.TrimSpace(response.Output.Sentence.Text); text != "" {
		return text
	}
	if text := strings.TrimSpace(response.Output.Output.Sentence.Text); text != "" {
		return text
	}
	for _, choice := range response.Choices {
		if text := qwenHTTPChoiceText(choice.Message.Content); text != "" {
			return text
		}
	}
	return ""
}

func qwenHTTPChoiceText(content json.RawMessage) string {
	if len(content) == 0 {
		return ""
	}
	var text string
	if json.Unmarshal(content, &text) == nil {
		return strings.TrimSpace(text)
	}
	var parts []qwenHTTPChoiceContent
	if json.Unmarshal(content, &parts) != nil {
		return ""
	}
	var textBuilder strings.Builder
	for _, part := range parts {
		textBuilder.WriteString(part.Text)
	}
	return strings.TrimSpace(textBuilder.String())
}

var _ Provider = (*QwenHTTPProvider)(nil)
