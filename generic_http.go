package asr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/go-resty/resty/v2"
)

const (
	defaultHTTPFileField          = "file"
	defaultHTTPModelField         = "model"
	defaultHTTPLanguageField      = "language"
	defaultHTTPPromptField        = "prompt"
	defaultHTTPTermsField         = "hotwords"
	defaultHTTPLanguageHintsField = "language_hints"
	defaultHTTPResponseBodyLimit  = 2 * 1024 * 1024
)

type GenericHTTPConfig struct {
	Name                      string
	Model                     string
	BaseURL                   string
	Path                      string
	APIKey                    string
	AuthHeader                string
	AuthScheme                string
	FileField                 string
	ModelField                string
	LanguageField             string
	PromptField               string
	TermsField                string
	LanguageHintsField        string
	ExtraFields               map[string]string
	RequireAPIKey             bool
	OmitModel                 bool
	OmitLanguage              bool
	OmitPrompt                bool
	OmitTerms                 bool
	OmitLanguageHints         bool
	ResponseBodyLimit         int
	AllowInsecureHTTP         bool
	SupportsWordTimes         bool
	SupportsAutoLanguage      bool
	SupportsLanguageHints     bool
	StripLeadingLanguageLabel bool
	AudioFormat               AudioFormat
}

type GenericHTTPProvider struct {
	client *resty.Client
	cfg    GenericHTTPConfig
}

type genericHTTPResponse struct {
	Text     string                    `json:"text"`
	Language string                    `json:"language"`
	Words    []genericHTTPResponseWord `json:"words"`
	Segments []genericHTTPResponseWord `json:"segments"`
}

type genericHTTPResponseWord struct {
	Word       string  `json:"word"`
	Text       string  `json:"text"`
	Start      float64 `json:"start"`
	End        float64 `json:"end"`
	Confidence float64 `json:"confidence"`
}

func NewGenericHTTPProvider(cfg GenericHTTPConfig) (*GenericHTTPProvider, error) {
	normalized, err := normalizeGenericHTTPConfig(cfg)
	if err != nil {
		return nil, err
	}
	client := resty.New().
		SetBaseURL(normalized.BaseURL).
		SetResponseBodyLimit(normalized.ResponseBodyLimit).
		SetRedirectPolicy(resty.NoRedirectPolicy())
	return &GenericHTTPProvider{client: client, cfg: normalized}, nil
}

func (p *GenericHTTPProvider) Name() string {
	if p == nil {
		return ""
	}
	return p.cfg.Name
}

func (p *GenericHTTPProvider) Model() string {
	if p == nil {
		return ""
	}
	return p.cfg.Model
}

func (p *GenericHTTPProvider) Capabilities() Capabilities {
	if p == nil {
		return Capabilities{}
	}
	return Capabilities{
		Formats:               []AudioFormat{p.cfg.AudioFormat},
		SupportsPrompt:        p.cfg.PromptField != "",
		SupportsTerms:         p.cfg.TermsField != "",
		SupportsWordTimes:     p.cfg.SupportsWordTimes,
		SupportsAutoLanguage:  p.cfg.SupportsAutoLanguage,
		SupportsLanguageHints: p.cfg.SupportsLanguageHints,
	}
}

func (p *GenericHTTPProvider) Transcribe(ctx context.Context, request ProviderRequest) (ProviderResult, error) {
	if p == nil || p.client == nil || len(request.Audio.Data) == 0 || request.RequestID == "" {
		return ProviderResult{}, ErrInvalidRequest
	}
	startedAt := time.Now()
	responseBody := genericHTTPResponse{}
	httpRequest := p.client.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetHeader("X-Request-ID", request.RequestID).
		SetFileReader(p.cfg.FileField, audioFileName(request.Audio.Format), bytes.NewReader(request.Audio.Data)).
		SetResult(&responseBody)
	if p.cfg.APIKey != "" {
		authValue := strings.TrimSpace(strings.TrimSpace(p.cfg.AuthScheme) + " " + p.cfg.APIKey)
		httpRequest.SetHeader(p.cfg.AuthHeader, authValue)
	}
	formData, err := p.requestFormData(request)
	if err != nil {
		return ProviderResult{}, err
	}
	if len(formData) > 0 {
		httpRequest.SetFormData(formData)
	}
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
	text := strings.TrimSpace(responseBody.Text)
	if p.cfg.StripLeadingLanguageLabel {
		text = stripLeadingLanguageLabel(text)
	}
	if text == "" {
		return ProviderResult{}, ErrNoSpeech
	}
	return ProviderResult{
		Text:             text,
		DetectedLanguage: responseBody.Language,
		Words:            convertGenericHTTPWords(responseBody),
		Provider:         p.cfg.Name,
		Model:            p.cfg.Model,
		Duration:         time.Since(startedAt),
	}, nil
}

func stripLeadingLanguageLabel(text string) string {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "[") {
		return text
	}
	closing := strings.IndexByte(text, ']')
	if closing <= 1 || closing > 65 {
		return text
	}
	for _, character := range text[1:closing] {
		if !unicode.IsLetter(character) && !unicode.IsDigit(character) &&
			character != ' ' && character != '-' && character != '_' {
			return text
		}
	}
	return strings.TrimSpace(text[closing+1:])
}

func (p *GenericHTTPProvider) requestFormData(request ProviderRequest) (map[string]string, error) {
	formData := make(map[string]string, len(p.cfg.ExtraFields)+6)
	for key, value := range p.cfg.ExtraFields {
		formData[key] = value
	}
	if p.cfg.ModelField != "" {
		formData[p.cfg.ModelField] = p.cfg.Model
	}
	if request.Language != "" && request.Language != automaticLanguage && p.cfg.LanguageField != "" {
		formData[p.cfg.LanguageField] = request.Language
	}
	prompt := strings.TrimSpace(request.Context.Prompt)
	if prompt != "" && p.cfg.PromptField != "" {
		formData[p.cfg.PromptField] = prompt
	}
	if len(request.Context.Terms) > 0 && p.cfg.TermsField != "" {
		encoded, err := json.Marshal(request.Context.Terms)
		if err != nil {
			return nil, errors.Join(ErrInvalidRequest, err)
		}
		formData[p.cfg.TermsField] = string(encoded)
	}
	if len(request.LanguageHints) > 0 && p.cfg.SupportsLanguageHints && p.cfg.LanguageHintsField != "" {
		encoded, err := json.Marshal(request.LanguageHints)
		if err != nil {
			return nil, errors.Join(ErrInvalidRequest, err)
		}
		formData[p.cfg.LanguageHintsField] = string(encoded)
	}
	for key, value := range request.Context.ExtraFields {
		if _, exists := formData[key]; !exists {
			formData[key] = value
		}
	}
	return formData, nil
}

func normalizeGenericHTTPConfig(cfg GenericHTTPConfig) (GenericHTTPConfig, error) {
	parsedURL, err := url.Parse(cfg.BaseURL)
	validScheme := parsedURL.Scheme == httpSchemeSecure ||
		(parsedURL.Scheme == httpSchemeInsecure && (cfg.AllowInsecureHTTP || isLoopbackHost(parsedURL.Hostname())))
	if err != nil || parsedURL.Host == "" || !validScheme || parsedURL.User != nil {
		return cfg, ErrInvalidConfig
	}
	if cfg.Name == "" || cfg.Model == "" || cfg.Path == "" {
		return cfg, ErrInvalidConfig
	}
	if cfg.RequireAPIKey && strings.TrimSpace(cfg.APIKey) == "" {
		return cfg, ErrInvalidConfig
	}
	if cfg.AuthHeader == "" {
		cfg.AuthHeader = "Authorization"
	}
	if cfg.AuthScheme == "" {
		cfg.AuthScheme = "Bearer"
	}
	if cfg.FileField == "" {
		cfg.FileField = defaultHTTPFileField
	}
	if cfg.OmitModel {
		cfg.ModelField = ""
	} else if cfg.ModelField == "" {
		cfg.ModelField = defaultHTTPModelField
	}
	if cfg.OmitLanguage {
		cfg.LanguageField = ""
	} else if cfg.LanguageField == "" {
		cfg.LanguageField = defaultHTTPLanguageField
	}
	if cfg.OmitPrompt {
		cfg.PromptField = ""
	} else if cfg.PromptField == "" {
		cfg.PromptField = defaultHTTPPromptField
	}
	if cfg.OmitTerms {
		cfg.TermsField = ""
	} else if cfg.TermsField == "" {
		cfg.TermsField = defaultHTTPTermsField
	}
	if cfg.OmitLanguageHints {
		cfg.LanguageHintsField = ""
	} else if cfg.LanguageHintsField == "" {
		cfg.LanguageHintsField = defaultHTTPLanguageHintsField
	}
	if cfg.ResponseBodyLimit <= 0 {
		cfg.ResponseBodyLimit = defaultHTTPResponseBodyLimit
	}
	if cfg.AudioFormat == "" {
		cfg.AudioFormat = AudioFormatWAVPCM16
	}
	if cfg.AudioFormat != AudioFormatWAVPCM16 && cfg.AudioFormat != AudioFormatRawPCM16 {
		return cfg, ErrInvalidConfig
	}
	return cfg, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(strings.TrimSpace(host), "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func classifyHTTPStatus(status int) error {
	switch {
	case status >= http.StatusOK && status < http.StatusMultipleChoices:
		return nil
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return ErrUnauthorized
	case status == http.StatusRequestTimeout:
		return ErrRequestTimeout
	case status == http.StatusTooManyRequests:
		return ErrRateLimited
	case status >= http.StatusInternalServerError:
		return ErrProviderUnavailable
	default:
		return ErrProviderRequest
	}
}

func convertGenericHTTPWords(response genericHTTPResponse) []Word {
	items := response.Words
	if len(items) == 0 {
		items = response.Segments
	}
	words := make([]Word, 0, len(items))
	for _, item := range items {
		text := item.Word
		if text == "" {
			text = item.Text
		}
		if text == "" || item.End < item.Start {
			continue
		}
		words = append(words, Word{
			Text:       text,
			StartAt:    item.Start,
			EndAt:      item.End,
			Confidence: item.Confidence,
		})
	}
	return words
}

func audioFileName(format AudioFormat) string {
	if format == AudioFormatRawPCM16 {
		return "audio.pcm"
	}
	return "audio.wav"
}
