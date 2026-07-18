package asr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

const (
	defaultMicrosoftHTTPName          = "microsoft"
	defaultMicrosoftHTTPModel         = "speech-recognition-conversation"
	defaultMicrosoftHTTPPath          = "/speech/recognition/conversation/cognitiveservices/v1"
	defaultMicrosoftLanguage          = "en-US"
	microsoftLocaleChineseSimplified  = "zh-CN"
	microsoftLocaleChineseTraditional = "zh-TW"
	microsoftLocaleChineseHongKong    = "zh-HK"
	defaultMicrosoftResponseBodySize  = 2 * 1024 * 1024
	microsoftHTTPAudioSampleRate      = 16_000
	microsoftHTTPMaxAudioDuration     = 60 * time.Second
	microsoftWAVHeaderSize            = 44
	microsoftAuthModeAuto             = "auto"
	microsoftAuthModeSubscriptionKey  = "subscription_key"
	microsoftAuthModeBearer           = "bearer"
	microsoftRecognitionSuccess       = "success"
	microsoftTicksPerSecond           = 10_000_000
)

// MicrosoftHTTPConfig configures the Microsoft Speech REST conversation API.
// Endpoint may be either a regional root URL or the complete recognition URL.
type MicrosoftHTTPConfig struct {
	Name              string
	Model             string
	Endpoint          string
	APIKey            string
	AuthMode          string
	DefaultLanguage   string
	ResponseBodyLimit int
	AllowInsecureHTTP bool
}

// MicrosoftHTTPProvider sends complete VAD/window WAV payloads to Microsoft
// Speech. It deliberately implements Provider rather than StreamingProvider:
// Microsoft server events and audio streaming are a separate protocol.
type MicrosoftHTTPProvider struct {
	client *resty.Client
	cfg    MicrosoftHTTPConfig
}

type microsoftSpeechResponse struct {
	RecognitionStatus string                 `json:"RecognitionStatus"`
	DisplayText       string                 `json:"DisplayText"`
	Offset            int64                  `json:"Offset"`
	Duration          int64                  `json:"Duration"`
	NBest             []microsoftSpeechNBest `json:"NBest"`
}

type microsoftSpeechNBest struct {
	Lexical    string                `json:"Lexical"`
	Display    string                `json:"Display"`
	Confidence float64               `json:"Confidence"`
	Words      []microsoftSpeechWord `json:"Words"`
}

type microsoftSpeechWord struct {
	Word     string `json:"Word"`
	Offset   int64  `json:"Offset"`
	Duration int64  `json:"Duration"`
}

func NewMicrosoftHTTPProvider(cfg MicrosoftHTTPConfig) (*MicrosoftHTTPProvider, error) {
	normalized, err := normalizeMicrosoftHTTPConfig(cfg)
	if err != nil {
		return nil, err
	}
	client := resty.New().
		SetResponseBodyLimit(normalized.ResponseBodyLimit).
		SetRedirectPolicy(resty.NoRedirectPolicy())
	return &MicrosoftHTTPProvider{client: client, cfg: normalized}, nil
}

func (p *MicrosoftHTTPProvider) Name() string {
	if p == nil {
		return ""
	}
	return p.cfg.Name
}

func (p *MicrosoftHTTPProvider) Model() string {
	if p == nil {
		return ""
	}
	return p.cfg.Model
}

func (p *MicrosoftHTTPProvider) Capabilities() Capabilities {
	if p == nil {
		return Capabilities{}
	}
	return Capabilities{Formats: []AudioFormat{AudioFormatWAVPCM16}, SupportsWordTimes: true}
}

func (p *MicrosoftHTTPProvider) Transcribe(
	ctx context.Context,
	request ProviderRequest,
) (ProviderResult, error) {
	if p == nil || p.client == nil || request.RequestID == "" || len(request.Audio.Data) < microsoftWAVHeaderSize ||
		request.Audio.Format != AudioFormatWAVPCM16 || request.Audio.SampleRate != microsoftHTTPAudioSampleRate ||
		request.Audio.Channels != 1 || microsoftAudioDuration(request.Audio) > microsoftHTTPMaxAudioDuration {
		return ProviderResult{}, ErrInvalidRequest
	}
	language, err := microsoftSpeechLocale(request.Language, p.cfg.DefaultLanguage)
	if err != nil {
		return ProviderResult{}, err
	}
	startedAt := time.Now()
	httpRequest := p.client.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetHeader("Content-Type", fmt.Sprintf("audio/wav; codecs=audio/pcm; samplerate=%d", request.Audio.SampleRate)).
		SetHeader("X-Request-ID", request.RequestID).
		SetQueryParams(map[string]string{defaultHTTPLanguageField: language, "format": "detailed"}).
		SetBody(request.Audio.Data)
	p.setAuthorization(httpRequest)

	response, err := httpRequest.Post(p.cfg.Endpoint)
	if err != nil {
		if ctx.Err() != nil {
			return ProviderResult{}, errors.Join(ErrRequestTimeout, ctx.Err(), err)
		}
		return ProviderResult{}, errors.Join(ErrProviderUnavailable, err)
	}
	if err := classifyHTTPStatus(response.StatusCode()); err != nil {
		return ProviderResult{}, err
	}
	var payload microsoftSpeechResponse
	if err := json.Unmarshal(response.Body(), &payload); err != nil {
		return ProviderResult{}, errors.Join(ErrProviderResponse, err)
	}
	if err := classifyMicrosoftRecognitionStatus(payload.RecognitionStatus); err != nil {
		return ProviderResult{}, err
	}
	text := microsoftSpeechText(payload)
	if text == "" {
		return ProviderResult{}, ErrNoSpeech
	}
	return ProviderResult{
		Text:     text,
		Words:    microsoftSpeechWords(payload),
		Provider: p.cfg.Name,
		Model:    p.cfg.Model,
		Duration: time.Since(startedAt),
	}, nil
}

func microsoftAudioDuration(audio AudioPayload) time.Duration {
	pcmBytes := len(audio.Data) - microsoftWAVHeaderSize
	if pcmBytes <= 0 || audio.SampleRate <= 0 || audio.Channels <= 0 {
		return 0
	}
	bytesPerSecond := audio.SampleRate * audio.Channels * 2
	return time.Duration(pcmBytes) * time.Second / time.Duration(bytesPerSecond)
}

func (p *MicrosoftHTTPProvider) setAuthorization(request *resty.Request) {
	switch p.cfg.AuthMode {
	case microsoftAuthModeBearer:
		request.SetHeader("Authorization", "Bearer "+stripBearerPrefix(p.cfg.APIKey))
	default:
		request.SetHeader("Ocp-Apim-Subscription-Key", p.cfg.APIKey)
	}
}

func normalizeMicrosoftHTTPConfig(cfg MicrosoftHTTPConfig) (MicrosoftHTTPConfig, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		cfg.Name = defaultMicrosoftHTTPName
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = defaultMicrosoftHTTPModel
	}
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	if cfg.APIKey == "" {
		return cfg, ErrInvalidConfig
	}
	endpoint := strings.TrimSpace(cfg.Endpoint)
	endpoint = replaceURLScheme(endpoint, "wss", "https")
	endpoint = replaceURLScheme(endpoint, "ws", "http")
	parsedURL, err := url.Parse(endpoint)
	validScheme := parsedURL.Scheme == httpSchemeSecure ||
		(parsedURL.Scheme == httpSchemeInsecure && (cfg.AllowInsecureHTTP || isLoopbackHost(parsedURL.Hostname())))
	if err != nil || parsedURL.Host == "" || !validScheme || parsedURL.User != nil ||
		parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return cfg, ErrInvalidConfig
	}
	if !strings.Contains(strings.ToLower(parsedURL.Path), "/speech/recognition/") {
		basePath := strings.TrimRight(parsedURL.Path, "/")
		if basePath == "" && strings.HasSuffix(strings.ToLower(parsedURL.Hostname()), ".cognitiveservices.azure.com") {
			basePath = "/stt"
		}
		parsedURL.Path = basePath + defaultMicrosoftHTTPPath
	}
	cfg.Endpoint = parsedURL.String()

	cfg.AuthMode = strings.ToLower(strings.TrimSpace(cfg.AuthMode))
	if cfg.AuthMode == "" || cfg.AuthMode == microsoftAuthModeAuto {
		if looksLikeBearerToken(cfg.APIKey) {
			cfg.AuthMode = microsoftAuthModeBearer
		} else {
			cfg.AuthMode = microsoftAuthModeSubscriptionKey
		}
	}
	if cfg.AuthMode != microsoftAuthModeSubscriptionKey && cfg.AuthMode != microsoftAuthModeBearer {
		return cfg, ErrInvalidConfig
	}
	if cfg.AuthMode == microsoftAuthModeBearer {
		cfg.APIKey = stripBearerPrefix(cfg.APIKey)
		if cfg.APIKey == "" {
			return cfg, ErrInvalidConfig
		}
	}
	if cfg.DefaultLanguage == "" {
		cfg.DefaultLanguage = defaultMicrosoftLanguage
	}
	defaultLanguage, err := NormalizeLanguageTag(cfg.DefaultLanguage)
	if err != nil || defaultLanguage == automaticLanguage {
		return cfg, ErrInvalidConfig
	}
	cfg.DefaultLanguage, err = microsoftSpeechLocale(defaultLanguage, defaultMicrosoftLanguage)
	if err != nil {
		return cfg, ErrInvalidConfig
	}
	if cfg.ResponseBodyLimit <= 0 {
		cfg.ResponseBodyLimit = defaultMicrosoftResponseBodySize
	}
	return cfg, nil
}

func replaceURLScheme(value, from, to string) string {
	prefix := from + "://"
	if len(value) >= len(prefix) && strings.EqualFold(value[:len(prefix)], prefix) {
		return to + "://" + value[len(prefix):]
	}
	return value
}

func looksLikeBearerToken(value string) bool {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) >= len("bearer ") && strings.EqualFold(trimmed[:len("bearer ")], "bearer ") {
		return true
	}
	return len(trimmed) > 80 && strings.Count(trimmed, ".") == 2
}

func stripBearerPrefix(value string) string {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) >= len("bearer ") && strings.EqualFold(trimmed[:len("bearer ")], "bearer ") {
		return strings.TrimSpace(trimmed[len("bearer "):])
	}
	return trimmed
}

func classifyMicrosoftRecognitionStatus(status string) error {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case microsoftRecognitionSuccess:
		return nil
	case "nomatch", "initialsilencetimeout", "babbletimeout", "endofdictation":
		return ErrNoSpeech
	default:
		return ErrProviderResponse
	}
}

func microsoftSpeechText(response microsoftSpeechResponse) string {
	if text := strings.TrimSpace(response.DisplayText); text != "" {
		return text
	}
	if len(response.NBest) == 0 {
		return ""
	}
	if text := strings.TrimSpace(response.NBest[0].Display); text != "" {
		return text
	}
	return strings.TrimSpace(response.NBest[0].Lexical)
}

func microsoftSpeechWords(response microsoftSpeechResponse) []Word {
	if len(response.NBest) == 0 {
		return nil
	}
	nbest := response.NBest[0]
	words := make([]Word, 0, len(nbest.Words))
	for _, item := range nbest.Words {
		text := strings.TrimSpace(item.Word)
		if text == "" || item.Offset < 0 || item.Duration < 0 {
			continue
		}
		start := float64(item.Offset) / microsoftTicksPerSecond
		words = append(words, Word{
			Text:       text,
			StartAt:    start,
			EndAt:      start + float64(item.Duration)/microsoftTicksPerSecond,
			Confidence: nbest.Confidence,
		})
	}
	return words
}

func microsoftSpeechLocale(value, fallback string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, automaticLanguage) {
		value = fallback
	}
	normalized, err := NormalizeLanguageTag(value)
	if err != nil || normalized == automaticLanguage {
		if err != nil {
			return "", err
		}
		return "", ErrLanguageInvalid
	}
	lower := strings.ToLower(normalized)
	switch lower {
	case "zh", "zh-hans", "zh-hans-cn":
		return microsoftLocaleChineseSimplified, nil
	case "zh-hant", "zh-hant-tw":
		return microsoftLocaleChineseTraditional, nil
	case "zh-hk", "zh-hant-hk":
		return microsoftLocaleChineseHongKong, nil
	}
	if strings.Contains(normalized, "-") {
		return normalized, nil
	}
	defaultLocales := map[string]string{
		"af": "af-ZA", "ar": "ar-SA", "bg": "bg-BG", "bn": "bn-IN",
		"cs": "cs-CZ", "da": "da-DK", "de": "de-DE", "el": "el-GR",
		"en": defaultMicrosoftLanguage, "es": "es-ES", "et": "et-EE", "fa": "fa-IR",
		"fi": "fi-FI", "fr": "fr-FR", "gu": "gu-IN", "he": "he-IL",
		"hi": "hi-IN", "hr": "hr-HR", "hu": "hu-HU", "id": "id-ID",
		"it": "it-IT", "ja": "ja-JP", "kn": "kn-IN", "ko": "ko-KR",
		"lt": "lt-LT", "lv": "lv-LV", "ml": "ml-IN", "mr": "mr-IN",
		"ms": "ms-MY", "nb": "nb-NO", "nl": "nl-NL", "no": "nb-NO",
		"pa": "pa-IN", "pl": "pl-PL", "pt": "pt-BR", "ro": "ro-RO",
		"ru": "ru-RU", "sk": "sk-SK", "sl": "sl-SI", "sr": "sr-RS",
		"sv": "sv-SE", "sw": "sw-KE", "ta": "ta-IN", "te": "te-IN",
		"th": "th-TH", "tr": "tr-TR", "uk": "uk-UA", "ur": "ur-PK",
		"vi": "vi-VN",
	}
	if locale, ok := defaultLocales[lower]; ok {
		return locale, nil
	}
	return normalized, nil
}

var _ Provider = (*MicrosoftHTTPProvider)(nil)
