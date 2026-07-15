package asr

import (
	"context"
	"errors"
	"slices"
	"time"
)

const (
	defaultRequestTimeout = 8 * time.Second
	defaultMaxConcurrency = 16
)

type ClientConfig struct {
	RequestTimeout time.Duration
	RetryCount     int
	MaxConcurrency int
	AudioFormat    AudioFormat
}

type Client struct {
	provider  Provider
	semaphore chan struct{}
	cfg       ClientConfig
}

func NewClient(provider Provider, cfg ClientConfig) (*Client, error) {
	if provider == nil || provider.Name() == "" || provider.Model() == "" {
		return nil, ErrInvalidConfig
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = defaultMaxConcurrency
	}
	if cfg.RetryCount < 0 || cfg.RetryCount > 1 {
		return nil, ErrInvalidConfig
	}
	if cfg.AudioFormat == "" {
		cfg.AudioFormat = AudioFormatWAVPCM16
	}
	if !slices.Contains(provider.Capabilities().Formats, cfg.AudioFormat) {
		return nil, ErrInvalidConfig
	}
	return &Client{
		provider:  provider,
		semaphore: make(chan struct{}, cfg.MaxConcurrency),
		cfg:       cfg,
	}, nil
}

func (c *Client) ProviderName() string {
	if c == nil || c.provider == nil {
		return ""
	}
	return c.provider.Name()
}

func (c *Client) ProviderModel() string {
	if c == nil || c.provider == nil {
		return ""
	}
	return c.provider.Model()
}

func (c *Client) Transcribe(ctx context.Context, request TranscriptionRequest) (TranscriptionResult, error) {
	if c == nil || c.provider == nil || request.RequestID == "" || request.SessionID == "" ||
		len(request.Samples) == 0 || request.SampleRate <= 0 || request.Channels != 1 {
		return TranscriptionResult{}, ErrInvalidRequest
	}
	languageTag, err := NormalizeLanguageTag(request.Language)
	if err != nil {
		return TranscriptionResult{}, err
	}
	languageHints, err := normalizeLanguageHints(request.LanguageHints)
	if err != nil {
		return TranscriptionResult{}, err
	}
	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return TranscriptionResult{}, errors.Join(ErrOverloaded, ctx.Err())
	}

	audio, err := EncodeAudio(request.Samples, request.SampleRate, request.Channels, c.cfg.AudioFormat)
	if err != nil {
		return TranscriptionResult{}, err
	}
	providerRequest := ProviderRequest{
		RequestID:     request.RequestID,
		SessionID:     request.SessionID,
		Language:      languageTag,
		LanguageHints: languageHints,
		Context:       cloneRecognitionContext(request.Context),
		Audio:         audio,
	}

	requestCtx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	var result ProviderResult
	for attempt := 0; attempt <= c.cfg.RetryCount; attempt++ {
		result, err = c.provider.Transcribe(requestCtx, providerRequest)
		if err == nil {
			return TranscriptionResult{ProviderResult: result, RequestID: request.RequestID}, nil
		}
		if errors.Is(err, ErrNoSpeech) {
			return TranscriptionResult{}, ErrNoSpeech
		}
		if !isRetryableProviderError(err) || requestCtx.Err() != nil {
			break
		}
	}
	if requestCtx.Err() != nil {
		return TranscriptionResult{}, errors.Join(ErrRequestTimeout, requestCtx.Err(), err)
	}
	return TranscriptionResult{}, errors.Join(ErrProviderRequest, err)
}

func cloneRecognitionContext(value RecognitionContext) RecognitionContext {
	cloned := RecognitionContext{
		Prompt: value.Prompt,
		Terms:  slices.Clone(value.Terms),
	}
	if len(value.ExtraFields) > 0 {
		cloned.ExtraFields = make(map[string]string, len(value.ExtraFields))
		for key, item := range value.ExtraFields {
			cloned.ExtraFields[key] = item
		}
	}
	return cloned
}

func isRetryableProviderError(err error) bool {
	return errors.Is(err, ErrProviderUnavailable) ||
		errors.Is(err, ErrRequestTimeout) ||
		errors.Is(err, ErrRateLimited)
}
