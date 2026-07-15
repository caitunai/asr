package asr

import (
	"context"
	"strings"
	"time"
)

const (
	defaultQwenOmniRealtimeName    = "qwen-omni-realtime"
	defaultQwenOmniRealtimeModel   = "qwen3.5-omni-plus-realtime"
	defaultQwenOmniTurnDetection   = QwenOmniTurnDetectionSemantic
	defaultQwenOmniVADThreshold    = 0.5
	defaultQwenOmniVADSilence      = 800 * time.Millisecond
	defaultQwenOmniASRInstructions = "Do not answer the user. Only process the input audio transcription."
	QwenOmniTurnDetectionServer    = "server_vad"
	QwenOmniTurnDetectionSemantic  = "semantic_vad"
)

type QwenOmniRealtimeConfig struct {
	Name                   string
	Model                  string
	Endpoint               string
	APIKey                 string
	WorkspaceID            string
	TurnDetectionType      string
	Instructions           string
	VADThreshold           *float64
	VADSilenceDuration     time.Duration
	HandshakeTimeout       time.Duration
	WriteTimeout           time.Duration
	FinishTimeout          time.Duration
	EventBuffer            int
	DisableServerVAD       bool
	KeepModelResponses     bool
	AllowInsecureWebSocket bool
}

// QwenOmniRealtimeProvider uses Qwen-Omni-Realtime's input transcription
// events as an ASR stream. The Omni model's assistant responses are canceled
// by default because they are outside this provider's ASR-only contract.
type QwenOmniRealtimeProvider struct {
	core *QwenRealtimeProvider
}

func NewQwenOmniRealtimeProvider(cfg QwenOmniRealtimeConfig) (*QwenOmniRealtimeProvider, error) {
	normalized, threshold, err := normalizeQwenOmniRealtimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	core, err := NewQwenRealtimeProvider(QwenRealtimeConfig{
		Name:                     normalized.Name,
		Model:                    normalized.Model,
		Endpoint:                 normalized.Endpoint,
		APIKey:                   normalized.APIKey,
		WorkspaceID:              normalized.WorkspaceID,
		ServerVADThreshold:       threshold,
		ServerVADSilenceDuration: normalized.VADSilenceDuration,
		DisableServerVAD:         normalized.DisableServerVAD,
		HandshakeTimeout:         normalized.HandshakeTimeout,
		WriteTimeout:             normalized.WriteTimeout,
		FinishTimeout:            normalized.FinishTimeout,
		EventBuffer:              normalized.EventBuffer,
		AllowInsecureWebSocket:   normalized.AllowInsecureWebSocket,
	})
	if err != nil {
		return nil, err
	}
	core.protocol = qwenRealtimeProtocolOmni
	core.omni = qwenOmniSettings{
		turnDetectionType: normalized.TurnDetectionType,
		instructions:      normalized.Instructions,
		keepModelResponse: normalized.KeepModelResponses,
	}
	return &QwenOmniRealtimeProvider{core: core}, nil
}

func (p *QwenOmniRealtimeProvider) Name() string {
	if p == nil || p.core == nil {
		return ""
	}
	return p.core.Name()
}

func (p *QwenOmniRealtimeProvider) Model() string {
	if p == nil || p.core == nil {
		return ""
	}
	return p.core.Model()
}

func (p *QwenOmniRealtimeProvider) ServerVADEnabled() bool {
	return p != nil && p.core != nil && p.core.ServerVADEnabled()
}

func (p *QwenOmniRealtimeProvider) StreamingCapabilities() StreamingCapabilities {
	return StreamingCapabilities{
		Formats:           []AudioFormat{AudioFormatRawPCM16},
		SampleRates:       []int{16000},
		SupportsServerVAD: true,
	}
}

func (p *QwenOmniRealtimeProvider) Start(
	ctx context.Context,
	request StreamingRequest,
) (ProviderStream, error) {
	if p == nil || p.core == nil {
		return nil, ErrInvalidConfig
	}
	if request.SampleRate != 16000 {
		return nil, ErrInvalidRequest
	}
	return p.core.Start(ctx, request)
}

func normalizeQwenOmniRealtimeConfig(
	cfg QwenOmniRealtimeConfig,
) (QwenOmniRealtimeConfig, float64, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultQwenOmniRealtimeName
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = defaultQwenOmniRealtimeModel
	}
	cfg.TurnDetectionType = strings.TrimSpace(cfg.TurnDetectionType)
	if cfg.TurnDetectionType == "" {
		cfg.TurnDetectionType = defaultQwenOmniTurnDetection
	}
	if cfg.TurnDetectionType != QwenOmniTurnDetectionServer &&
		cfg.TurnDetectionType != QwenOmniTurnDetectionSemantic {
		return cfg, 0, ErrInvalidConfig
	}
	cfg.Instructions = strings.TrimSpace(cfg.Instructions)
	if cfg.Instructions == "" {
		cfg.Instructions = defaultQwenOmniASRInstructions
	}
	threshold := defaultQwenOmniVADThreshold
	if cfg.VADThreshold != nil {
		threshold = *cfg.VADThreshold
	}
	if threshold < -1 || threshold > 1 {
		return cfg, 0, ErrInvalidConfig
	}
	if cfg.VADSilenceDuration == 0 {
		cfg.VADSilenceDuration = defaultQwenOmniVADSilence
	}
	if cfg.VADSilenceDuration < 200*time.Millisecond || cfg.VADSilenceDuration > 6*time.Second {
		return cfg, 0, ErrInvalidConfig
	}
	return cfg, threshold, nil
}

var _ StreamingProvider = (*QwenOmniRealtimeProvider)(nil)
