package asr

import (
	"context"
	"time"
)

type StreamingCapabilities struct {
	Formats               []AudioFormat
	SampleRates           []int
	SupportsPrompt        bool
	SupportsTerms         bool
	SupportsLanguageHints bool
	SupportsServerVAD     bool
	SupportsResume        bool
}

type StreamingRequest struct {
	SessionID     string
	Language      string
	LanguageHints []string
	Context       RecognitionContext
	SampleRate    int
	Channels      int
	Format        AudioFormat
	ServerVAD     bool
}

type StreamingAudioChunk struct {
	Data        []byte
	Sequence    uint64
	StartSample int64
	EndSample   int64
}

type ProviderStreamEvent struct {
	Err      error
	ResultID string
	Text     string
	StartAt  time.Duration
	EndAt    time.Duration
	IsFinal  bool
}

// StreamingProvider creates one persistent provider-side ASR stream. It does
// not expose a concrete WebSocket implementation to the SDK consumer.
type StreamingProvider interface {
	Name() string
	Model() string
	StreamingCapabilities() StreamingCapabilities
	Start(ctx context.Context, request StreamingRequest) (ProviderStream, error)
}

// ProviderStream adapters own their provider connection, writer, reader,
// handshake, keepalive, and provider-specific message protocol.
type ProviderStream interface {
	WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error
	CloseInput(ctx context.Context) error
	Events() <-chan ProviderStreamEvent
	Done() <-chan struct{}
	Wait(ctx context.Context) error
	Close()
}
