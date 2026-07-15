package asr

import (
	"context"
	"time"
)

type AudioSessionMode string

const (
	AudioSessionModeSegmentedHTTP     AudioSessionMode = "segmented_http"
	AudioSessionModeRealtimeWebSocket AudioSessionMode = "realtime_websocket"
)

type SpeechBoundaryType string

const (
	SpeechBoundaryStart SpeechBoundaryType = "speech_start"
	SpeechBoundarySoft  SpeechBoundaryType = "speech_soft_boundary"
	SpeechBoundaryEnd   SpeechBoundaryType = "speech_end"
)

// SpeechBoundary uses absolute sample positions and is deliberately
// independent from any concrete VAD package.
type SpeechBoundary struct {
	Type               SpeechBoundaryType
	SourceSegmentIndex int
	StartSample        int64
	EndSample          int64
}

type AudioChunk struct {
	Samples    []float32
	Boundaries []SpeechBoundary
}

// FinalAudioChunk contains only finish-time PCM and boundary deltas. Callers
// must not pass their complete VAD history in FinalBoundaries.
type FinalAudioChunk struct {
	Samples         []float32
	Boundaries      []SpeechBoundary
	FinalBoundaries []SpeechBoundary
}

type InputRequirements struct {
	SpeechBoundariesRequired bool
	SpeechBoundariesOptional bool
}

// AudioSessionRequest contains only source/session metadata. Provider and
// scheduling policy remain owned by the factory implementation.
type AudioSessionRequest struct {
	SessionID          string
	Language           string
	LanguageHints      []string
	Context            RecognitionContext
	SampleRate         int
	Channels           int
	MaxBufferedSamples int
}

// AudioSessionFactory lets applications select a segmented HTTP or realtime
// streaming backend without coupling their input transport to either one.
type AudioSessionFactory interface {
	Mode() AudioSessionMode
	NewAudioSession(ctx context.Context, request AudioSessionRequest) (AudioSession, error)
}

// AudioSession is transport-neutral. PCM may come from a WebSocket, a file, a
// microphone, or any other source.
type AudioSession interface {
	Mode() AudioSessionMode
	Requirements() InputRequirements
	Push(ctx context.Context, chunk AudioChunk) error
	Finish(ctx context.Context, final FinalAudioChunk) error
	Events() <-chan Event
	Done() <-chan struct{}
	Wait(ctx context.Context) error
	RecommendedWaitTimeout() time.Duration
	Close()
}
