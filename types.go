package asr

import (
	"context"
	"time"
)

type AudioFormat string

const (
	AudioFormatWAVPCM16 AudioFormat = "wav_pcm_s16le"
	AudioFormatRawPCM16 AudioFormat = "raw_pcm_s16le"
)

type TranscriptState string

const (
	TranscriptStatePreview     TranscriptState = "preview"
	TranscriptStateProvisional TranscriptState = "provisional"
	TranscriptStateStable      TranscriptState = "stable"
	TranscriptStateDegraded    TranscriptState = "degraded"
	TranscriptStateDiscarded   TranscriptState = "discarded"
)

type EventType string

const (
	EventSessionReady       EventType = "asr.session_ready"
	EventIntermediateResult EventType = "asr.intermediate_result"
	EventSegmentResult      EventType = "asr.segment_result"
	EventWindowResult       EventType = "asr.window_result"
	EventRevisionBatch      EventType = "asr.revision_batch"
	EventRecognitionError   EventType = "asr.error"
	EventCompleted          EventType = "asr.session_completed"
)

type FinalizationReason string

const (
	FinalizationNextWindow     FinalizationReason = "next_window"
	FinalizationSilenceTimeout FinalizationReason = "silence_timeout"
	FinalizationLongSegment    FinalizationReason = "long_segment_boundary"
	FinalizationLongSpeech     FinalizationReason = "long_speech_commit"
	FinalizationAudioStop      FinalizationReason = "audio_stop"
	FinalizationRequestTimeout FinalizationReason = "request_timeout_degraded"
	FinalizationProviderFinal  FinalizationReason = "provider_final"
)

type EvidenceQuality string

const (
	EvidenceCrossWindowHigh EvidenceQuality = "cross_window_high"
	EvidenceProviderTime    EvidenceQuality = "provider_timestamp"
	EvidenceStandalone      EvidenceQuality = "standalone"
	EvidenceDegraded        EvidenceQuality = "degraded"
	EvidenceProviderFinal   EvidenceQuality = "provider_final"
)

type RecognitionContext struct {
	Prompt      string            `json:"prompt,omitempty"`
	Terms       []string          `json:"terms,omitempty"`
	ExtraFields map[string]string `json:"extra_fields,omitempty"`
}

type Capabilities struct {
	Formats               []AudioFormat
	SupportsPrompt        bool
	SupportsTerms         bool
	SupportsWordTimes     bool
	SupportsAutoLanguage  bool
	SupportsLanguageHints bool
}

type AudioPayload struct {
	Data       []byte
	Format     AudioFormat
	SampleRate int
	Channels   int
}

type ProviderRequest struct {
	RequestID     string
	SessionID     string
	Language      string
	LanguageHints []string
	Context       RecognitionContext
	Audio         AudioPayload
}

type Word struct {
	Text       string  `json:"text"`
	StartAt    float64 `json:"start_at"`
	EndAt      float64 `json:"end_at"`
	Confidence float64 `json:"confidence,omitempty"`
}

type ProviderResult struct {
	Text             string
	DetectedLanguage string
	Words            []Word
	Provider         string
	Model            string
	Duration         time.Duration
}

type Provider interface {
	Name() string
	Model() string
	Capabilities() Capabilities
	Transcribe(ctx context.Context, request ProviderRequest) (ProviderResult, error)
}

type TranscriptionRequest struct {
	RequestID     string
	SessionID     string
	Language      string
	LanguageHints []string
	Context       RecognitionContext
	Samples       []float32
	AudioEndAt    float64
	SampleRate    int
	Channels      int
	Authoritative bool
}

type TranscriptionResult struct {
	ProviderResult
	RequestID string
}

type Recognizer interface {
	ProviderName() string
	ProviderModel() string
	Transcribe(ctx context.Context, request TranscriptionRequest) (TranscriptionResult, error)
}

type Segment struct {
	Index          int
	StartAt        float64
	EndAt          float64
	StreamDuration float64
	Samples        []float32
}

type SegmentRef struct {
	Index   int     `json:"index"`
	StartAt float64 `json:"start_at"`
	EndAt   float64 `json:"end_at"`
}

type SegmentResult struct {
	SegmentIndex       int                `json:"segment_index"`
	SourceWindowIndex  int                `json:"source_window_index"`
	Revision           int                `json:"revision"`
	State              TranscriptState    `json:"stability"`
	Text               string             `json:"text"`
	FinalizationReason FinalizationReason `json:"finalization_reason,omitempty"`
	EvidenceQuality    EvidenceQuality    `json:"evidence_quality,omitempty"`
}

// IntermediateResult is a non-authoritative transcript from the beginning of
// the current physical speech region to a confirmed soft boundary.
type IntermediateResult struct {
	Text         string  `json:"text"`
	Provider     string  `json:"provider"`
	Model        string  `json:"model"`
	SegmentIndex int     `json:"segment_index"`
	StartAt      float64 `json:"start_at"`
	EndAt        float64 `json:"end_at"`
}

type AlignmentInfo struct {
	Strategy     string  `json:"strategy"`
	Score        float64 `json:"score"`
	Coverage     float64 `json:"coverage"`
	AnchorCount  int     `json:"anchor_count"`
	Reliable     bool    `json:"reliable"`
	RejectReason string  `json:"reject_reason,omitempty"`
}

type RevisionBatch struct {
	ID                    string          `json:"revision_batch_id"`
	EvidenceRequestIDs    []string        `json:"evidence_request_ids"`
	EvidenceWindowIndices []int           `json:"evidence_window_indices"`
	Segments              []SegmentResult `json:"segments"`
	Alignment             AlignmentInfo   `json:"alignment"`
}

type WindowResult struct {
	RequestID        string       `json:"request_id"`
	WindowIndex      int          `json:"window_index"`
	Segments         []SegmentRef `json:"segments"`
	Text             string       `json:"text"`
	DetectedLanguage string       `json:"detected_language,omitempty"`
	Provider         string       `json:"provider"`
	Model            string       `json:"model"`
	Words            []Word       `json:"words,omitempty"`
}

type EventError struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	RequestID    string `json:"request_id,omitempty"`
	SegmentIndex int    `json:"segment_index,omitempty"`
	Final        bool   `json:"final"`
}

type Event struct {
	Type          EventType           `json:"type"`
	SessionID     string              `json:"session_id"`
	Sequence      uint64              `json:"sequence"`
	Timestamp     time.Time           `json:"timestamp"`
	Provider      string              `json:"provider,omitempty"`
	Model         string              `json:"model,omitempty"`
	Intermediate  *IntermediateResult `json:"intermediate,omitempty"`
	Segment       *SegmentResult      `json:"segment,omitempty"`
	Window        *WindowResult       `json:"window,omitempty"`
	RevisionBatch *RevisionBatch      `json:"revision_batch,omitempty"`
	Error         *EventError         `json:"error,omitempty"`
}
