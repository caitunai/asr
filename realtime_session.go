package asr

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

const (
	defaultRealtimeEventBuffer = 128
	defaultRealtimeWaitTimeout = 20 * time.Second
	defaultRealtimeChunk       = 100 * time.Millisecond
)

type RealtimeSessionConfig struct {
	Request            StreamingRequest
	EventBuffer        int
	MinimumWaitTimeout time.Duration
	ChunkDuration      time.Duration
}

// RealtimeSession adapts a persistent provider stream to AudioSession. It
// accepts denoised float32 PCM and deliberately ignores local VAD boundaries;
// utterance detection, when enabled, belongs to the remote provider.
type RealtimeSession struct {
	provider StreamingProvider
	stream   ProviderStream
	cfg      RealtimeSessionConfig

	ctx    context.Context //nolint:containedctx // The session owns the stream lifecycle.
	cancel context.CancelFunc

	events chan Event
	done   chan struct{}

	inputMu    sync.Mutex
	resultMu   sync.Mutex
	closeOnce  sync.Once
	finishOnce sync.Once

	finished     bool
	sequence     uint64
	chunkSeq     uint64
	sampleCount  int64
	nextIndex    int
	chunkSamples int
	pending      []float32
	items        map[string]*realtimeResultState
	waitErr      error
}

type realtimeResultState struct {
	index    int
	revision int
}

func NewRealtimeSession(
	ctx context.Context,
	provider StreamingProvider,
	cfg RealtimeSessionConfig,
) (*RealtimeSession, error) {
	if ctx == nil || provider == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Request.SampleRate <= 0 || cfg.Request.Channels != 1 ||
		cfg.Request.Format != AudioFormatRawPCM16 {
		return nil, ErrInvalidConfig
	}
	if cfg.Request.SessionID == "" {
		id, err := newRandomSessionID()
		if err != nil {
			return nil, err
		}
		cfg.Request.SessionID = id
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultRealtimeEventBuffer
	}
	if cfg.MinimumWaitTimeout <= 0 {
		cfg.MinimumWaitTimeout = defaultRealtimeWaitTimeout
	}
	if cfg.ChunkDuration <= 0 {
		cfg.ChunkDuration = defaultRealtimeChunk
	}
	if cfg.ChunkDuration < 20*time.Millisecond || cfg.ChunkDuration > time.Second {
		return nil, ErrInvalidConfig
	}
	chunkSamples := int(durationSamples(cfg.ChunkDuration, int64(cfg.Request.SampleRate)))
	if chunkSamples <= 0 {
		return nil, ErrInvalidConfig
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	stream, err := provider.Start(sessionCtx, cfg.Request)
	if err != nil {
		cancel()
		return nil, errors.Join(ErrProviderRequest, err)
	}
	session := &RealtimeSession{
		provider:     provider,
		stream:       stream,
		cfg:          cfg,
		ctx:          sessionCtx,
		cancel:       cancel,
		events:       make(chan Event, cfg.EventBuffer),
		done:         make(chan struct{}),
		chunkSamples: chunkSamples,
		pending:      make([]float32, 0, chunkSamples),
		items:        make(map[string]*realtimeResultState),
	}
	go session.run(sessionCtx)
	return session, nil
}

func (s *RealtimeSession) Mode() AudioSessionMode { return AudioSessionModeRealtimeWebSocket }

func (s *RealtimeSession) Requirements() InputRequirements {
	return InputRequirements{}
}

func (s *RealtimeSession) Push(ctx context.Context, chunk AudioChunk) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.inputMu.Lock()
	defer s.inputMu.Unlock()
	if s.finished {
		return ErrSessionClosed
	}
	return s.pushSamplesLocked(ctx, chunk.Samples)
}

func (s *RealtimeSession) Finish(ctx context.Context, final FinalAudioChunk) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.inputMu.Lock()
	defer s.inputMu.Unlock()
	if s.finished {
		return ErrSessionClosed
	}
	if err := s.pushSamplesLocked(ctx, final.Samples); err != nil {
		return err
	}
	if err := s.flushPendingLocked(ctx); err != nil {
		return err
	}
	s.finished = true
	var closeErr error
	s.finishOnce.Do(func() {
		closeErr = s.stream.CloseInput(ctx)
	})
	if closeErr != nil {
		return realtimeProviderRequestError(closeErr)
	}
	return nil
}

func (s *RealtimeSession) pushSamplesLocked(ctx context.Context, samples []float32) error {
	if len(samples) == 0 {
		return nil
	}
	s.pending = append(s.pending, samples...)
	wroteChunk := false
	for len(s.pending) >= s.chunkSamples {
		if err := s.writeSamplesLocked(ctx, s.pending[:s.chunkSamples]); err != nil {
			return err
		}
		s.pending = s.pending[s.chunkSamples:]
		wroteChunk = true
	}
	if wroteChunk {
		s.pending = append(make([]float32, 0, s.chunkSamples), s.pending...)
	}
	return nil
}

func (s *RealtimeSession) flushPendingLocked(ctx context.Context) error {
	if len(s.pending) == 0 {
		return nil
	}
	if err := s.writeSamplesLocked(ctx, s.pending); err != nil {
		return err
	}
	s.pending = s.pending[:0]
	return nil
}

func (s *RealtimeSession) writeSamplesLocked(ctx context.Context, samples []float32) error {
	payload, err := EncodeAudio(samples, s.cfg.Request.SampleRate, s.cfg.Request.Channels, AudioFormatRawPCM16)
	if err != nil {
		return err
	}
	start := s.sampleCount
	end := start + int64(len(samples))
	chunk := StreamingAudioChunk{
		Data:        payload.Data,
		Sequence:    s.chunkSeq,
		StartSample: start,
		EndSample:   end,
	}
	if err := s.stream.WriteAudio(ctx, chunk); err != nil {
		return realtimeProviderRequestError(err)
	}
	s.chunkSeq++
	s.sampleCount = end
	return nil
}

func realtimeProviderRequestError(err error) error {
	if errors.Is(err, ErrProviderRequest) {
		return err
	}
	return errors.Join(ErrProviderRequest, err)
}

func (s *RealtimeSession) Events() <-chan Event {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *RealtimeSession) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *RealtimeSession) Wait(ctx context.Context) error {
	if s == nil {
		return ErrSessionClosed
	}
	select {
	case <-s.done:
		s.resultMu.Lock()
		defer s.resultMu.Unlock()
		return s.waitErr
	case <-ctx.Done():
		return errors.Join(ErrRequestTimeout, ctx.Err())
	}
}

func (s *RealtimeSession) RecommendedWaitTimeout() time.Duration {
	if s == nil || s.cfg.MinimumWaitTimeout <= 0 {
		return defaultRealtimeWaitTimeout
	}
	return s.cfg.MinimumWaitTimeout
}

func (s *RealtimeSession) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		s.stream.Close()
	})
}

func (s *RealtimeSession) run(ctx context.Context) {
	defer close(s.events)
	defer close(s.done)
	s.emit(Event{
		Type:      EventSessionReady,
		SessionID: s.cfg.Request.SessionID,
		Provider:  s.provider.Name(),
		Model:     s.provider.Model(),
	})

	emittedError := false
	for providerEvent := range s.stream.Events() {
		if providerEvent.Err != nil {
			emittedError = true
			s.emitProviderError(providerEvent)
			continue
		}
		s.emitTranscript(providerEvent)
	}
	waitErr := s.stream.Wait(context.WithoutCancel(ctx))
	s.resultMu.Lock()
	s.waitErr = waitErr
	s.resultMu.Unlock()
	if waitErr != nil && !emittedError {
		s.emitProviderError(ProviderStreamEvent{Err: waitErr, IsFinal: true})
	}
	s.emit(Event{
		Type:      EventCompleted,
		SessionID: s.cfg.Request.SessionID,
		Provider:  s.provider.Name(),
		Model:     s.provider.Model(),
	})
}

func (s *RealtimeSession) emitTranscript(providerEvent ProviderStreamEvent) {
	if providerEvent.Started && providerEvent.ResultID != "" {
		s.resultState(providerEvent.ResultID)
	}
	text := strings.TrimSpace(providerEvent.Text)
	if text == "" {
		text = strings.TrimSpace(providerEvent.ConfirmedText + providerEvent.DraftText)
	}
	if text == "" {
		return
	}
	state := s.resultState(providerEvent.ResultID)
	state.revision++
	stability := TranscriptStatePreview
	if providerEvent.IsFinal {
		stability = TranscriptStateStable
	} else if providerEvent.ConfirmedText != "" {
		stability = TranscriptStateProvisional
	}
	result := &SegmentResult{
		SegmentIndex: state.index,
		Revision:     state.revision,
		State:        stability,
		Text:         text,
	}
	if providerEvent.IsFinal {
		result.FinalizationReason = FinalizationProviderFinal
		result.EvidenceQuality = EvidenceProviderFinal
	}
	s.emit(Event{
		Type:      EventSegmentResult,
		SessionID: s.cfg.Request.SessionID,
		Provider:  s.provider.Name(),
		Model:     s.provider.Model(),
		Segment:   result,
	})
}

func (s *RealtimeSession) emitProviderError(providerEvent ProviderStreamEvent) {
	index := 0
	if providerEvent.ResultID != "" {
		index = s.resultState(providerEvent.ResultID).index
	}
	s.emit(Event{
		Type:      EventRecognitionError,
		SessionID: s.cfg.Request.SessionID,
		Provider:  s.provider.Name(),
		Model:     s.provider.Model(),
		Error: &EventError{
			Code:         classifyEventError(providerEvent.Err),
			Message:      "ASR realtime request failed",
			RequestID:    providerEvent.ResultID,
			SegmentIndex: index,
			Final:        providerEvent.IsFinal,
		},
	})
}

func (s *RealtimeSession) resultState(resultID string) *realtimeResultState {
	if resultID == "" {
		resultID = "current"
	}
	state, exists := s.items[resultID]
	if exists {
		return state
	}
	state = &realtimeResultState{index: s.nextIndex}
	s.nextIndex++
	s.items[resultID] = state
	return state
}

func (s *RealtimeSession) emit(event Event) {
	s.sequence++
	event.Sequence = s.sequence
	event.Timestamp = time.Now().UTC()
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

var _ AudioSession = (*RealtimeSession)(nil)
