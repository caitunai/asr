package asr

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultSegmentedMaxBufferDuration = 2 * time.Minute
	defaultSegmentedIdlePreRoll       = 100 * time.Millisecond
	defaultSegmentedShortMaxDuration  = 6 * time.Second
	defaultSegmentedNeighborWait      = 3 * time.Second
	defaultLongSpeechCommitAfter      = 15 * time.Second
	defaultLongSpeechCommitPrefix     = 5 * time.Second
	defaultSegmentedMaxWindow         = 65 * time.Second
	defaultSegmentedRequestTimeout    = 8 * time.Second
	defaultSegmentedMinimumWait       = 23 * time.Second
)

type SegmentedSessionConfig struct {
	Session                SessionConfig
	MaxBufferedSamples     int
	IdlePreRollSamples     int
	RequestTimeoutHint     time.Duration
	MinimumWaitTimeout     time.Duration
	LongSpeechCommitAfter  time.Duration
	LongSpeechCommitPrefix time.Duration
}

// SegmentedSession turns a continuous PCM stream plus transport-neutral speech
// boundaries into preview requests and authoritative rolling ASR windows.
// Push and Finish are serialized because audio chunks are an ordered stream.
type SegmentedSession struct {
	core      *Session
	scheduled *ScheduledRecognizer
	cfg       SegmentedSessionConfig

	ctx    context.Context //nolint:containedctx // The session owns its asynchronous request lifecycle.
	cancel context.CancelFunc

	events      chan Event
	eventInput  chan Event
	forwardDone chan struct{}
	muxDone     chan struct{}

	previewWG       sync.WaitGroup
	previewEpoch    atomic.Uint64
	previewSequence atomic.Uint64
	closeOnce       sync.Once
	inputMu         sync.Mutex

	activeSpeech        *segmentedActiveSpeech
	samples             []float32
	sampleBase          int64
	nextSegmentIndex    int
	lastCompletedSource int
	hasCompletedSource  bool
	finished            bool
}

type segmentedActiveSpeech struct {
	sourceIndex    int
	startSample    int64
	softBoundaries []int64
}

func NewSegmentedSession(
	ctx context.Context,
	recognizer Recognizer,
	cfg SegmentedSessionConfig,
) (*SegmentedSession, error) {
	if ctx == nil || recognizer == nil {
		return nil, ErrInvalidConfig
	}
	normalized, err := normalizeSegmentedSessionConfig(cfg)
	if err != nil {
		return nil, err
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	scheduled, err := NewScheduledRecognizer(sessionCtx, recognizer)
	if err != nil {
		cancel()
		return nil, err
	}
	core, err := NewSession(scheduled, nil, normalized.Session)
	if err != nil {
		cancel()
		scheduled.Close()
		return nil, err
	}
	session := &SegmentedSession{
		core:        core,
		scheduled:   scheduled,
		cfg:         normalized,
		ctx:         sessionCtx,
		cancel:      cancel,
		events:      make(chan Event, normalized.Session.EventBuffer),
		eventInput:  make(chan Event, normalized.Session.EventBuffer),
		forwardDone: make(chan struct{}),
		muxDone:     make(chan struct{}),
		samples:     make([]float32, 0, min(normalized.MaxBufferedSamples, normalized.Session.SampleRate*10)),
	}
	go session.multiplexEvents()
	go session.forwardCoreEvents()
	return session, nil
}

func (s *SegmentedSession) Mode() AudioSessionMode { return AudioSessionModeSegmentedHTTP }

func (s *SegmentedSession) Requirements() InputRequirements {
	return InputRequirements{SpeechBoundariesRequired: true}
}

func (s *SegmentedSession) Events() <-chan Event {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *SegmentedSession) Done() <-chan struct{} {
	if s == nil || s.core == nil {
		return nil
	}
	return s.core.Done()
}

func (s *SegmentedSession) Wait(ctx context.Context) error {
	if s == nil || s.core == nil {
		return ErrSessionClosed
	}
	return s.core.Wait(ctx)
}

func (s *SegmentedSession) RecommendedWaitTimeout() time.Duration {
	if s == nil || s.scheduled == nil {
		return defaultSegmentedMinimumWait
	}
	outstanding := s.scheduled.Stats().AuthoritativeOutstanding + 1
	drain := time.Duration(outstanding)*s.cfg.RequestTimeoutHint +
		s.cfg.Session.TailFinalizeSilence + time.Second
	return max(s.cfg.MinimumWaitTimeout, drain)
}

func (s *SegmentedSession) Push(ctx context.Context, chunk AudioChunk) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.inputMu.Lock()
	defer s.inputMu.Unlock()
	if s.finished {
		return ErrSessionClosed
	}
	return s.pushLocked(ctx, chunk)
}

func (s *SegmentedSession) Finish(ctx context.Context, final FinalAudioChunk) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.inputMu.Lock()
	defer s.inputMu.Unlock()
	if s.finished {
		return ErrSessionClosed
	}
	if err := s.pushLocked(ctx, AudioChunk{Samples: final.Samples, Boundaries: final.Boundaries}); err != nil {
		return err
	}
	for _, boundary := range final.FinalBoundaries {
		if err := s.handleBoundary(ctx, boundary); err != nil {
			return err
		}
	}
	s.finished = true
	s.previewEpoch.Add(1)
	return s.core.Stop(ctx)
}

func (s *SegmentedSession) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		s.previewWG.Wait()
		s.core.Close()
		s.scheduled.Close()
		<-s.forwardDone
		<-s.muxDone
		s.samples = nil
	})
}

func (s *SegmentedSession) pushLocked(ctx context.Context, chunk AudioChunk) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(ErrSessionClosed, err)
	}
	if len(chunk.Samples) > s.cfg.MaxBufferedSamples {
		return ErrPCMBufferLimit
	}
	s.samples = append(s.samples, chunk.Samples...)
	for _, boundary := range chunk.Boundaries {
		if err := s.handleBoundary(ctx, boundary); err != nil {
			return err
		}
	}
	if err := s.commitAgedSoftBoundaries(ctx); err != nil {
		return err
	}
	if s.activeSpeech == nil {
		s.compactIdleAudio()
	}
	// Check after boundary processing so a speech_end contained in the same
	// chunk can release the completed prefix before enforcing the limit.
	if len(s.samples) > s.cfg.MaxBufferedSamples {
		return ErrPCMBufferLimit
	}
	return nil
}

func (s *SegmentedSession) handleBoundary(ctx context.Context, boundary SpeechBoundary) error {
	switch boundary.Type {
	case SpeechBoundaryStart:
		return s.startSpeech(ctx, boundary)
	case SpeechBoundarySoft:
		s.recordSoftBoundary(boundary)
		return nil
	case SpeechBoundaryEnd:
		return s.finishSpeech(ctx, boundary)
	default:
		return ErrSegmentInvalid
	}
}

func (s *SegmentedSession) startSpeech(ctx context.Context, boundary SpeechBoundary) error {
	if s.isCompletedSource(boundary.SourceSegmentIndex) {
		return nil
	}
	if !s.sampleAvailable(boundary.StartSample) {
		return ErrSegmentInvalid
	}
	if err := s.core.SpeechStarted(ctx); err != nil {
		return err
	}
	s.previewEpoch.Add(1)
	s.activeSpeech = &segmentedActiveSpeech{
		sourceIndex: boundary.SourceSegmentIndex,
		startSample: boundary.StartSample,
	}
	return nil
}

func (s *SegmentedSession) finishSpeech(ctx context.Context, boundary SpeechBoundary) error {
	if s.isCompletedSource(boundary.SourceSegmentIndex) {
		return nil
	}
	if s.activeSpeech == nil || s.activeSpeech.sourceIndex != boundary.SourceSegmentIndex {
		if err := s.startSpeech(ctx, boundary); err != nil {
			return err
		}
	}
	if boundary.EndSample <= s.activeSpeech.startSample || !s.sampleAvailable(boundary.EndSample) {
		return ErrSegmentInvalid
	}
	s.previewEpoch.Add(1)
	if err := s.submitRange(ctx, s.activeSpeech.startSample, boundary.EndSample); err != nil {
		return err
	}
	s.lastCompletedSource = boundary.SourceSegmentIndex
	s.hasCompletedSource = true
	s.activeSpeech = nil
	s.compactBefore(boundary.EndSample)
	return nil
}

func (s *SegmentedSession) submitRange(ctx context.Context, startSample, endSample int64) error {
	return s.submitRangeAt(ctx, startSample, endSample, s.streamSampleCount())
}

func (s *SegmentedSession) submitRangeAt(
	ctx context.Context,
	startSample int64,
	endSample int64,
	streamSample int64,
) error {
	segment := Segment{
		Index:          s.nextSegmentIndex,
		StartAt:        float64(startSample) / float64(s.cfg.Session.SampleRate),
		EndAt:          float64(endSample) / float64(s.cfg.Session.SampleRate),
		StreamDuration: float64(streamSample) / float64(s.cfg.Session.SampleRate),
		Samples:        s.cloneRange(startSample, endSample),
	}
	if err := s.core.AddSegment(ctx, segment); err != nil {
		return err
	}
	s.nextSegmentIndex++
	return nil
}

func (s *SegmentedSession) submitIntermediate(boundary SpeechBoundary) {
	if s.activeSpeech == nil || s.activeSpeech.sourceIndex != boundary.SourceSegmentIndex ||
		boundary.EndSample <= s.activeSpeech.startSample || !s.sampleAvailable(boundary.EndSample) {
		return
	}
	epoch := s.previewEpoch.Load()
	requestSequence := s.previewSequence.Add(1)
	segmentIndex := s.nextSegmentIndex
	startSample := s.activeSpeech.startSample
	endSample := boundary.EndSample
	samples := s.cloneRange(startSample, endSample)
	s.previewWG.Add(1)
	go func() {
		defer s.previewWG.Done()
		result, err := s.scheduled.Transcribe(s.ctx, TranscriptionRequest{
			RequestID:     s.cfg.Session.SessionID + ":intermediate:" + strconv.Itoa(segmentIndex) + ":" + strconv.FormatUint(requestSequence, 10),
			SessionID:     s.cfg.Session.SessionID,
			Language:      s.cfg.Session.Language,
			LanguageHints: append([]string(nil), s.cfg.Session.LanguageHints...),
			Context:       cloneRecognitionContext(s.cfg.Session.Context),
			Samples:       samples,
			AudioEndAt:    float64(endSample) / float64(s.cfg.Session.SampleRate),
			SampleRate:    s.cfg.Session.SampleRate,
			Channels:      s.cfg.Session.Channels,
		})
		if err != nil || s.previewEpoch.Load() != epoch {
			return
		}
		s.emit(Event{
			Type:     EventIntermediateResult,
			Provider: result.Provider,
			Model:    result.Model,
			Intermediate: &IntermediateResult{
				SegmentIndex: segmentIndex,
				StartAt:      float64(startSample) / float64(s.cfg.Session.SampleRate),
				EndAt:        float64(endSample) / float64(s.cfg.Session.SampleRate),
				Text:         result.Text,
				Provider:     result.Provider,
				Model:        result.Model,
			},
		})
	}()
}

func (s *SegmentedSession) recordSoftBoundary(boundary SpeechBoundary) {
	if s.activeSpeech == nil || s.activeSpeech.sourceIndex != boundary.SourceSegmentIndex ||
		boundary.EndSample <= s.activeSpeech.startSample || !s.sampleAvailable(boundary.EndSample) {
		return
	}
	boundaries := s.activeSpeech.softBoundaries
	if len(boundaries) > 0 && boundary.EndSample <= boundaries[len(boundaries)-1] {
		return
	}
	s.activeSpeech.softBoundaries = append(s.activeSpeech.softBoundaries, boundary.EndSample)
	s.submitIntermediate(boundary)
}

func (s *SegmentedSession) commitAgedSoftBoundaries(ctx context.Context) error {
	for {
		boundary := s.nextCommitBoundary()
		if boundary <= 0 {
			return nil
		}
		s.previewEpoch.Add(1)
		if err := s.submitRangeAt(ctx, s.activeSpeech.startSample, boundary, boundary); err != nil {
			return err
		}
		if err := s.core.SpeechSplit(ctx); err != nil {
			return err
		}
		s.advanceActiveSpeech(boundary)
	}
}

func (s *SegmentedSession) nextCommitBoundary() int64 {
	if s.activeSpeech == nil || len(s.activeSpeech.softBoundaries) == 0 {
		return 0
	}
	sampleRate := int64(s.cfg.Session.SampleRate)
	commitAfter := durationSamples(s.cfg.LongSpeechCommitAfter, sampleRate)
	commitPrefix := durationSamples(s.cfg.LongSpeechCommitPrefix, sampleRate)
	streamSample := s.streamSampleCount()
	if streamSample-s.activeSpeech.startSample < commitAfter {
		return 0
	}
	latestAllowed := s.activeSpeech.startSample + commitPrefix
	boundary := int64(0)
	for _, candidate := range s.activeSpeech.softBoundaries {
		if candidate > latestAllowed {
			break
		}
		if candidate > s.activeSpeech.startSample {
			boundary = candidate
		}
	}
	return boundary
}

func (s *SegmentedSession) advanceActiveSpeech(boundary int64) {
	index := 0
	for index < len(s.activeSpeech.softBoundaries) && s.activeSpeech.softBoundaries[index] <= boundary {
		index++
	}
	s.activeSpeech.startSample = boundary
	s.activeSpeech.softBoundaries = append([]int64(nil), s.activeSpeech.softBoundaries[index:]...)
	s.compactBefore(boundary)
}

func (s *SegmentedSession) forwardCoreEvents() {
	defer close(s.forwardDone)
	for event := range s.core.Events() {
		s.emit(event)
	}
}

func (s *SegmentedSession) emit(event Event) {
	select {
	case s.eventInput <- event:
	case <-s.ctx.Done():
	}
}

func (s *SegmentedSession) multiplexEvents() {
	defer close(s.muxDone)
	defer close(s.events)
	var sequence uint64
	for {
		select {
		case event := <-s.eventInput:
			sequence++
			event.SessionID = s.cfg.Session.SessionID
			event.Sequence = sequence
			event.Timestamp = time.Now().UTC()
			select {
			case s.events <- event:
			case <-s.ctx.Done():
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *SegmentedSession) streamSampleCount() int64 {
	return s.sampleBase + int64(len(s.samples))
}

func (s *SegmentedSession) sampleAvailable(sample int64) bool {
	return sample >= s.sampleBase && sample <= s.streamSampleCount()
}

func (s *SegmentedSession) cloneRange(startSample, endSample int64) []float32 {
	start := int(startSample - s.sampleBase)
	end := int(endSample - s.sampleBase)
	return append([]float32(nil), s.samples[start:end]...)
}

func (s *SegmentedSession) compactBefore(sample int64) {
	offset := int(min(max(sample-s.sampleBase, 0), int64(len(s.samples))))
	if offset == 0 {
		return
	}
	s.samples = append([]float32(nil), s.samples[offset:]...)
	s.sampleBase += int64(offset)
}

func (s *SegmentedSession) compactIdleAudio() {
	keepFrom := s.streamSampleCount() - int64(s.cfg.IdlePreRollSamples)
	if keepFrom > s.sampleBase {
		s.compactBefore(keepFrom)
	}
}

func (s *SegmentedSession) isCompletedSource(sourceIndex int) bool {
	return s.hasCompletedSource && sourceIndex <= s.lastCompletedSource
}

func normalizeSegmentedSessionConfig(cfg SegmentedSessionConfig) (SegmentedSessionConfig, error) {
	if cfg.Session.SampleRate <= 0 || cfg.Session.Channels != 1 {
		return cfg, ErrInvalidConfig
	}
	if cfg.Session.SessionID == "" {
		id, err := newRandomSessionID()
		if err != nil {
			return cfg, err
		}
		cfg.Session.SessionID = id
	}
	if cfg.Session.ShortSegmentMaxDuration == 0 && cfg.Session.ShortSegmentNeighborWait == 0 {
		cfg.Session.ShortSegmentMaxDuration = defaultSegmentedShortMaxDuration
		cfg.Session.ShortSegmentNeighborWait = defaultSegmentedNeighborWait
	}
	if cfg.LongSpeechCommitAfter == 0 {
		cfg.LongSpeechCommitAfter = defaultLongSpeechCommitAfter
	}
	if cfg.LongSpeechCommitPrefix == 0 {
		cfg.LongSpeechCommitPrefix = defaultLongSpeechCommitPrefix
	}
	if cfg.LongSpeechCommitAfter <= 0 || cfg.LongSpeechCommitPrefix <= 0 ||
		cfg.LongSpeechCommitPrefix >= cfg.LongSpeechCommitAfter {
		return cfg, ErrInvalidConfig
	}
	if cfg.Session.MaxWindowDuration <= 0 {
		cfg.Session.MaxWindowDuration = defaultSegmentedMaxWindow
	}
	if cfg.MaxBufferedSamples <= 0 {
		cfg.MaxBufferedSamples = int(defaultSegmentedMaxBufferDuration.Seconds()) * cfg.Session.SampleRate
	}
	if cfg.IdlePreRollSamples <= 0 {
		cfg.IdlePreRollSamples = int(defaultSegmentedIdlePreRoll.Seconds() * float64(cfg.Session.SampleRate))
	}
	if cfg.IdlePreRollSamples >= cfg.MaxBufferedSamples {
		return cfg, ErrInvalidConfig
	}
	if cfg.RequestTimeoutHint <= 0 {
		cfg.RequestTimeoutHint = defaultSegmentedRequestTimeout
	}
	if cfg.MinimumWaitTimeout <= 0 {
		cfg.MinimumWaitTimeout = defaultSegmentedMinimumWait
	}
	if cfg.Session.EventBuffer <= 0 {
		cfg.Session.EventBuffer = defaultSessionEventBuffer
	}
	return cfg, nil
}

func durationSamples(duration time.Duration, sampleRate int64) int64 {
	seconds := int64(duration / time.Second)
	remainder := int64(duration % time.Second)
	return seconds*sampleRate + remainder*sampleRate/int64(time.Second)
}

func newRandomSessionID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", errors.Join(ErrInvalidConfig, err)
	}
	return hex.EncodeToString(data), nil
}

var _ AudioSession = (*SegmentedSession)(nil)
