package asr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

const (
	defaultElevenLabsRealtimeName            = "elevenlabs-realtime"
	defaultElevenLabsRealtimeEndpoint        = "wss://api.elevenlabs.io/v1/speech-to-text/realtime"
	defaultElevenLabsRealtimeModel           = "scribe_v2_realtime"
	defaultElevenLabsCommitStrategy          = ElevenLabsCommitStrategyVAD
	defaultElevenLabsVADSilence              = 300 * time.Millisecond
	defaultElevenLabsVADThreshold            = 0.4
	defaultElevenLabsMinSpeech               = 100 * time.Millisecond
	defaultElevenLabsMinSilence              = 100 * time.Millisecond
	defaultElevenLabsManualCommit            = 20 * time.Second
	defaultElevenLabsFinalPadding            = 100 * time.Millisecond
	defaultElevenLabsHandshakeTimeout        = 10 * time.Second
	defaultElevenLabsWriteTimeout            = 5 * time.Second
	defaultElevenLabsFinishTimeout           = 20 * time.Second
	defaultElevenLabsEventBuffer             = 128
	defaultElevenLabsMinTranscriptLogProb    = -5.0
	elevenLabsAPIKeyHeader                   = "xi-api-key"
	elevenLabsMessageSessionStarted          = "session_started"
	elevenLabsMessagePartial                 = "partial_transcript"
	elevenLabsMessageCommitted               = "committed_transcript"
	elevenLabsMessageCommittedWithTimestamps = "committed_transcript_with_timestamps"
	elevenLabsMessageInputAudio              = "input_audio_chunk"
	elevenLabsErrorAuth                      = "auth_error"
	elevenLabsErrorQuota                     = "quota_exceeded"
	elevenLabsErrorCommitThrottled           = "commit_throttled"
	elevenLabsErrorUnacceptedTerms           = "unaccepted_terms"
	elevenLabsErrorRateLimited               = "rate_limited"
	elevenLabsErrorQueueOverflow             = "queue_overflow"
	elevenLabsErrorResourceExhausted         = "resource_exhausted"
	elevenLabsErrorSessionTimeLimit          = "session_time_limit_exceeded"
	elevenLabsErrorInput                     = "input_error"
	elevenLabsErrorChunkSize                 = "chunk_size_exceeded"
	elevenLabsErrorInsufficientAudio         = "insufficient_audio_activity"
	elevenLabsErrorTranscriber               = "transcriber_error"
	elevenLabsErrorGeneric                   = "error"
	elevenLabsMaxKeyterms                    = 50
	elevenLabsMaxKeytermRunes                = 20
	ElevenLabsCommitStrategyManual           = "manual"
	ElevenLabsCommitStrategyVAD              = "vad"
)

type ElevenLabsRealtimeConfig struct {
	Name                     string
	Endpoint                 string
	Model                    string
	APIKey                   string
	CommitStrategy           string
	VADSilenceThreshold      time.Duration
	VADThreshold             *float64
	MinSpeechDuration        time.Duration
	MinSilenceDuration       time.Duration
	ManualCommitInterval     time.Duration
	DisableTimestamps        bool
	DisableLanguageDetection bool
	NoVerbatim               bool
	FilterBackgroundAudio    bool
	DisableLogging           bool
	EmitPartials             bool
	MinTranscriptLogProb     *float64
	HandshakeTimeout         time.Duration
	WriteTimeout             time.Duration
	FinishTimeout            time.Duration
	EventBuffer              int
	AllowInsecureWebSocket   bool
}

type ElevenLabsRealtimeProvider struct {
	cfg    ElevenLabsRealtimeConfig
	dialer websocket.Dialer
}

type elevenLabsRealtimeStream struct {
	provider *ElevenLabsRealtimeProvider
	request  StreamingRequest
	conn     *websocket.Conn

	ctx    context.Context //nolint:containedctx // The stream owns the provider connection lifecycle.
	cancel context.CancelFunc

	events  chan ProviderStreamEvent
	done    chan struct{}
	updated chan error

	writeMu      sync.Mutex
	stateMu      sync.Mutex
	resultMu     sync.Mutex
	closeOnce    sync.Once
	completeOnce sync.Once
	updatedOnce  sync.Once
	finishTimer  sync.Once
	sessionReady atomic.Bool

	writeClosed        bool
	wroteAudio         bool
	expectedSeq        uint64
	nextSample         int64
	samplesSinceCommit int64
	commitsPending     int
	closing            bool
	turnNumber         uint64
	turnID             string
	waitErr            error
	waitResultSet      bool
}

type elevenLabsServerEvent struct {
	MessageType  string                     `json:"message_type"`
	SessionID    string                     `json:"session_id"`
	Text         string                     `json:"text"`
	LanguageCode string                     `json:"language_code"`
	Words        []elevenLabsTranscriptWord `json:"words"`
	Error        string                     `json:"error"`
}

type elevenLabsTranscriptWord struct {
	Text      string  `json:"text"`
	Start     float64 `json:"start"`
	End       float64 `json:"end"`
	Type      string  `json:"type"`
	SpeakerID string  `json:"speaker_id"`
	LogProb   float64 `json:"logprob"`
}

type elevenLabsProviderError struct {
	messageType string
	detail      string
}

func (e elevenLabsProviderError) Error() string {
	if e.messageType == "" {
		return "elevenlabs realtime provider error"
	}
	if detail := elevenLabsErrorDetail(e.detail); detail != "" {
		return "elevenlabs realtime provider error: " + e.messageType + ": " + detail
	}
	return "elevenlabs realtime provider error: " + e.messageType
}

func elevenLabsErrorDetail(detail string) string {
	detail = strings.Join(strings.Fields(detail), " ")
	runes := []rune(detail)
	if len(runes) > 512 {
		detail = string(runes[:512])
	}
	return detail
}

func NewElevenLabsRealtimeProvider(cfg ElevenLabsRealtimeConfig) (*ElevenLabsRealtimeProvider, error) {
	normalized, err := normalizeElevenLabsRealtimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &ElevenLabsRealtimeProvider{cfg: normalized, dialer: *websocket.DefaultDialer}, nil
}

func (p *ElevenLabsRealtimeProvider) Name() string { return p.cfg.Name }

func (p *ElevenLabsRealtimeProvider) Model() string { return p.cfg.Model }

func (p *ElevenLabsRealtimeProvider) ServerVADEnabled() bool {
	return p != nil && p.cfg.CommitStrategy == ElevenLabsCommitStrategyVAD
}

func (p *ElevenLabsRealtimeProvider) StreamingCapabilities() StreamingCapabilities {
	return StreamingCapabilities{
		Formats:           []AudioFormat{AudioFormatRawPCM16},
		SampleRates:       []int{8000, 16000, 22050, 24000, 44100, 48000},
		SupportsPrompt:    false,
		SupportsTerms:     true,
		SupportsServerVAD: true,
	}
}

func (p *ElevenLabsRealtimeProvider) Start(
	ctx context.Context,
	request StreamingRequest,
) (ProviderStream, error) {
	if p == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	normalized, err := normalizeElevenLabsStreamingRequest(request)
	if err != nil {
		return nil, err
	}
	normalized.ServerVAD = p.ServerVADEnabled()
	endpoint, err := elevenLabsRealtimeEndpoint(p.cfg, normalized)
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set(elevenLabsAPIKeyHeader, p.cfg.APIKey)
	conn, response, err := p.dialer.DialContext(ctx, endpoint, headers)
	if response != nil && response.Body != nil {
		_, _ = io.CopyN(io.Discard, response.Body, 4<<10)
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, classifyElevenLabsDialError(response, err)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream := &elevenLabsRealtimeStream{
		provider: p,
		request:  normalized,
		conn:     conn,
		ctx:      streamCtx,
		cancel:   cancel,
		events:   make(chan ProviderStreamEvent, p.cfg.EventBuffer),
		done:     make(chan struct{}),
		updated:  make(chan error, 1),
	}
	go stream.readLoop()
	go stream.closeOnContext()
	timer := time.NewTimer(p.cfg.HandshakeTimeout)
	defer timer.Stop()
	select {
	case updateErr := <-stream.updated:
		if updateErr != nil {
			stream.Close()
			return nil, updateErr
		}
		return stream, nil
	case <-timer.C:
		stream.fail(errors.Join(ErrRequestTimeout, ErrProviderUnavailable))
		return nil, errors.Join(ErrRequestTimeout, ErrProviderUnavailable)
	case <-ctx.Done():
		stream.Close()
		return nil, errors.Join(ErrProviderUnavailable, ctx.Err())
	}
}

func (s *elevenLabsRealtimeStream) WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error {
	if s == nil || len(chunk.Data) == 0 || len(chunk.Data)%2 != 0 ||
		chunk.Sequence != s.expectedSeq || chunk.StartSample != s.nextSample ||
		chunk.EndSample <= chunk.StartSample ||
		chunk.EndSample-chunk.StartSample != int64(len(chunk.Data)/2) {
		return ErrInvalidRequest
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeClosed {
		return ErrSessionClosed
	}
	chunkSamples := int64(len(chunk.Data) / 2)
	commit := false
	if !s.provider.ServerVADEnabled() {
		commitSamples := durationSamples(s.provider.cfg.ManualCommitInterval, int64(s.request.SampleRate))
		commit = s.samplesSinceCommit+chunkSamples >= commitSamples
	}
	if err := s.writeAudioChunkLocked(ctx, chunk.Data, commit); err != nil {
		s.fail(err)
		return err
	}
	s.wroteAudio = true
	if commit {
		s.samplesSinceCommit = 0
		s.markCommitSent()
	} else {
		s.samplesSinceCommit += chunkSamples
	}
	s.expectedSeq++
	s.nextSample = chunk.EndSample
	return nil
}

func (s *elevenLabsRealtimeStream) CloseInput(ctx context.Context) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeClosed {
		return ErrSessionClosed
	}
	s.writeClosed = true
	s.stateMu.Lock()
	s.closing = true
	s.stateMu.Unlock()
	if !s.wroteAudio {
		s.complete()
		return nil
	}
	paddingSamples := durationSamples(defaultElevenLabsFinalPadding, int64(s.request.SampleRate))
	padding := make([]byte, int(paddingSamples)*2)
	if err := s.writeAudioChunkLocked(ctx, padding, true); err != nil {
		s.fail(err)
		return err
	}
	s.markCommitSent()
	s.startFinishTimer()
	return nil
}

func (s *elevenLabsRealtimeStream) Events() <-chan ProviderStreamEvent {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *elevenLabsRealtimeStream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *elevenLabsRealtimeStream) Wait(ctx context.Context) error {
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

func (s *elevenLabsRealtimeStream) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.conn.Close()
	})
}

func (s *elevenLabsRealtimeStream) writeAudioChunkLocked(
	ctx context.Context,
	data []byte,
	commit bool,
) error {
	event := map[string]any{
		realtimeFieldMessageType: elevenLabsMessageInputAudio,
		"audio_base_64":          base64.StdEncoding.EncodeToString(data),
		"commit":                 commit,
		realtimeFieldSampleRate:  s.request.SampleRate,
	}
	return s.writeEvent(ctx, event)
}

func (s *elevenLabsRealtimeStream) writeEvent(ctx context.Context, event map[string]any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	deadline := time.Now().Add(s.provider.cfg.WriteTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	if err := s.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	return nil
}

func (s *elevenLabsRealtimeStream) readLoop() {
	realtimeJSONReadLoop[elevenLabsServerEvent]{
		ctx:              s.ctx,
		conn:             s.conn,
		cancel:           s.cancel,
		events:           s.events,
		done:             s.done,
		hasWaitResult:    s.hasWaitResult,
		setWaitResult:    s.setWaitResult,
		currentWaitError: s.currentWaitError,
		signalUpdated:    s.signalUpdated,
		emit:             s.emit,
		handleEvent:      s.handleServerEvent,
	}.run()
}

func (s *elevenLabsRealtimeStream) handleServerEvent(event elevenLabsServerEvent) {
	switch event.MessageType {
	case elevenLabsMessageSessionStarted:
		if strings.TrimSpace(event.SessionID) == "" {
			providerErr := ErrProviderResponse
			s.emit(ProviderStreamEvent{Err: providerErr, IsFinal: true})
			s.setWaitResult(providerErr)
			s.signalUpdated(providerErr)
			return
		}
		s.sessionReady.Store(true)
		s.signalUpdated(nil)
	case elevenLabsMessagePartial:
		s.handlePartial(event.Text)
	case elevenLabsMessageCommitted:
		if s.provider.cfg.DisableTimestamps {
			s.handleCommitted(event)
		}
	case elevenLabsMessageCommittedWithTimestamps:
		s.handleCommitted(event)
	default:
		if isElevenLabsErrorEvent(event.MessageType) {
			s.handleProviderError(event)
		}
	}
}

func (s *elevenLabsRealtimeStream) handlePartial(text string) {
	if !s.provider.cfg.EmitPartials || !elevenLabsTranscriptHasContent(text) {
		return
	}
	s.stateMu.Lock()
	resultID, started := s.currentResultLocked()
	s.stateMu.Unlock()
	if started {
		s.emit(ProviderStreamEvent{ResultID: resultID, Started: true})
	}
	s.emit(ProviderStreamEvent{ResultID: resultID, Text: text})
}

func (s *elevenLabsRealtimeStream) handleCommitted(event elevenLabsServerEvent) {
	s.stateMu.Lock()
	text := event.Text
	accepted := elevenLabsTranscriptAccepted(text, event.Words, *s.provider.cfg.MinTranscriptLogProb)
	resultID := ""
	started := false
	discardedResultID := ""
	if accepted {
		resultID, started = s.currentResultLocked()
	} else if s.turnID != "" {
		discardedResultID = s.turnID
	}
	s.turnID = ""
	if s.commitsPending > 0 {
		s.commitsPending--
	}
	shouldComplete := s.closing && s.commitsPending == 0
	s.stateMu.Unlock()
	if started {
		s.emit(ProviderStreamEvent{ResultID: resultID, Started: true})
	}
	if discardedResultID != "" {
		s.emit(ProviderStreamEvent{ResultID: discardedResultID, Discarded: true})
	}
	if accepted {
		startAt, endAt := elevenLabsWordRange(event.Words)
		s.emit(ProviderStreamEvent{
			ResultID:         resultID,
			Text:             text,
			ConfirmedText:    text,
			DetectedLanguage: event.LanguageCode,
			StartAt:          startAt,
			EndAt:            endAt,
			IsFinal:          true,
		})
	}
	if shouldComplete {
		s.complete()
	}
}

func elevenLabsTranscriptHasContent(text string) bool {
	for _, character := range text {
		if unicode.IsLetter(character) || unicode.IsNumber(character) {
			return true
		}
	}
	return false
}

func elevenLabsTranscriptAccepted(
	text string,
	words []elevenLabsTranscriptWord,
	minimumMeanLogProb float64,
) bool {
	if !elevenLabsTranscriptHasContent(text) {
		return false
	}
	totalLogProb := 0.0
	lexicalWords := 0
	for _, word := range words {
		if !elevenLabsTranscriptHasContent(word.Text) {
			continue
		}
		totalLogProb += word.LogProb
		lexicalWords++
	}
	return lexicalWords == 0 || totalLogProb/float64(lexicalWords) >= minimumMeanLogProb
}

func (s *elevenLabsRealtimeStream) handleProviderError(event elevenLabsServerEvent) {
	providerErr := errors.Join(
		classifyElevenLabsServerError(event.MessageType),
		elevenLabsProviderError{messageType: event.MessageType, detail: event.Error},
	)
	if event.MessageType == elevenLabsErrorInsufficientAudio && s.settleEmptyFinalCommit() {
		return
	}
	if event.MessageType == elevenLabsErrorCommitThrottled && !s.isClosing() {
		s.settleOneCommit()
		s.emit(ProviderStreamEvent{Err: providerErr})
		return
	}
	s.emit(ProviderStreamEvent{Err: providerErr, IsFinal: true})
	s.setWaitResult(providerErr)
	s.signalUpdated(providerErr)
	_ = s.conn.Close()
}

func (s *elevenLabsRealtimeStream) currentResultLocked() (string, bool) {
	if s.turnID != "" {
		return s.turnID, false
	}
	s.turnNumber++
	s.turnID = s.request.SessionID + "_turn_" + qwenSequence(s.turnNumber)
	return s.turnID, true
}

func (s *elevenLabsRealtimeStream) markCommitSent() {
	s.stateMu.Lock()
	s.commitsPending++
	s.stateMu.Unlock()
}

func (s *elevenLabsRealtimeStream) settleOneCommit() {
	s.stateMu.Lock()
	if s.commitsPending > 0 {
		s.commitsPending--
	}
	shouldComplete := s.closing && s.commitsPending == 0
	s.stateMu.Unlock()
	if shouldComplete {
		s.complete()
	}
}

func (s *elevenLabsRealtimeStream) settleEmptyFinalCommit() bool {
	s.stateMu.Lock()
	if !s.closing || s.commitsPending == 0 {
		s.stateMu.Unlock()
		return false
	}
	s.commitsPending--
	shouldComplete := s.commitsPending == 0
	s.stateMu.Unlock()
	if shouldComplete {
		s.complete()
	}
	return true
}

func (s *elevenLabsRealtimeStream) isClosing() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.closing
}

func (s *elevenLabsRealtimeStream) complete() {
	s.completeOnce.Do(func() {
		s.setWaitResult(nil)
		_ = s.conn.Close()
	})
}

func (s *elevenLabsRealtimeStream) emit(event ProviderStreamEvent) {
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *elevenLabsRealtimeStream) signalUpdated(err error) {
	s.updatedOnce.Do(func() { s.updated <- err })
}

func (s *elevenLabsRealtimeStream) setWaitResult(err error) {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	if s.waitResultSet {
		return
	}
	s.waitErr = err
	s.waitResultSet = true
}

func (s *elevenLabsRealtimeStream) hasWaitResult() bool {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitResultSet
}

func (s *elevenLabsRealtimeStream) currentWaitError() error {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitErr
}

func (s *elevenLabsRealtimeStream) fail(err error) {
	s.setWaitResult(err)
	_ = s.conn.Close()
}

func (s *elevenLabsRealtimeStream) closeOnContext() {
	select {
	case <-s.ctx.Done():
		_ = s.conn.Close()
	case <-s.done:
	}
}

func (s *elevenLabsRealtimeStream) startFinishTimer() {
	s.finishTimer.Do(func() {
		go func() {
			timer := time.NewTimer(s.provider.cfg.FinishTimeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				s.fail(errors.Join(ErrRequestTimeout, ErrProviderUnavailable))
			case <-s.done:
			case <-s.ctx.Done():
			}
		}()
	})
}

func normalizeElevenLabsRealtimeConfig(
	cfg ElevenLabsRealtimeConfig,
) (ElevenLabsRealtimeConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultElevenLabsRealtimeName
	}
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultElevenLabsRealtimeEndpoint
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = defaultElevenLabsRealtimeModel
	}
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	if cfg.APIKey == "" {
		return cfg, ErrInvalidConfig
	}
	if _, err := normalizeElevenLabsEndpoint(cfg.Endpoint, cfg.AllowInsecureWebSocket); err != nil {
		return cfg, err
	}
	cfg.CommitStrategy = strings.ToLower(strings.TrimSpace(cfg.CommitStrategy))
	if cfg.CommitStrategy == "" {
		cfg.CommitStrategy = defaultElevenLabsCommitStrategy
	}
	if cfg.CommitStrategy != ElevenLabsCommitStrategyManual &&
		cfg.CommitStrategy != ElevenLabsCommitStrategyVAD {
		return cfg, ErrInvalidConfig
	}
	if cfg.VADSilenceThreshold == 0 {
		cfg.VADSilenceThreshold = defaultElevenLabsVADSilence
	}
	if cfg.VADSilenceThreshold < 300*time.Millisecond || cfg.VADSilenceThreshold > 3*time.Second {
		return cfg, ErrInvalidConfig
	}
	if cfg.VADThreshold == nil {
		threshold := defaultElevenLabsVADThreshold
		cfg.VADThreshold = &threshold
	} else {
		threshold := *cfg.VADThreshold
		cfg.VADThreshold = &threshold
	}
	if *cfg.VADThreshold < 0.1 || *cfg.VADThreshold > 0.9 {
		return cfg, ErrInvalidConfig
	}
	if cfg.MinTranscriptLogProb == nil {
		minimum := defaultElevenLabsMinTranscriptLogProb
		cfg.MinTranscriptLogProb = &minimum
	} else {
		minimum := *cfg.MinTranscriptLogProb
		cfg.MinTranscriptLogProb = &minimum
	}
	if *cfg.MinTranscriptLogProb < -20 || *cfg.MinTranscriptLogProb > 0 {
		return cfg, ErrInvalidConfig
	}
	if cfg.MinSpeechDuration == 0 {
		cfg.MinSpeechDuration = defaultElevenLabsMinSpeech
	}
	if cfg.MinSilenceDuration == 0 {
		cfg.MinSilenceDuration = defaultElevenLabsMinSilence
	}
	if cfg.MinSpeechDuration < 50*time.Millisecond || cfg.MinSpeechDuration > 2*time.Second ||
		cfg.MinSilenceDuration < 50*time.Millisecond || cfg.MinSilenceDuration > 2*time.Second {
		return cfg, ErrInvalidConfig
	}
	if cfg.ManualCommitInterval == 0 {
		cfg.ManualCommitInterval = defaultElevenLabsManualCommit
	}
	if cfg.ManualCommitInterval < time.Second || cfg.ManualCommitInterval > 30*time.Second {
		return cfg, ErrInvalidConfig
	}
	if cfg.FilterBackgroundAudio && !cfg.DisableTimestamps {
		return cfg, ErrInvalidConfig
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultElevenLabsHandshakeTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultElevenLabsWriteTimeout
	}
	if cfg.FinishTimeout <= 0 {
		cfg.FinishTimeout = defaultElevenLabsFinishTimeout
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultElevenLabsEventBuffer
	}
	return cfg, nil
}

func normalizeElevenLabsStreamingRequest(request StreamingRequest) (StreamingRequest, error) {
	if request.SessionID == "" || request.Channels != 1 || request.Format != AudioFormatRawPCM16 ||
		elevenLabsAudioFormat(request.SampleRate) == "" {
		return request, ErrInvalidRequest
	}
	languageTag, err := NormalizeLanguageTag(request.Language)
	if err != nil {
		return request, err
	}
	languageHints, err := normalizeLanguageHints(request.LanguageHints)
	if err != nil {
		return request, err
	}
	request.Language = languageTag
	request.LanguageHints = languageHints
	request.Context = cloneRecognitionContext(request.Context)
	return request, nil
}

func elevenLabsRealtimeEndpoint(
	cfg ElevenLabsRealtimeConfig,
	request StreamingRequest,
) (string, error) {
	endpoint, err := normalizeElevenLabsEndpoint(cfg.Endpoint, cfg.AllowInsecureWebSocket)
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", errors.Join(ErrInvalidConfig, err)
	}
	query := parsed.Query()
	query.Set("model_id", cfg.Model)
	query.Set("audio_format", elevenLabsAudioFormat(request.SampleRate))
	query.Set("commit_strategy", cfg.CommitStrategy)
	query.Set("include_timestamps", strconv.FormatBool(!cfg.DisableTimestamps))
	includeLanguageDetection := !cfg.DisableLanguageDetection && !cfg.DisableTimestamps
	query.Set("include_language_detection", strconv.FormatBool(includeLanguageDetection))
	query.Set("no_verbatim", strconv.FormatBool(cfg.NoVerbatim))
	query.Set("filter_background_audio", strconv.FormatBool(cfg.FilterBackgroundAudio))
	query.Set("enable_logging", strconv.FormatBool(!cfg.DisableLogging))
	query.Set("vad_silence_threshold_secs", strconv.FormatFloat(cfg.VADSilenceThreshold.Seconds(), 'f', -1, 64))
	query.Set("vad_threshold", strconv.FormatFloat(*cfg.VADThreshold, 'f', -1, 64))
	query.Set("min_speech_duration_ms", strconv.FormatInt(cfg.MinSpeechDuration.Milliseconds(), 10))
	query.Set("min_silence_duration_ms", strconv.FormatInt(cfg.MinSilenceDuration.Milliseconds(), 10))
	if request.Language != "" && request.Language != automaticLanguage {
		query.Set("language_code", openAIRealtimeLanguage(request.Language))
	}
	for _, keyterm := range elevenLabsKeyterms(request.Context.Terms) {
		query.Add("keyterms", keyterm)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func normalizeElevenLabsEndpoint(endpoint string, allowInsecure bool) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", errors.Join(ErrInvalidConfig, err)
	}
	switch parsed.Scheme {
	case httpSchemeSecure:
		parsed.Scheme = webSocketSchemeSecure
	case httpSchemeInsecure:
		parsed.Scheme = webSocketSchemeInsecure
	case webSocketSchemeSecure, webSocketSchemeInsecure:
	default:
		return "", ErrInvalidConfig
	}
	if parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return "", ErrInvalidConfig
	}
	if parsed.Scheme == webSocketSchemeInsecure && !allowInsecure && !isLoopbackHost(parsed.Hostname()) {
		return "", ErrInvalidConfig
	}
	return parsed.String(), nil
}

func elevenLabsAudioFormat(sampleRate int) string {
	switch sampleRate {
	case 8000, 16000, 22050, 24000, 44100, 48000:
		return "pcm_" + strconv.Itoa(sampleRate)
	default:
		return ""
	}
}

func elevenLabsKeyterms(terms []string) []string {
	seen := make(map[string]struct{}, min(len(terms), elevenLabsMaxKeyterms))
	keyterms := make([]string, 0, min(len(terms), elevenLabsMaxKeyterms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" || utf8.RuneCountInString(term) > elevenLabsMaxKeytermRunes {
			continue
		}
		if _, exists := seen[term]; exists {
			continue
		}
		seen[term] = struct{}{}
		keyterms = append(keyterms, term)
		if len(keyterms) == elevenLabsMaxKeyterms {
			break
		}
	}
	return keyterms
}

func elevenLabsWordRange(words []elevenLabsTranscriptWord) (time.Duration, time.Duration) {
	var start time.Duration
	var end time.Duration
	found := false
	for _, word := range words {
		if word.Start < 0 || word.End < word.Start {
			continue
		}
		wordStart := time.Duration(word.Start * float64(time.Second))
		wordEnd := time.Duration(word.End * float64(time.Second))
		if !found || wordStart < start {
			start = wordStart
		}
		if !found || wordEnd > end {
			end = wordEnd
		}
		found = true
	}
	return start, end
}

func isElevenLabsErrorEvent(messageType string) bool {
	switch messageType {
	case elevenLabsErrorGeneric, elevenLabsErrorAuth, elevenLabsErrorQuota,
		elevenLabsErrorCommitThrottled, elevenLabsErrorUnacceptedTerms,
		elevenLabsErrorRateLimited, elevenLabsErrorQueueOverflow,
		elevenLabsErrorResourceExhausted, elevenLabsErrorSessionTimeLimit,
		elevenLabsErrorInput, elevenLabsErrorChunkSize,
		elevenLabsErrorInsufficientAudio, elevenLabsErrorTranscriber:
		return true
	default:
		return false
	}
}

func classifyElevenLabsDialError(response *http.Response, err error) error {
	if response == nil {
		return errors.Join(ErrProviderUnavailable, err)
	}
	switch response.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return errors.Join(ErrUnauthorized, err)
	case http.StatusTooManyRequests:
		return errors.Join(ErrRateLimited, err)
	default:
		if response.StatusCode >= http.StatusInternalServerError {
			return errors.Join(ErrProviderUnavailable, err)
		}
		return errors.Join(ErrProviderRequest, err)
	}
}

func classifyElevenLabsServerError(messageType string) error {
	switch messageType {
	case elevenLabsErrorAuth:
		return ErrUnauthorized
	case elevenLabsErrorQuota, elevenLabsErrorRateLimited, elevenLabsErrorCommitThrottled:
		return ErrRateLimited
	case elevenLabsErrorQueueOverflow, elevenLabsErrorResourceExhausted,
		elevenLabsErrorSessionTimeLimit:
		return ErrProviderUnavailable
	case elevenLabsErrorInput, elevenLabsErrorChunkSize, elevenLabsErrorUnacceptedTerms:
		return ErrProviderRequest
	default:
		return ErrProviderResponse
	}
}

var (
	_ StreamingProvider = (*ElevenLabsRealtimeProvider)(nil)
	_ ProviderStream    = (*elevenLabsRealtimeStream)(nil)
)
