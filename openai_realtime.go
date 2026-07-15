package asr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultOpenAIRealtimeName             = "openai-realtime"
	defaultOpenAIRealtimeEndpoint         = "wss://api.openai.com/v1/realtime"
	defaultOpenAIRealtimeModel            = "gpt-4o-mini-transcribe"
	openAIRealtimeWhisperModel            = "gpt-realtime-whisper"
	defaultOpenAIRealtimeDelay            = OpenAIRealtimeDelayMedium
	defaultOpenAIRealtimeCommitInterval   = 3 * time.Second
	defaultOpenAIRealtimeHandshakeTimeout = 10 * time.Second
	defaultOpenAIRealtimeWriteTimeout     = 5 * time.Second
	defaultOpenAIRealtimeFinishTimeout    = 20 * time.Second
	defaultOpenAIRealtimeEventBuffer      = 128
	openAIRealtimeMinCommitAudio          = 100 * time.Millisecond
	openAIRealtimeSampleRate              = 24000
	defaultOpenAITurnDetectionType        = OpenAITurnDetectionSemanticVAD
	defaultOpenAISemanticVADEagerness     = OpenAISemanticVADEagernessAuto
	defaultOpenAIServerVADThreshold       = 0.5
	defaultOpenAIServerVADPrefixPadding   = 300 * time.Millisecond
	defaultOpenAIServerVADSilence         = 800 * time.Millisecond
	OpenAIRealtimeDelayMinimal            = "minimal"
	OpenAIRealtimeDelayLow                = "low"
	OpenAIRealtimeDelayMedium             = "medium"
	OpenAIRealtimeDelayHigh               = "high"
	OpenAIRealtimeDelayXHigh              = "xhigh"
	OpenAITurnDetectionServerVAD          = "server_vad"
	OpenAITurnDetectionSemanticVAD        = "semantic_vad"
	OpenAISemanticVADEagernessLow         = "low"
	OpenAISemanticVADEagernessMedium      = "medium"
	OpenAISemanticVADEagernessHigh        = "high"
	OpenAISemanticVADEagernessAuto        = "auto"
	openAIRealtimeSessionType             = "transcription"
	openAIRealtimeEndpointPath            = "/v1/realtime"
	openAIEventAudioCommitted             = "input_audio_buffer.committed"
	openAIEventTranscriptionFailed        = "conversation.item.input_audio_transcription.failed"
	webSocketSchemeSecure                 = "wss"
	webSocketSchemeInsecure               = "ws"
	httpSchemeSecure                      = "https"
	httpSchemeInsecure                    = "http"
)

type OpenAIRealtimeConfig struct {
	Name                   string
	Endpoint               string
	Model                  string
	APIKey                 string
	Delay                  string
	TurnDetectionType      string
	SemanticVADEagerness   string
	ServerVADThreshold     *float64
	ServerVADPrefixPadding time.Duration
	ServerVADSilence       time.Duration
	DisableTurnDetection   bool
	CommitInterval         time.Duration
	HandshakeTimeout       time.Duration
	WriteTimeout           time.Duration
	FinishTimeout          time.Duration
	EventBuffer            int
	AllowInsecureWebSocket bool
}

// OpenAIRealtimeProvider implements OpenAI's GA realtime transcription
// WebSocket protocol.
type OpenAIRealtimeProvider struct {
	cfg    OpenAIRealtimeConfig
	dialer websocket.Dialer
}

type openAIRealtimeStream struct {
	provider  *OpenAIRealtimeProvider
	request   StreamingRequest
	conn      *websocket.Conn
	resampler *pcm16StreamResampler

	ctx    context.Context //nolint:containedctx // The stream owns the provider connection lifecycle.
	cancel context.CancelFunc

	events  chan ProviderStreamEvent
	done    chan struct{}
	updated chan error

	writeMu      sync.Mutex
	resultMu     sync.Mutex
	stateMu      sync.Mutex
	closeOnce    sync.Once
	completeOnce sync.Once
	updatedOnce  sync.Once
	finishTimer  sync.Once
	eventCounter atomic.Uint64
	sessionReady atomic.Bool

	writeClosed        bool
	expectedSeq        uint64
	nextSample         int64
	samplesSinceCommit int64
	manualCommits      int
	pendingResults     map[string]struct{}
	closing            bool
	waitErr            error
	waitResultSet      bool
	partials           map[string]string
}

type openAIRealtimeServerEvent struct {
	Type       string                    `json:"type"`
	EventID    string                    `json:"event_id"`
	ItemID     string                    `json:"item_id"`
	ResponseID string                    `json:"response_id"`
	Delta      string                    `json:"delta"`
	Text       string                    `json:"text"`
	Transcript string                    `json:"transcript"`
	Error      openAIRealtimeServerError `json:"error"`
}

type openAIRealtimeServerError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Param   string `json:"param"`
	EventID string `json:"event_id"`
}

func (e openAIRealtimeServerError) Error() string {
	if code := strings.TrimSpace(e.Code); code != "" {
		return "openai realtime provider error: " + code
	}
	return "openai realtime provider error"
}

func NewOpenAIRealtimeProvider(cfg OpenAIRealtimeConfig) (*OpenAIRealtimeProvider, error) {
	normalized, err := normalizeOpenAIRealtimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &OpenAIRealtimeProvider{cfg: normalized, dialer: *websocket.DefaultDialer}, nil
}

func (p *OpenAIRealtimeProvider) Name() string { return p.cfg.Name }

func (p *OpenAIRealtimeProvider) Model() string { return p.cfg.Model }

// TurnDetectionEnabled reports whether OpenAI owns utterance boundaries.
func (p *OpenAIRealtimeProvider) TurnDetectionEnabled() bool {
	return p != nil && !p.cfg.DisableTurnDetection && p.cfg.Model != openAIRealtimeWhisperModel
}

// ServerVADEnabled retains the common streaming-provider contract. For this
// provider, true means either server_vad or semantic_vad is enabled.
func (p *OpenAIRealtimeProvider) ServerVADEnabled() bool { return p.TurnDetectionEnabled() }

func (p *OpenAIRealtimeProvider) StreamingCapabilities() StreamingCapabilities {
	return StreamingCapabilities{
		Formats:               []AudioFormat{AudioFormatRawPCM16},
		SampleRates:           []int{8000, 16000, openAIRealtimeSampleRate},
		SupportsLanguageHints: true,
		SupportsServerVAD:     true,
	}
}

func (p *OpenAIRealtimeProvider) Start(
	ctx context.Context,
	request StreamingRequest,
) (ProviderStream, error) {
	if p == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	normalized, err := normalizeOpenAIStreamingRequest(request)
	if err != nil {
		return nil, err
	}
	normalized.ServerVAD = p.TurnDetectionEnabled()
	endpoint, err := openAIRealtimeEndpoint(p.cfg.Endpoint, p.cfg.AllowInsecureWebSocket)
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+p.cfg.APIKey)
	conn, response, err := p.dialer.DialContext(ctx, endpoint, headers)
	if response != nil && response.Body != nil {
		_, _ = io.CopyN(io.Discard, response.Body, 4<<10)
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, classifyOpenAIRealtimeDialError(response, err)
	}
	resampler, err := newPCM16StreamResampler(normalized.SampleRate, openAIRealtimeSampleRate)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream := &openAIRealtimeStream{
		provider:       p,
		request:        normalized,
		conn:           conn,
		resampler:      resampler,
		ctx:            streamCtx,
		cancel:         cancel,
		events:         make(chan ProviderStreamEvent, p.cfg.EventBuffer),
		done:           make(chan struct{}),
		updated:        make(chan error, 1),
		partials:       make(map[string]string),
		pendingResults: make(map[string]struct{}),
	}
	go stream.readLoop()
	go stream.closeOnContext()
	if err := stream.sendSessionUpdate(ctx); err != nil {
		stream.Close()
		return nil, err
	}
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

func (s *openAIRealtimeStream) WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error {
	if s == nil || len(chunk.Data) == 0 || len(chunk.Data)%2 != 0 ||
		chunk.Sequence != s.expectedSeq || chunk.StartSample != s.nextSample ||
		chunk.EndSample <= chunk.StartSample || chunk.EndSample-chunk.StartSample != int64(len(chunk.Data)/2) {
		return ErrInvalidRequest
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeClosed {
		return ErrSessionClosed
	}
	providerAudio, err := s.resampler.Push(chunk.Data)
	if err != nil {
		return err
	}
	if len(providerAudio) > 0 {
		if err := s.writeAudioEvent(ctx, providerAudio); err != nil {
			s.fail(err)
			return err
		}
	}
	s.expectedSeq++
	s.nextSample = chunk.EndSample
	s.samplesSinceCommit += int64(len(providerAudio) / 2)
	commitSamples := durationSamples(s.provider.cfg.CommitInterval, openAIRealtimeSampleRate)
	if !s.provider.TurnDetectionEnabled() && s.samplesSinceCommit >= commitSamples {
		if err := s.commitAudioLocked(ctx, false); err != nil {
			s.fail(err)
			return err
		}
	}
	return nil
}

func (s *openAIRealtimeStream) CloseInput(ctx context.Context) error {
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
	tail, err := s.resampler.Flush()
	if err != nil {
		s.fail(err)
		return err
	}
	if len(tail) > 0 {
		if err := s.writeAudioEvent(ctx, tail); err != nil {
			s.fail(err)
			return err
		}
		s.samplesSinceCommit += int64(len(tail) / 2)
	}
	if err := s.commitAudioLocked(ctx, true); err != nil {
		s.fail(err)
		return err
	}
	s.startFinishTimer()
	s.completeIfDrained()
	return nil
}

func (s *openAIRealtimeStream) Events() <-chan ProviderStreamEvent { return s.events }

func (s *openAIRealtimeStream) Done() <-chan struct{} { return s.done }

func (s *openAIRealtimeStream) Wait(ctx context.Context) error {
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

func (s *openAIRealtimeStream) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.conn.Close()
	})
}

func (s *openAIRealtimeStream) sendSessionUpdate(ctx context.Context) error {
	transcription := s.transcriptionConfig()
	session := map[string]any{
		qwenFieldType: openAIRealtimeSessionType,
		qwenFieldAudio: map[string]any{
			"input": map[string]any{
				"format": map[string]any{
					qwenFieldType: "audio/pcm",
					"rate":        openAIRealtimeSampleRate,
				},
				"turn_detection": s.turnDetectionConfig(),
				"transcription":  transcription,
			},
		},
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeEvent(ctx, map[string]any{
		qwenFieldEventID: s.nextEventID(qwenFieldSession),
		qwenFieldType:    qwenEventSessionUpdate,
		qwenFieldSession: session,
	})
}

func (s *openAIRealtimeStream) turnDetectionConfig() any {
	if !s.provider.TurnDetectionEnabled() {
		return nil
	}
	if s.provider.cfg.TurnDetectionType == OpenAITurnDetectionServerVAD {
		return map[string]any{
			qwenFieldType:            OpenAITurnDetectionServerVAD,
			qwenFieldThreshold:       *s.provider.cfg.ServerVADThreshold,
			"prefix_padding_ms":      s.provider.cfg.ServerVADPrefixPadding.Milliseconds(),
			qwenFieldSilenceDuration: s.provider.cfg.ServerVADSilence.Milliseconds(),
		}
	}
	return map[string]any{
		qwenFieldType: OpenAITurnDetectionSemanticVAD,
		"eagerness":   s.provider.cfg.SemanticVADEagerness,
	}
}

func (s *openAIRealtimeStream) transcriptionConfig() map[string]any {
	transcription := map[string]any{defaultHTTPModelField: s.provider.cfg.Model}
	if s.provider.cfg.Model == openAIRealtimeWhisperModel {
		transcription["delay"] = s.provider.cfg.Delay
	}
	language := s.request.Language
	if language == automaticLanguage && len(s.request.LanguageHints) > 0 {
		language = s.request.LanguageHints[0]
	}
	if language != "" && language != automaticLanguage {
		transcription["language"] = openAIRealtimeLanguage(language)
	}
	return transcription
}

func (s *openAIRealtimeStream) writeAudioEvent(ctx context.Context, data []byte) error {
	return s.writeEvent(ctx, map[string]any{
		qwenFieldEventID: s.nextEventID("append"),
		qwenFieldType:    qwenEventAudioAppend,
		qwenFieldAudio:   base64.StdEncoding.EncodeToString(data),
	})
}

func (s *openAIRealtimeStream) commitAudioLocked(ctx context.Context, padTail bool) error {
	if s.samplesSinceCommit == 0 {
		return nil
	}
	minimumSamples := durationSamples(openAIRealtimeMinCommitAudio, openAIRealtimeSampleRate)
	paddingSamples := int64(0)
	if padTail && s.provider.TurnDetectionEnabled() {
		// A short tail guarantees that the final manual commit has enough audio
		// even when an automatic VAD commit raced with CloseInput.
		paddingSamples = minimumSamples
	} else if padTail && s.samplesSinceCommit < minimumSamples {
		paddingSamples = minimumSamples - s.samplesSinceCommit
	}
	if paddingSamples > 0 {
		padding := make([]byte, int(paddingSamples)*2)
		if err := s.writeAudioEvent(ctx, padding); err != nil {
			return err
		}
	}
	s.stateMu.Lock()
	s.manualCommits++
	s.stateMu.Unlock()
	if err := s.writeEvent(ctx, map[string]any{
		qwenFieldEventID: s.nextEventID("commit"),
		qwenFieldType:    qwenEventAudioCommit,
	}); err != nil {
		s.stateMu.Lock()
		s.manualCommits--
		s.stateMu.Unlock()
		return err
	}
	s.samplesSinceCommit = 0
	return nil
}

func (s *openAIRealtimeStream) writeEvent(ctx context.Context, event map[string]any) error {
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

func (s *openAIRealtimeStream) readLoop() {
	defer s.cancel()
	defer func() { _ = s.conn.Close() }()
	defer close(s.events)
	defer close(s.done)
	for {
		_, payload, err := s.conn.ReadMessage()
		if err != nil {
			if !s.hasWaitResult() {
				if s.ctx.Err() != nil {
					s.setWaitResult(errors.Join(ErrSessionClosed, s.ctx.Err()))
				} else {
					s.setWaitResult(errors.Join(ErrProviderUnavailable, err))
				}
			}
			s.signalUpdated(s.currentWaitError())
			return
		}
		var event openAIRealtimeServerEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			s.emit(ProviderStreamEvent{Err: errors.Join(ErrProviderResponse, err)})
			continue
		}
		s.handleServerEvent(event)
		if s.hasWaitResult() {
			return
		}
	}
}

func (s *openAIRealtimeStream) handleServerEvent(event openAIRealtimeServerEvent) {
	switch event.Type {
	case qwenEventSessionUpdated:
		s.sessionReady.Store(true)
		s.signalUpdated(nil)
	case openAIEventAudioCommitted:
		resultID := openAIRealtimeResultID(event)
		s.commitStarted(resultID)
		s.emit(ProviderStreamEvent{ResultID: resultID, Started: true})
	case qwenEventTranscriptionDelta:
		resultID := openAIRealtimeResultID(event)
		delta := event.Delta
		if delta == "" {
			delta = event.Text
		}
		if delta == "" {
			return
		}
		s.partials[resultID] += delta
		s.emit(ProviderStreamEvent{
			ResultID:      resultID,
			Text:          s.partials[resultID],
			ConfirmedText: s.partials[resultID],
		})
	case qwenEventTranscriptionCompleted, "response.text.done":
		resultID := openAIRealtimeResultID(event)
		text := event.Transcript
		if text == "" {
			text = event.Text
		}
		if text == "" {
			text = s.partials[resultID]
		}
		delete(s.partials, resultID)
		if strings.TrimSpace(text) != "" {
			s.emit(ProviderStreamEvent{
				ResultID:      resultID,
				Text:          text,
				ConfirmedText: text,
				IsFinal:       true,
			})
		}
		s.commitFinished(resultID)
	case openAIEventTranscriptionFailed:
		resultID := openAIRealtimeResultID(event)
		s.emit(ProviderStreamEvent{
			Err:      errors.Join(ErrProviderResponse, event.Error),
			ResultID: resultID,
			IsFinal:  true,
		})
		s.commitFinished(resultID)
	case qwenEventError:
		if s.settleEmptyCloseCommit(event.Error) {
			return
		}
		providerErr := errors.Join(classifyOpenAIRealtimeServerError(event.Error), event.Error)
		isRecoverable := s.sessionReady.Load()
		s.emit(ProviderStreamEvent{Err: providerErr, IsFinal: !isRecoverable})
		if !isRecoverable {
			s.setWaitResult(providerErr)
			s.signalUpdated(providerErr)
		}
	}
}

func (s *openAIRealtimeStream) commitStarted(resultID string) {
	s.stateMu.Lock()
	if s.manualCommits > 0 {
		s.manualCommits--
	}
	if resultID != "" {
		s.pendingResults[resultID] = struct{}{}
	}
	s.stateMu.Unlock()
	s.completeIfDrained()
}

func (s *openAIRealtimeStream) commitFinished(resultID string) {
	s.stateMu.Lock()
	delete(s.pendingResults, resultID)
	shouldComplete := s.closing && s.manualCommits == 0 && len(s.pendingResults) == 0
	s.stateMu.Unlock()
	if shouldComplete {
		s.complete()
	}
}

func (s *openAIRealtimeStream) completeIfDrained() {
	s.stateMu.Lock()
	shouldComplete := s.closing && s.manualCommits == 0 && len(s.pendingResults) == 0
	s.stateMu.Unlock()
	if shouldComplete {
		s.complete()
	}
}

func (s *openAIRealtimeStream) settleEmptyCloseCommit(serverError openAIRealtimeServerError) bool {
	if !isOpenAIRealtimeEmptyCommitError(serverError) {
		return false
	}
	s.stateMu.Lock()
	if !s.closing || s.manualCommits == 0 {
		s.stateMu.Unlock()
		return false
	}
	s.manualCommits--
	s.stateMu.Unlock()
	s.completeIfDrained()
	return true
}

func (s *openAIRealtimeStream) complete() {
	s.completeOnce.Do(func() {
		s.setWaitResult(nil)
		_ = s.conn.Close()
	})
}

func (s *openAIRealtimeStream) emit(event ProviderStreamEvent) {
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *openAIRealtimeStream) signalUpdated(err error) {
	s.updatedOnce.Do(func() { s.updated <- err })
}

func (s *openAIRealtimeStream) setWaitResult(err error) {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	if s.waitResultSet {
		return
	}
	s.waitErr = err
	s.waitResultSet = true
}

func (s *openAIRealtimeStream) hasWaitResult() bool {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitResultSet
}

func (s *openAIRealtimeStream) currentWaitError() error {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitErr
}

func (s *openAIRealtimeStream) fail(err error) {
	s.setWaitResult(err)
	_ = s.conn.Close()
}

func (s *openAIRealtimeStream) closeOnContext() {
	select {
	case <-s.ctx.Done():
		_ = s.conn.Close()
	case <-s.done:
	}
}

func (s *openAIRealtimeStream) startFinishTimer() {
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

func (s *openAIRealtimeStream) nextEventID(kind string) string {
	return s.request.SessionID + "_" + kind + "_" + qwenSequence(s.eventCounter.Add(1))
}

func normalizeOpenAIRealtimeConfig(cfg OpenAIRealtimeConfig) (OpenAIRealtimeConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultOpenAIRealtimeName
	}
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultOpenAIRealtimeEndpoint
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = defaultOpenAIRealtimeModel
	}
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	if cfg.APIKey == "" {
		return cfg, ErrInvalidConfig
	}
	if _, err := openAIRealtimeEndpoint(cfg.Endpoint, cfg.AllowInsecureWebSocket); err != nil {
		return cfg, err
	}
	cfg.Delay = strings.ToLower(strings.TrimSpace(cfg.Delay))
	if cfg.Delay == "" {
		cfg.Delay = defaultOpenAIRealtimeDelay
	}
	switch cfg.Delay {
	case OpenAIRealtimeDelayMinimal, OpenAIRealtimeDelayLow, OpenAIRealtimeDelayMedium,
		OpenAIRealtimeDelayHigh, OpenAIRealtimeDelayXHigh:
	default:
		return cfg, ErrInvalidConfig
	}
	cfg.TurnDetectionType = strings.ToLower(strings.TrimSpace(cfg.TurnDetectionType))
	if cfg.TurnDetectionType == "" {
		cfg.TurnDetectionType = defaultOpenAITurnDetectionType
	}
	if cfg.TurnDetectionType != OpenAITurnDetectionServerVAD &&
		cfg.TurnDetectionType != OpenAITurnDetectionSemanticVAD {
		return cfg, ErrInvalidConfig
	}
	cfg.SemanticVADEagerness = strings.ToLower(strings.TrimSpace(cfg.SemanticVADEagerness))
	if cfg.SemanticVADEagerness == "" {
		cfg.SemanticVADEagerness = defaultOpenAISemanticVADEagerness
	}
	switch cfg.SemanticVADEagerness {
	case OpenAISemanticVADEagernessLow, OpenAISemanticVADEagernessMedium,
		OpenAISemanticVADEagernessHigh, OpenAISemanticVADEagernessAuto:
	default:
		return cfg, ErrInvalidConfig
	}
	if cfg.ServerVADThreshold == nil {
		threshold := defaultOpenAIServerVADThreshold
		cfg.ServerVADThreshold = &threshold
	} else {
		threshold := *cfg.ServerVADThreshold
		cfg.ServerVADThreshold = &threshold
	}
	if *cfg.ServerVADThreshold < 0 || *cfg.ServerVADThreshold > 1 {
		return cfg, ErrInvalidConfig
	}
	if cfg.ServerVADPrefixPadding == 0 {
		cfg.ServerVADPrefixPadding = defaultOpenAIServerVADPrefixPadding
	}
	if cfg.ServerVADPrefixPadding < time.Millisecond {
		return cfg, ErrInvalidConfig
	}
	if cfg.ServerVADSilence == 0 {
		cfg.ServerVADSilence = defaultOpenAIServerVADSilence
	}
	if cfg.ServerVADSilence < time.Millisecond {
		return cfg, ErrInvalidConfig
	}
	if cfg.CommitInterval == 0 {
		cfg.CommitInterval = defaultOpenAIRealtimeCommitInterval
	}
	if cfg.CommitInterval < 500*time.Millisecond || cfg.CommitInterval > 30*time.Second {
		return cfg, ErrInvalidConfig
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultOpenAIRealtimeHandshakeTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultOpenAIRealtimeWriteTimeout
	}
	if cfg.FinishTimeout <= 0 {
		cfg.FinishTimeout = defaultOpenAIRealtimeFinishTimeout
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultOpenAIRealtimeEventBuffer
	}
	return cfg, nil
}

func normalizeOpenAIStreamingRequest(request StreamingRequest) (StreamingRequest, error) {
	if request.SessionID == "" || request.Channels != 1 || request.Format != AudioFormatRawPCM16 ||
		(request.SampleRate != 8000 && request.SampleRate != 16000 &&
			request.SampleRate != openAIRealtimeSampleRate) {
		return request, ErrInvalidRequest
	}
	language, err := NormalizeLanguageTag(request.Language)
	if err != nil {
		return request, err
	}
	hints, err := normalizeLanguageHints(request.LanguageHints)
	if err != nil {
		return request, err
	}
	request.Language = language
	request.LanguageHints = hints
	request.Context = cloneRecognitionContext(request.Context)
	return request, nil
}

func openAIRealtimeEndpoint(endpoint string, allowInsecure bool) (string, error) {
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
	path := strings.TrimRight(parsed.Path, "/")
	if path == "" {
		path = openAIRealtimeEndpointPath
	}
	if path != openAIRealtimeEndpointPath {
		return "", ErrInvalidConfig
	}
	parsed.Path = path
	query := parsed.Query()
	query.Set("intent", "transcription")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func classifyOpenAIRealtimeDialError(response *http.Response, err error) error {
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

func classifyOpenAIRealtimeServerError(serverError openAIRealtimeServerError) error {
	code := strings.ToLower(serverError.Code)
	switch {
	case strings.Contains(code, "auth"), strings.Contains(code, "unauthorized"),
		strings.Contains(code, "forbidden"):
		return ErrUnauthorized
	case strings.Contains(code, "rate"), strings.Contains(code, "quota"):
		return ErrRateLimited
	case strings.Contains(code, "overload"):
		return ErrOverloaded
	default:
		return ErrProviderResponse
	}
}

func isOpenAIRealtimeEmptyCommitError(serverError openAIRealtimeServerError) bool {
	details := strings.ToLower(strings.Join([]string{
		serverError.Type,
		serverError.Code,
		serverError.Message,
		serverError.Param,
	}, " "))
	return strings.Contains(details, "commit") &&
		(strings.Contains(details, "empty") || strings.Contains(details, "no audio"))
}

func openAIRealtimeResultID(event openAIRealtimeServerEvent) string {
	if event.ItemID != "" {
		return event.ItemID
	}
	if event.ResponseID != "" {
		return event.ResponseID
	}
	return event.EventID
}

func openAIRealtimeLanguage(language string) string {
	primary, _, _ := strings.Cut(language, "-")
	return strings.ToLower(primary)
}

var (
	_ StreamingProvider = (*OpenAIRealtimeProvider)(nil)
	_ ProviderStream    = (*openAIRealtimeStream)(nil)
)
