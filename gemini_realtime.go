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
	defaultGeminiRealtimeName        = "gemini-realtime"
	defaultGeminiRealtimeEndpoint    = "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
	defaultGeminiRealtimeModel       = "gemini-3.1-flash-live-preview"
	defaultGeminiRealtimeInstruction = "Listen carefully to the user's audio. After each user turn, respond only with a brief neutral acknowledgment."
	defaultGeminiStartSensitivity    = GeminiStartSensitivityHigh
	defaultGeminiEndSensitivity      = GeminiEndSensitivityHigh
	defaultGeminiPrefixPadding       = 300 * time.Millisecond
	defaultGeminiSilenceDuration     = 300 * time.Millisecond
	defaultGeminiMaxContinuousTurn   = 15 * time.Second
	defaultGeminiFinalDrain          = 500 * time.Millisecond
	defaultGeminiFinishIdle          = 2 * time.Second
	defaultGeminiHandshakeTimeout    = 10 * time.Second
	defaultGeminiWriteTimeout        = 5 * time.Second
	defaultGeminiFinishTimeout       = 20 * time.Second
	defaultGeminiEventBuffer         = 128
	geminiRealtimeSampleRate         = 16000
	geminiAudioMIMEType              = "audio/pcm;rate=16000"
	geminiActivityHandlingInterrupts = "START_OF_ACTIVITY_INTERRUPTS"
	GeminiStartSensitivityHigh       = "START_SENSITIVITY_HIGH"
	GeminiStartSensitivityLow        = "START_SENSITIVITY_LOW"
	GeminiEndSensitivityHigh         = "END_SENSITIVITY_HIGH"
	GeminiEndSensitivityLow          = "END_SENSITIVITY_LOW"
)

type GeminiRealtimeConfig struct {
	Name                            string
	Endpoint                        string
	Model                           string
	APIKey                          string
	SystemInstruction               string
	StartOfSpeechSensitivity        string
	EndOfSpeechSensitivity          string
	PrefixPadding                   time.Duration
	SilenceDuration                 time.Duration
	MaxContinuousTurn               time.Duration
	FinalTranscriptDrain            time.Duration
	FinishIdleTimeout               time.Duration
	HandshakeTimeout                time.Duration
	WriteTimeout                    time.Duration
	FinishTimeout                   time.Duration
	EventBuffer                     int
	DisableContextWindowCompression bool
	DisableContinuousTurnFlush      bool
	AllowInsecureWebSocket          bool
}

// GeminiRealtimeProvider implements the Gemini Live API raw WebSocket
// protocol for input audio transcription.
type GeminiRealtimeProvider struct {
	cfg    GeminiRealtimeConfig
	dialer websocket.Dialer
}

type geminiRealtimeStream struct {
	provider  *GeminiRealtimeProvider
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
	sessionReady atomic.Bool
	// providerSamples and turnBoundarySample use Gemini's normalized 16kHz
	// sample clock, independently of the caller's input sample rate.
	providerSamples    atomic.Int64
	turnBoundarySample atomic.Int64

	writeClosed            bool
	expectedSeq            uint64
	nextSample             int64
	wroteAudio             bool
	closing                bool
	turnNumber             uint64
	turnID                 string
	turnText               string
	boundarySeen           bool
	boundaryEpoch          uint64
	activityEpoch          uint64
	ignoreNextTurnComplete bool
	waitErr                error
	waitResultSet          bool
}

type geminiServerMessage struct {
	SetupComplete *struct{}            `json:"setupComplete"`
	ServerContent *geminiServerContent `json:"serverContent"`
	Error         *geminiServerError   `json:"error"`
	GoAway        *geminiGoAway        `json:"goAway"`
}

type geminiServerContent struct {
	InputTranscription *geminiTranscription `json:"inputTranscription"`
	GenerationComplete bool                 `json:"generationComplete"`
	TurnComplete       bool                 `json:"turnComplete"`
	Interrupted        bool                 `json:"interrupted"`
}

type geminiTranscription struct {
	Text string `json:"text"`
}

type geminiGoAway struct {
	TimeLeft string `json:"timeLeft"`
}

type geminiServerError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

func (e geminiServerError) Error() string {
	if status := strings.TrimSpace(e.Status); status != "" {
		return "gemini realtime provider error: " + status
	}
	return "gemini realtime provider error"
}

func NewGeminiRealtimeProvider(cfg GeminiRealtimeConfig) (*GeminiRealtimeProvider, error) {
	normalized, err := normalizeGeminiRealtimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &GeminiRealtimeProvider{cfg: normalized, dialer: *websocket.DefaultDialer}, nil
}

func (p *GeminiRealtimeProvider) Name() string { return p.cfg.Name }

func (p *GeminiRealtimeProvider) Model() string { return p.cfg.Model }

func (p *GeminiRealtimeProvider) ServerVADEnabled() bool { return p != nil }

func (p *GeminiRealtimeProvider) StreamingCapabilities() StreamingCapabilities {
	return StreamingCapabilities{
		Formats:           []AudioFormat{AudioFormatRawPCM16},
		SampleRates:       []int{8000, 16000, 24000, 32000, 44100, 48000},
		SupportsServerVAD: true,
	}
}

func (p *GeminiRealtimeProvider) Start(
	ctx context.Context,
	request StreamingRequest,
) (ProviderStream, error) {
	if p == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	normalized, err := normalizeGeminiStreamingRequest(request)
	if err != nil {
		return nil, err
	}
	normalized.ServerVAD = true
	endpoint, err := geminiRealtimeEndpoint(p.cfg.Endpoint, p.cfg.APIKey, p.cfg.AllowInsecureWebSocket)
	if err != nil {
		return nil, err
	}
	conn, response, err := p.dialer.DialContext(ctx, endpoint, nil)
	if response != nil && response.Body != nil {
		_, _ = io.CopyN(io.Discard, response.Body, 4<<10)
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, classifyGeminiDialError(response, err)
	}
	resampler, err := newGeminiRealtimeResampler(normalized.SampleRate)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream := &geminiRealtimeStream{
		provider:  p,
		request:   normalized,
		conn:      conn,
		resampler: resampler,
		ctx:       streamCtx,
		cancel:    cancel,
		events:    make(chan ProviderStreamEvent, p.cfg.EventBuffer),
		done:      make(chan struct{}),
		updated:   make(chan error, 1),
	}
	go stream.readLoop()
	go stream.closeOnContext()
	if err := stream.sendSetup(ctx); err != nil {
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

func (s *geminiRealtimeStream) WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error {
	if s == nil {
		return ErrSessionClosed
	}
	if len(chunk.Data) == 0 || len(chunk.Data)%2 != 0 || chunk.Sequence != s.expectedSeq ||
		chunk.StartSample != s.nextSample || chunk.EndSample <= chunk.StartSample ||
		chunk.EndSample-chunk.StartSample != int64(len(chunk.Data)/2) {
		return ErrInvalidRequest
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeClosed {
		return ErrSessionClosed
	}
	if s.continuousTurnFlushDue() {
		if err := s.writeAudioStreamEndLocked(ctx); err != nil {
			s.fail(err)
			return err
		}
		s.handleTurnBoundary()
	}
	providerAudio := chunk.Data
	if s.resampler != nil {
		var err error
		providerAudio, err = s.resampler.Push(chunk.Data)
		if err != nil {
			return err
		}
	}
	if len(providerAudio) > 0 {
		if err := s.writeAudioLocked(ctx, providerAudio); err != nil {
			s.fail(err)
			return err
		}
		s.wroteAudio = true
		s.providerSamples.Add(int64(len(providerAudio) / 2))
	}
	s.expectedSeq++
	s.nextSample = chunk.EndSample
	return nil
}

func (s *geminiRealtimeStream) CloseInput(ctx context.Context) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeClosed {
		return ErrSessionClosed
	}
	s.writeClosed = true
	var tail []byte
	if s.resampler != nil {
		var err error
		tail, err = s.resampler.Flush()
		if err != nil {
			s.fail(err)
			return err
		}
	}
	if len(tail) > 0 {
		if err := s.writeAudioLocked(ctx, tail); err != nil {
			s.fail(err)
			return err
		}
		s.wroteAudio = true
		s.providerSamples.Add(int64(len(tail) / 2))
	}
	if err := s.writeAudioStreamEndLocked(ctx); err != nil {
		s.fail(err)
		return err
	}
	s.stateMu.Lock()
	s.closing = true
	s.activityEpoch++
	activityEpoch := s.activityEpoch
	wroteAudio := s.wroteAudio
	s.stateMu.Unlock()
	s.startFinishTimer()
	if !wroteAudio {
		s.complete()
		return nil
	}
	s.scheduleFinishIdle(activityEpoch)
	return nil
}

func newGeminiRealtimeResampler(inputRate int) (*pcm16StreamResampler, error) {
	if inputRate == geminiRealtimeSampleRate {
		return nil, nil
	}
	return newPCM16StreamResampler(inputRate, geminiRealtimeSampleRate)
}

func (s *geminiRealtimeStream) Events() <-chan ProviderStreamEvent {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *geminiRealtimeStream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *geminiRealtimeStream) Wait(ctx context.Context) error {
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

func (s *geminiRealtimeStream) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.conn.Close()
	})
}

func (s *geminiRealtimeStream) sendSetup(ctx context.Context) error {
	setup := map[string]any{
		defaultHTTPModelField: "models/" + s.provider.cfg.Model,
		"generationConfig": map[string]any{
			"responseModalities": []string{"AUDIO"},
		},
		"inputAudioTranscription": map[string]any{},
		"realtimeInputConfig": map[string]any{
			"automaticActivityDetection": map[string]any{
				"disabled":                 false,
				"startOfSpeechSensitivity": s.provider.cfg.StartOfSpeechSensitivity,
				"endOfSpeechSensitivity":   s.provider.cfg.EndOfSpeechSensitivity,
				"prefixPaddingMs":          s.provider.cfg.PrefixPadding.Milliseconds(),
				"silenceDurationMs":        s.provider.cfg.SilenceDuration.Milliseconds(),
			},
			"activityHandling": geminiActivityHandlingInterrupts,
		},
	}
	if instruction := strings.TrimSpace(s.provider.cfg.SystemInstruction); instruction != "" {
		setup["systemInstruction"] = map[string]any{
			"parts": []map[string]string{{qwenFieldText: instruction}},
		}
	}
	if !s.provider.cfg.DisableContextWindowCompression {
		setup["contextWindowCompression"] = map[string]any{
			"slidingWindow": map[string]any{},
		}
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeEvent(ctx, map[string]any{"setup": setup})
}

func (s *geminiRealtimeStream) writeAudioLocked(ctx context.Context, data []byte) error {
	return s.writeEvent(ctx, map[string]any{
		"realtimeInput": map[string]any{
			"audio": map[string]any{
				"data":     base64.StdEncoding.EncodeToString(data),
				"mimeType": geminiAudioMIMEType,
			},
		},
	})
}

func (s *geminiRealtimeStream) writeAudioStreamEndLocked(ctx context.Context) error {
	return s.writeEvent(ctx, map[string]any{
		"realtimeInput": map[string]any{"audioStreamEnd": true},
	})
}

func (s *geminiRealtimeStream) continuousTurnFlushDue() bool {
	if s.provider.cfg.DisableContinuousTurnFlush || s.provider.cfg.MaxContinuousTurn <= 0 {
		return false
	}
	maximumSamples := durationSamples(
		s.provider.cfg.MaxContinuousTurn,
		geminiRealtimeSampleRate,
	)
	return s.providerSamples.Load()-s.turnBoundarySample.Load() >= maximumSamples
}

func (s *geminiRealtimeStream) writeEvent(ctx context.Context, event map[string]any) error {
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

func (s *geminiRealtimeStream) readLoop() {
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
		var event geminiServerMessage
		if err := json.Unmarshal(payload, &event); err != nil {
			s.emit(ProviderStreamEvent{Err: errors.Join(ErrProviderResponse, err)})
			continue
		}
		if s.handleServerMessage(event) {
			return
		}
		if s.hasWaitResult() {
			return
		}
	}
}

func (s *geminiRealtimeStream) handleServerMessage(event geminiServerMessage) bool {
	if event.SetupComplete != nil {
		s.sessionReady.Store(true)
		s.signalUpdated(nil)
	}
	if event.Error != nil {
		providerErr := errors.Join(classifyGeminiServerError(*event.Error), *event.Error)
		s.emit(ProviderStreamEvent{Err: providerErr, IsFinal: true})
		s.setWaitResult(providerErr)
		s.signalUpdated(providerErr)
		return true
	}
	if event.GoAway != nil {
		providerErr := ErrProviderUnavailable
		s.emit(ProviderStreamEvent{Err: providerErr, IsFinal: true})
		s.setWaitResult(providerErr)
		s.signalUpdated(providerErr)
		return true
	}
	if event.ServerContent == nil {
		return false
	}
	content := event.ServerContent
	if content.InputTranscription != nil {
		s.handleInputTranscription(content.InputTranscription.Text)
	}
	if s.hasInputTurnBoundary(content) {
		s.handleTurnBoundary()
	}
	return false
}

func (s *geminiRealtimeStream) hasInputTurnBoundary(content *geminiServerContent) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	boundary := false
	if content.GenerationComplete {
		boundary = true
		s.ignoreNextTurnComplete = true
	}
	if content.Interrupted {
		// interrupted describes the previous model output. The speech that
		// caused it belongs to the next input turn and must not be finalized.
		s.ignoreNextTurnComplete = true
	}
	if content.TurnComplete {
		if s.ignoreNextTurnComplete {
			s.ignoreNextTurnComplete = false
		} else {
			boundary = true
		}
	}
	return boundary
}

func (s *geminiRealtimeStream) handleInputTranscription(chunk string) {
	if strings.TrimSpace(chunk) == "" {
		return
	}
	s.stateMu.Lock()
	started := false
	if s.turnID == "" {
		s.turnNumber++
		s.turnID = s.request.SessionID + "_turn_" + qwenSequence(s.turnNumber)
		started = true
	}
	s.turnText = appendGeminiTranscript(s.turnText, chunk)
	resultID := s.turnID
	text := s.turnText
	s.activityEpoch++
	activityEpoch := s.activityEpoch
	boundarySeen := s.boundarySeen
	if boundarySeen {
		s.boundaryEpoch++
	}
	boundaryEpoch := s.boundaryEpoch
	closing := s.closing
	s.stateMu.Unlock()
	if started {
		s.emit(ProviderStreamEvent{ResultID: resultID, Started: true})
	}
	s.emit(ProviderStreamEvent{
		ResultID:      resultID,
		Text:          text,
		ConfirmedText: text,
	})
	if boundarySeen {
		s.scheduleBoundaryFinalization(boundaryEpoch)
	}
	if closing {
		s.scheduleFinishIdle(activityEpoch)
	}
}

func (s *geminiRealtimeStream) handleTurnBoundary() {
	s.turnBoundarySample.Store(s.providerSamples.Load())
	s.stateMu.Lock()
	s.boundarySeen = true
	s.boundaryEpoch++
	boundaryEpoch := s.boundaryEpoch
	s.activityEpoch++
	activityEpoch := s.activityEpoch
	closing := s.closing
	hasText := strings.TrimSpace(s.turnText) != ""
	s.stateMu.Unlock()
	if hasText || closing {
		s.scheduleBoundaryFinalization(boundaryEpoch)
	}
	if closing {
		s.scheduleFinishIdle(activityEpoch)
	}
}

func (s *geminiRealtimeStream) scheduleBoundaryFinalization(epoch uint64) {
	go func() {
		timer := time.NewTimer(s.provider.cfg.FinalTranscriptDrain)
		defer timer.Stop()
		select {
		case <-timer.C:
			s.finalizeBoundary(epoch)
		case <-s.done:
		case <-s.ctx.Done():
		}
	}()
}

func (s *geminiRealtimeStream) finalizeBoundary(epoch uint64) {
	s.stateMu.Lock()
	if !s.boundarySeen || s.boundaryEpoch != epoch {
		s.stateMu.Unlock()
		return
	}
	resultID := s.turnID
	text := strings.TrimSpace(s.turnText)
	s.turnID = ""
	s.turnText = ""
	s.boundarySeen = false
	closing := s.closing
	s.stateMu.Unlock()
	if text != "" {
		s.emit(ProviderStreamEvent{
			ResultID:      resultID,
			Text:          text,
			ConfirmedText: text,
			IsFinal:       true,
		})
	}
	if closing {
		s.complete()
	}
}

func (s *geminiRealtimeStream) scheduleFinishIdle(epoch uint64) {
	go func() {
		timer := time.NewTimer(s.provider.cfg.FinishIdleTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			s.finalizeIdleClose(epoch)
		case <-s.done:
		case <-s.ctx.Done():
		}
	}()
}

func (s *geminiRealtimeStream) finalizeIdleClose(epoch uint64) {
	s.stateMu.Lock()
	if !s.closing || s.activityEpoch != epoch {
		s.stateMu.Unlock()
		return
	}
	resultID := s.turnID
	text := strings.TrimSpace(s.turnText)
	s.turnID = ""
	s.turnText = ""
	s.boundarySeen = false
	s.stateMu.Unlock()
	if text != "" {
		s.emit(ProviderStreamEvent{
			ResultID:      resultID,
			Text:          text,
			ConfirmedText: text,
			IsFinal:       true,
		})
	}
	s.complete()
}

func (s *geminiRealtimeStream) complete() {
	s.completeOnce.Do(func() {
		s.setWaitResult(nil)
		_ = s.conn.Close()
	})
}

func (s *geminiRealtimeStream) emit(event ProviderStreamEvent) {
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *geminiRealtimeStream) signalUpdated(err error) {
	s.updatedOnce.Do(func() { s.updated <- err })
}

func (s *geminiRealtimeStream) setWaitResult(err error) {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	if s.waitResultSet {
		return
	}
	s.waitErr = err
	s.waitResultSet = true
}

func (s *geminiRealtimeStream) hasWaitResult() bool {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitResultSet
}

func (s *geminiRealtimeStream) currentWaitError() error {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitErr
}

func (s *geminiRealtimeStream) fail(err error) {
	s.setWaitResult(err)
	_ = s.conn.Close()
}

func (s *geminiRealtimeStream) closeOnContext() {
	select {
	case <-s.ctx.Done():
		_ = s.conn.Close()
	case <-s.done:
	}
}

func (s *geminiRealtimeStream) startFinishTimer() {
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

func normalizeGeminiRealtimeConfig(cfg GeminiRealtimeConfig) (GeminiRealtimeConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultGeminiRealtimeName
	}
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultGeminiRealtimeEndpoint
	}
	cfg.Model = strings.TrimPrefix(strings.TrimSpace(cfg.Model), "models/")
	if cfg.Model == "" {
		cfg.Model = defaultGeminiRealtimeModel
	}
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	if cfg.APIKey == "" {
		return cfg, ErrInvalidConfig
	}
	if _, err := geminiRealtimeEndpoint(cfg.Endpoint, cfg.APIKey, cfg.AllowInsecureWebSocket); err != nil {
		return cfg, err
	}
	cfg.SystemInstruction = strings.TrimSpace(cfg.SystemInstruction)
	if cfg.SystemInstruction == "" {
		cfg.SystemInstruction = defaultGeminiRealtimeInstruction
	}
	cfg.StartOfSpeechSensitivity = strings.ToUpper(strings.TrimSpace(cfg.StartOfSpeechSensitivity))
	if cfg.StartOfSpeechSensitivity == "" {
		cfg.StartOfSpeechSensitivity = defaultGeminiStartSensitivity
	}
	if cfg.StartOfSpeechSensitivity != GeminiStartSensitivityHigh &&
		cfg.StartOfSpeechSensitivity != GeminiStartSensitivityLow {
		return cfg, ErrInvalidConfig
	}
	cfg.EndOfSpeechSensitivity = strings.ToUpper(strings.TrimSpace(cfg.EndOfSpeechSensitivity))
	if cfg.EndOfSpeechSensitivity == "" {
		cfg.EndOfSpeechSensitivity = defaultGeminiEndSensitivity
	}
	if cfg.EndOfSpeechSensitivity != GeminiEndSensitivityHigh &&
		cfg.EndOfSpeechSensitivity != GeminiEndSensitivityLow {
		return cfg, ErrInvalidConfig
	}
	if cfg.PrefixPadding == 0 {
		cfg.PrefixPadding = defaultGeminiPrefixPadding
	}
	if cfg.PrefixPadding < time.Millisecond {
		return cfg, ErrInvalidConfig
	}
	if cfg.SilenceDuration == 0 {
		cfg.SilenceDuration = defaultGeminiSilenceDuration
	}
	if cfg.SilenceDuration < time.Millisecond {
		return cfg, ErrInvalidConfig
	}
	if cfg.MaxContinuousTurn == 0 {
		cfg.MaxContinuousTurn = defaultGeminiMaxContinuousTurn
	}
	if cfg.MaxContinuousTurn < 2*time.Second || cfg.MaxContinuousTurn > 5*time.Minute {
		return cfg, ErrInvalidConfig
	}
	if cfg.FinalTranscriptDrain == 0 {
		cfg.FinalTranscriptDrain = defaultGeminiFinalDrain
	}
	if cfg.FinalTranscriptDrain < 10*time.Millisecond || cfg.FinalTranscriptDrain > 2*time.Second {
		return cfg, ErrInvalidConfig
	}
	if cfg.FinishIdleTimeout == 0 {
		cfg.FinishIdleTimeout = defaultGeminiFinishIdle
	}
	if cfg.FinishIdleTimeout < cfg.FinalTranscriptDrain {
		return cfg, ErrInvalidConfig
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultGeminiHandshakeTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultGeminiWriteTimeout
	}
	if cfg.FinishTimeout <= 0 {
		cfg.FinishTimeout = defaultGeminiFinishTimeout
	}
	if cfg.FinishIdleTimeout >= cfg.FinishTimeout {
		return cfg, ErrInvalidConfig
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultGeminiEventBuffer
	}
	return cfg, nil
}

func normalizeGeminiStreamingRequest(request StreamingRequest) (StreamingRequest, error) {
	if request.SessionID == "" || request.SampleRate <= 0 || request.Channels != 1 ||
		request.Format != AudioFormatRawPCM16 {
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

func geminiRealtimeEndpoint(endpoint, apiKey string, allowInsecure bool) (string, error) {
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
	query := parsed.Query()
	query.Set("key", apiKey)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func classifyGeminiDialError(response *http.Response, err error) error {
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

func classifyGeminiServerError(serverError geminiServerError) error {
	status := strings.ToUpper(strings.TrimSpace(serverError.Status))
	switch {
	case serverError.Code == http.StatusUnauthorized, serverError.Code == http.StatusForbidden,
		strings.Contains(status, "UNAUTHENTICATED"), strings.Contains(status, "PERMISSION_DENIED"):
		return ErrUnauthorized
	case serverError.Code == http.StatusTooManyRequests, strings.Contains(status, "RESOURCE_EXHAUSTED"):
		return ErrRateLimited
	case serverError.Code >= http.StatusInternalServerError,
		strings.Contains(status, "UNAVAILABLE"), strings.Contains(status, "ABORTED"):
		return ErrProviderUnavailable
	default:
		return ErrProviderResponse
	}
}

func appendGeminiTranscript(current, next string) string {
	if current == "" {
		return next
	}
	if next == "" || strings.HasSuffix(current, next) {
		return current
	}
	if strings.HasPrefix(next, current) {
		return next
	}
	return current + next
}

var (
	_ StreamingProvider = (*GeminiRealtimeProvider)(nil)
	_ ProviderStream    = (*geminiRealtimeStream)(nil)
)
