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
	defaultQwenRealtimeName             = "qwen-realtime"
	defaultQwenRealtimeModel            = "qwen3-asr-flash-realtime"
	defaultQwenRealtimeHandshakeTimeout = 10 * time.Second
	defaultQwenRealtimeWriteTimeout     = 5 * time.Second
	defaultQwenRealtimeFinishTimeout    = 20 * time.Second
	defaultQwenRealtimeEventBuffer      = 128
	defaultQwenRealtimeVADThreshold     = 0.0
	defaultQwenRealtimeSilence          = 400 * time.Millisecond
	qwenRealtimeBetaHeader              = "realtime=v1"
	qwenFieldEventID                    = "event_id"
	qwenFieldType                       = "type"
	qwenFieldText                       = "text"
	qwenFieldSession                    = "session"
	qwenFieldAudio                      = "audio"
	qwenAudioFormatPCM                  = "pcm"
	qwenEventSessionUpdate              = "session.update"
	qwenEventSessionUpdated             = "session.updated"
	qwenEventAudioAppend                = "input_audio_buffer.append"
	qwenEventSpeechStarted              = "input_audio_buffer.speech_started"
	qwenEventSpeechStopped              = "input_audio_buffer.speech_stopped"
	qwenEventTranscriptionCompleted     = "conversation.item.input_audio_transcription.completed"
	qwenEventError                      = "error"
	qwenOmniFinishPadding               = 100 * time.Millisecond
	qwenOmniIdleDrain                   = 1500 * time.Millisecond
	qwenOmniCancelErrorWindow           = 5 * time.Second
)

type qwenRealtimeProtocol uint8

const (
	qwenRealtimeProtocolASR qwenRealtimeProtocol = iota
	qwenRealtimeProtocolOmni
)

type qwenOmniSettings struct {
	turnDetectionType string
	instructions      string
	keepModelResponse bool
}

type QwenRealtimeConfig struct {
	Name                     string
	Model                    string
	Endpoint                 string
	APIKey                   string
	WorkspaceID              string
	ServerVADThreshold       float64
	ServerVADSilenceDuration time.Duration
	DisableServerVAD         bool
	HandshakeTimeout         time.Duration
	WriteTimeout             time.Duration
	FinishTimeout            time.Duration
	EventBuffer              int
	AllowInsecureWebSocket   bool
}

type QwenRealtimeProvider struct {
	cfg      QwenRealtimeConfig
	dialer   websocket.Dialer
	omni     qwenOmniSettings
	protocol qwenRealtimeProtocol
}

type qwenRealtimeStream struct {
	provider *QwenRealtimeProvider
	request  StreamingRequest
	conn     *websocket.Conn

	ctx    context.Context //nolint:containedctx // The provider stream owns the connection lifecycle.
	cancel context.CancelFunc

	events  chan ProviderStreamEvent
	done    chan struct{}
	updated chan error

	writeMu         sync.Mutex
	resultMu        sync.Mutex
	omniMu          sync.Mutex
	closeOnce       sync.Once
	updatedOnce     sync.Once
	finishTimer     sync.Once
	eventCounter    atomic.Uint64
	sessionReady    atomic.Bool
	writeClosed     bool
	expectedSeq     uint64
	nextSample      int64
	waitErr         error
	waitResultSet   bool
	itemTimes       map[string]qwenItemTime
	omniPending     map[string]struct{}
	omniClosing     bool
	omniCancelUntil time.Time
}

type qwenItemTime struct {
	start time.Duration
	end   time.Duration
}

type qwenServerEvent struct {
	Type       string          `json:"type"`
	EventID    string          `json:"event_id"`
	ItemID     string          `json:"item_id"`
	Text       string          `json:"text"`
	Stash      string          `json:"stash"`
	Transcript string          `json:"transcript"`
	Language   string          `json:"language"`
	Emotion    string          `json:"emotion"`
	AudioStart int64           `json:"audio_start_ms"`
	AudioEnd   int64           `json:"audio_end_ms"`
	Error      qwenServerError `json:"error"`
}

type qwenServerError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Param   string `json:"param"`
	EventID string `json:"event_id"`
}

func (e qwenServerError) Error() string {
	code := strings.TrimSpace(e.Code)
	if code == "" {
		return "qwen realtime provider error"
	}
	return "qwen realtime provider error: " + code
}

func NewQwenRealtimeProvider(cfg QwenRealtimeConfig) (*QwenRealtimeProvider, error) {
	normalized, err := normalizeQwenRealtimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &QwenRealtimeProvider{
		cfg:    normalized,
		dialer: *websocket.DefaultDialer,
	}, nil
}

func (p *QwenRealtimeProvider) Name() string { return p.cfg.Name }

func (p *QwenRealtimeProvider) Model() string { return p.cfg.Model }

// ServerVADEnabled reports the normalized Qwen provider policy. Server VAD is
// enabled by default and can only be disabled explicitly in provider config.
func (p *QwenRealtimeProvider) ServerVADEnabled() bool {
	return p != nil && !p.cfg.DisableServerVAD
}

func (p *QwenRealtimeProvider) StreamingCapabilities() StreamingCapabilities {
	return StreamingCapabilities{
		Formats:               []AudioFormat{AudioFormatRawPCM16},
		SampleRates:           []int{8000, 16000},
		SupportsPrompt:        true,
		SupportsTerms:         true,
		SupportsLanguageHints: true,
		SupportsServerVAD:     true,
	}
}

func (p *QwenRealtimeProvider) Start(
	ctx context.Context,
	request StreamingRequest,
) (ProviderStream, error) {
	if p == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	normalized, err := normalizeQwenStreamingRequest(request)
	if err != nil {
		return nil, err
	}
	normalized.ServerVAD = p.ServerVADEnabled()
	endpoint, err := qwenEndpoint(p.cfg.Endpoint, p.cfg.Model)
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+p.cfg.APIKey)
	if p.protocol == qwenRealtimeProtocolASR {
		headers.Set("OpenAI-Beta", qwenRealtimeBetaHeader)
	}
	if p.cfg.WorkspaceID != "" {
		headers.Set("X-DashScope-WorkSpace", p.cfg.WorkspaceID)
	}
	conn, response, err := p.dialer.DialContext(ctx, endpoint, headers)
	if response != nil && response.Body != nil {
		_, _ = io.CopyN(io.Discard, response.Body, 4<<10)
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, classifyQwenDialError(response, err)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream := &qwenRealtimeStream{
		provider:    p,
		request:     normalized,
		conn:        conn,
		ctx:         streamCtx,
		cancel:      cancel,
		events:      make(chan ProviderStreamEvent, p.cfg.EventBuffer),
		done:        make(chan struct{}),
		updated:     make(chan error, 1),
		itemTimes:   make(map[string]qwenItemTime),
		omniPending: make(map[string]struct{}),
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
	case err := <-stream.updated:
		if err != nil {
			stream.Close()
			return nil, err
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

func (s *qwenRealtimeStream) WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error {
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
	event := map[string]any{
		qwenFieldEventID: s.nextEventID("audio"),
		qwenFieldType:    qwenEventAudioAppend,
		qwenFieldAudio:   base64.StdEncoding.EncodeToString(chunk.Data),
	}
	if err := s.writeEvent(ctx, event); err != nil {
		s.fail(err)
		return err
	}
	s.expectedSeq++
	s.nextSample = chunk.EndSample
	return nil
}

func (s *qwenRealtimeStream) CloseInput(ctx context.Context) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeClosed {
		return ErrSessionClosed
	}
	s.writeClosed = true
	if s.provider.protocol == qwenRealtimeProtocolOmni {
		return s.closeOmniInputLocked(ctx)
	}
	if !s.request.ServerVAD {
		if err := s.writeEvent(ctx, map[string]any{
			qwenFieldEventID: s.nextEventID("commit"),
			qwenFieldType:    "input_audio_buffer.commit",
		}); err != nil {
			s.fail(err)
			return err
		}
	}
	if err := s.writeEvent(ctx, map[string]any{
		qwenFieldEventID: s.nextEventID("finish"),
		qwenFieldType:    "session.finish",
	}); err != nil {
		s.fail(err)
		return err
	}
	s.startFinishTimer()
	return nil
}

func (s *qwenRealtimeStream) closeOmniInputLocked(ctx context.Context) error {
	s.omniMu.Lock()
	s.omniClosing = true
	s.omniMu.Unlock()
	if s.request.ServerVAD {
		paddingDuration := s.provider.cfg.ServerVADSilenceDuration + qwenOmniFinishPadding
		paddingSamples := durationSamples(paddingDuration, int64(s.request.SampleRate))
		if paddingSamples <= 0 {
			return ErrInvalidConfig
		}
		padding := make([]byte, int(paddingSamples)*2)
		if err := s.writeEvent(ctx, map[string]any{
			qwenFieldEventID: s.nextEventID("drain"),
			qwenFieldType:    qwenEventAudioAppend,
			qwenFieldAudio:   base64.StdEncoding.EncodeToString(padding),
		}); err != nil {
			s.fail(err)
			return err
		}
		s.scheduleOmniIdleCompletion(paddingDuration + qwenOmniIdleDrain)
	} else if err := s.writeEvent(ctx, map[string]any{
		qwenFieldEventID: s.nextEventID("commit"),
		qwenFieldType:    "input_audio_buffer.commit",
	}); err != nil {
		s.fail(err)
		return err
	}
	s.startFinishTimer()
	return nil
}

func (s *qwenRealtimeStream) Events() <-chan ProviderStreamEvent {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *qwenRealtimeStream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *qwenRealtimeStream) Wait(ctx context.Context) error {
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

func (s *qwenRealtimeStream) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.conn.Close()
	})
}

func (s *qwenRealtimeStream) sendSessionUpdate(ctx context.Context) error {
	if s.provider.protocol == qwenRealtimeProtocolOmni {
		return s.sendOmniSessionUpdate(ctx)
	}
	session := map[string]any{
		"modalities":         []string{qwenFieldText},
		"input_audio_format": qwenAudioFormatPCM,
		"sample_rate":        s.request.SampleRate,
	}
	transcription := make(map[string]any)
	if s.request.Language != "" && s.request.Language != automaticLanguage {
		transcription["language"] = s.request.Language
	} else if len(s.request.LanguageHints) > 0 {
		transcription["language"] = s.request.LanguageHints[0]
	}
	if corpus := qwenContextCorpus(s.request.Context); corpus != "" {
		transcription["corpus"] = map[string]any{qwenFieldText: corpus}
	}
	if len(transcription) > 0 {
		session["input_audio_transcription"] = transcription
	}
	if s.request.ServerVAD {
		session["turn_detection"] = map[string]any{
			qwenFieldType:         "server_vad",
			"threshold":           s.provider.cfg.ServerVADThreshold,
			"silence_duration_ms": s.provider.cfg.ServerVADSilenceDuration.Milliseconds(),
		}
	} else {
		session["turn_detection"] = nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeEvent(ctx, map[string]any{
		qwenFieldEventID: s.nextEventID(qwenFieldSession),
		qwenFieldType:    qwenEventSessionUpdate,
		qwenFieldSession: session,
	})
}

func (s *qwenRealtimeStream) sendOmniSessionUpdate(ctx context.Context) error {
	session := map[string]any{
		"modalities":                []string{qwenFieldText},
		"input_audio_format":        qwenAudioFormatPCM,
		"input_audio_transcription": map[string]any{},
		"instructions":              s.provider.omni.instructions,
	}
	if s.request.ServerVAD {
		session["turn_detection"] = map[string]any{
			qwenFieldType:         s.provider.omni.turnDetectionType,
			"threshold":           s.provider.cfg.ServerVADThreshold,
			"silence_duration_ms": s.provider.cfg.ServerVADSilenceDuration.Milliseconds(),
		}
	} else {
		session["turn_detection"] = nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeEvent(ctx, map[string]any{
		qwenFieldEventID: s.nextEventID(qwenFieldSession),
		qwenFieldType:    qwenEventSessionUpdate,
		qwenFieldSession: session,
	})
}

func (s *qwenRealtimeStream) writeEvent(ctx context.Context, event map[string]any) error {
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

func (s *qwenRealtimeStream) readLoop() {
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
		var event qwenServerEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			s.emit(ProviderStreamEvent{Err: errors.Join(ErrProviderResponse, err)})
			continue
		}
		switch event.Type {
		case qwenEventSessionUpdated:
			s.sessionReady.Store(true)
			s.signalUpdated(nil)
		case qwenEventSpeechStarted:
			itemTime := s.itemTimes[event.ItemID]
			itemTime.start = time.Duration(event.AudioStart) * time.Millisecond
			s.itemTimes[event.ItemID] = itemTime
			s.emit(ProviderStreamEvent{
				ResultID: event.ItemID,
				StartAt:  itemTime.start,
				Started:  true,
			})
			s.trackOmniItemStarted(event.ItemID)
		case qwenEventSpeechStopped:
			itemTime := s.itemTimes[event.ItemID]
			itemTime.end = time.Duration(event.AudioEnd) * time.Millisecond
			s.itemTimes[event.ItemID] = itemTime
		case "conversation.item.input_audio_transcription.text",
			"conversation.item.input_audio_transcription.delta":
			itemTime := s.itemTimes[event.ItemID]
			s.emit(ProviderStreamEvent{
				ResultID:         event.ItemID,
				Text:             event.Text + event.Stash,
				ConfirmedText:    event.Text,
				DraftText:        event.Stash,
				DetectedLanguage: event.Language,
				Emotion:          event.Emotion,
				StartAt:          itemTime.start,
				EndAt:            itemTime.end,
			})
		case qwenEventTranscriptionCompleted:
			itemTime := s.itemTimes[event.ItemID]
			s.emit(ProviderStreamEvent{
				ResultID:         event.ItemID,
				Text:             event.Transcript,
				ConfirmedText:    event.Transcript,
				DetectedLanguage: event.Language,
				Emotion:          event.Emotion,
				StartAt:          itemTime.start,
				EndAt:            itemTime.end,
				IsFinal:          true,
			})
			delete(s.itemTimes, event.ItemID)
			s.trackOmniItemFinished(event.ItemID)
		case "conversation.item.input_audio_transcription.failed":
			s.emit(ProviderStreamEvent{
				Err:      errors.Join(ErrProviderResponse, event.Error),
				ResultID: event.ItemID,
				IsFinal:  true,
			})
			s.trackOmniItemFinished(event.ItemID)
		case "response.created":
			s.cancelOmniResponse()
		case "response.done":
			s.clearOmniCancelPending()
		case qwenEventError:
			if s.isExpectedOmniCancelError(event.Error) {
				continue
			}
			providerErr := errors.Join(classifyQwenServerError(event.Error), event.Error)
			isRecoverable := s.provider.protocol == qwenRealtimeProtocolOmni && s.sessionReady.Load()
			s.emit(ProviderStreamEvent{
				Err:      providerErr,
				ResultID: event.ItemID,
				IsFinal:  !isRecoverable,
			})
			if isRecoverable {
				continue
			}
			s.setWaitResult(providerErr)
			s.signalUpdated(providerErr)
			return
		case "session.finished":
			s.setWaitResult(nil)
			s.signalUpdated(nil)
			return
		}
	}
}

func (s *qwenRealtimeStream) emit(event ProviderStreamEvent) {
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *qwenRealtimeStream) signalUpdated(err error) {
	s.updatedOnce.Do(func() { s.updated <- err })
}

func (s *qwenRealtimeStream) setWaitResult(err error) {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	if s.waitResultSet {
		return
	}
	s.waitErr = err
	s.waitResultSet = true
}

func (s *qwenRealtimeStream) hasWaitResult() bool {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitResultSet
}

func (s *qwenRealtimeStream) currentWaitError() error {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitErr
}

func (s *qwenRealtimeStream) fail(err error) {
	s.setWaitResult(err)
	_ = s.conn.Close()
}

func (s *qwenRealtimeStream) closeOnContext() {
	select {
	case <-s.ctx.Done():
		_ = s.conn.Close()
	case <-s.done:
	}
}

func (s *qwenRealtimeStream) startFinishTimer() {
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

func (s *qwenRealtimeStream) trackOmniItemStarted(itemID string) {
	if s.provider.protocol != qwenRealtimeProtocolOmni || itemID == "" {
		return
	}
	s.omniMu.Lock()
	s.omniPending[itemID] = struct{}{}
	s.omniMu.Unlock()
}

func (s *qwenRealtimeStream) trackOmniItemFinished(itemID string) {
	if s.provider.protocol != qwenRealtimeProtocolOmni {
		return
	}
	s.omniMu.Lock()
	delete(s.omniPending, itemID)
	shouldComplete := s.omniClosing && len(s.omniPending) == 0
	s.omniMu.Unlock()
	if shouldComplete {
		s.completeOmniStream()
	}
}

func (s *qwenRealtimeStream) scheduleOmniIdleCompletion(delay time.Duration) {
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			s.omniMu.Lock()
			shouldComplete := s.omniClosing && len(s.omniPending) == 0
			s.omniMu.Unlock()
			if shouldComplete {
				s.completeOmniStream()
			}
		case <-s.done:
		case <-s.ctx.Done():
		}
	}()
}

func (s *qwenRealtimeStream) completeOmniStream() {
	s.setWaitResult(nil)
	_ = s.conn.Close()
}

func (s *qwenRealtimeStream) cancelOmniResponse() {
	if s.provider.protocol != qwenRealtimeProtocolOmni || s.provider.omni.keepModelResponse {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.omniMu.Lock()
	s.omniCancelUntil = time.Now().Add(qwenOmniCancelErrorWindow)
	s.omniMu.Unlock()
	err := s.writeEvent(s.ctx, map[string]any{
		qwenFieldEventID: s.nextEventID("cancel"),
		qwenFieldType:    "response.cancel",
	})
	if err != nil {
		s.omniMu.Lock()
		s.omniCancelUntil = time.Time{}
		s.omniMu.Unlock()
	}
}

func (s *qwenRealtimeStream) isExpectedOmniCancelError(serverError qwenServerError) bool {
	if s.provider.protocol != qwenRealtimeProtocolOmni {
		return false
	}
	// Omni text-only responses can finish between response.created and the
	// ASR-only adapter's response.cancel. The resulting event-level error only
	// means there is no longer an answer to cancel; input transcription remains
	// usable and the provider connection must stay open.
	explicitCancelError := strings.Contains(serverError.EventID, "_cancel_") ||
		strings.Contains(strings.ToLower(serverError.Code), "cancel")
	s.omniMu.Lock()
	cancelPending := !s.omniCancelUntil.IsZero() && time.Now().Before(s.omniCancelUntil)
	if explicitCancelError || cancelPending {
		s.omniCancelUntil = time.Time{}
	}
	s.omniMu.Unlock()
	return explicitCancelError || cancelPending
}

func (s *qwenRealtimeStream) clearOmniCancelPending() {
	if s.provider.protocol != qwenRealtimeProtocolOmni {
		return
	}
	s.omniMu.Lock()
	s.omniCancelUntil = time.Time{}
	s.omniMu.Unlock()
}

func (s *qwenRealtimeStream) nextEventID(kind string) string {
	return s.request.SessionID + "_" + kind + "_" + qwenSequence(s.eventCounter.Add(1))
}

func normalizeQwenRealtimeConfig(cfg QwenRealtimeConfig) (QwenRealtimeConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultQwenRealtimeName
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = defaultQwenRealtimeModel
	}
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.WorkspaceID = strings.TrimSpace(cfg.WorkspaceID)
	if cfg.Endpoint == "" || cfg.APIKey == "" || cfg.ServerVADThreshold < -1 || cfg.ServerVADThreshold > 1 {
		return cfg, ErrInvalidConfig
	}
	parsed, err := url.Parse(cfg.Endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" ||
		(parsed.Scheme != "wss" && parsed.Scheme != "ws") {
		if err != nil {
			return cfg, errors.Join(ErrInvalidConfig, err)
		}
		return cfg, ErrInvalidConfig
	}
	if parsed.Scheme == "ws" && !cfg.AllowInsecureWebSocket && !isLoopbackHost(parsed.Hostname()) {
		return cfg, ErrInvalidConfig
	}
	if cfg.ServerVADThreshold == 0 {
		cfg.ServerVADThreshold = defaultQwenRealtimeVADThreshold
	}
	if cfg.ServerVADSilenceDuration == 0 {
		cfg.ServerVADSilenceDuration = defaultQwenRealtimeSilence
	}
	if cfg.ServerVADSilenceDuration < 200*time.Millisecond || cfg.ServerVADSilenceDuration > 6*time.Second {
		return cfg, ErrInvalidConfig
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultQwenRealtimeHandshakeTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultQwenRealtimeWriteTimeout
	}
	if cfg.FinishTimeout <= 0 {
		cfg.FinishTimeout = defaultQwenRealtimeFinishTimeout
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultQwenRealtimeEventBuffer
	}
	return cfg, nil
}

func normalizeQwenStreamingRequest(request StreamingRequest) (StreamingRequest, error) {
	if request.SessionID == "" || request.Channels != 1 || request.Format != AudioFormatRawPCM16 ||
		(request.SampleRate != 8000 && request.SampleRate != 16000) {
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

func qwenEndpoint(endpoint, model string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", errors.Join(ErrInvalidConfig, err)
	}
	query := parsed.Query()
	query.Set("model", model)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func qwenContextCorpus(context RecognitionContext) string {
	parts := make([]string, 0, 1+len(context.Terms))
	if prompt := strings.TrimSpace(context.Prompt); prompt != "" {
		parts = append(parts, prompt)
	}
	seen := make(map[string]struct{}, len(context.Terms))
	for _, term := range context.Terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		if _, exists := seen[term]; exists {
			continue
		}
		seen[term] = struct{}{}
		parts = append(parts, term)
	}
	return strings.Join(parts, "\n")
}

func classifyQwenDialError(response *http.Response, err error) error {
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

func classifyQwenServerError(serverError qwenServerError) error {
	code := strings.ToLower(serverError.Code)
	switch {
	case strings.Contains(code, "auth"), strings.Contains(code, "unauthorized"):
		return ErrUnauthorized
	case strings.Contains(code, "rate"), strings.Contains(code, "quota"):
		return ErrRateLimited
	case strings.Contains(code, "overload"):
		return ErrOverloaded
	default:
		return ErrProviderResponse
	}
}

func qwenSequence(value uint64) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var data [20]byte
	index := len(data)
	for value > 0 {
		index--
		data[index] = digits[value%10]
		value /= 10
	}
	return string(data[index:])
}

var (
	_ StreamingProvider = (*QwenRealtimeProvider)(nil)
	_ ProviderStream    = (*qwenRealtimeStream)(nil)
)
