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
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultVLLMRealtimeName             = "vllm-realtime"
	defaultVLLMRealtimeEndpoint         = "ws://127.0.0.1:6006/v1/realtime"
	defaultVLLMRealtimeModel            = "Qwen/Qwen3-ASR-0.6B"
	defaultVLLMRealtimeHandshakeTimeout = 10 * time.Second
	defaultVLLMRealtimeWriteTimeout     = 5 * time.Second
	defaultVLLMRealtimeFinishTimeout    = 20 * time.Second
	defaultVLLMRealtimeEventBuffer      = 128
	vllmRealtimeSampleRate              = 16000
	vllmRealtimeEndpointPath            = "/v1/realtime"
	vllmEventSessionCreated             = "session.created"
	vllmEventSessionUpdate              = "session.update"
	vllmEventAudioAppend                = "input_audio_buffer.append"
	vllmEventAudioCommit                = "input_audio_buffer.commit"
	vllmEventTranscriptionDelta         = "transcription.delta"
	vllmEventTranscriptionDone          = "transcription.done"
	vllmEventError                      = "error"
)

type VLLMRealtimeConfig struct {
	Name                   string
	Endpoint               string
	Model                  string
	APIKey                 string
	HandshakeTimeout       time.Duration
	WriteTimeout           time.Duration
	FinishTimeout          time.Duration
	EventBuffer            int
	AllowInsecureWebSocket bool
}

type VLLMRealtimeProvider struct {
	cfg    VLLMRealtimeConfig
	dialer websocket.Dialer
}

type vllmRealtimeStream struct {
	provider *VLLMRealtimeProvider
	request  StreamingRequest
	conn     *websocket.Conn

	ctx    context.Context //nolint:containedctx // The stream owns the provider connection lifecycle.
	cancel context.CancelFunc

	events chan ProviderStreamEvent
	done   chan struct{}
	ready  chan error

	writeMu     sync.Mutex
	stateMu     sync.Mutex
	resultMu    sync.Mutex
	closeOnce   sync.Once
	readyOnce   sync.Once
	finishTimer sync.Once

	writeClosed   bool
	expectedSeq   uint64
	nextSample    int64
	partial       string
	started       bool
	waitErr       error
	waitResultSet bool
}

type vllmRealtimeServerEvent struct {
	Type    string          `json:"type"`
	ID      string          `json:"id"`
	Created int64           `json:"created"`
	Delta   string          `json:"delta"`
	Text    string          `json:"text"`
	Usage   json.RawMessage `json:"usage"`
	Error   json.RawMessage `json:"error"`
	Code    string          `json:"code"`
}

type vllmRealtimeError struct {
	Code    string
	Message string
}

func (e vllmRealtimeError) Error() string {
	if e.Code != "" {
		return "vllm realtime provider error: " + e.Code
	}
	return "vllm realtime provider error"
}

func NewVLLMRealtimeProvider(cfg VLLMRealtimeConfig) (*VLLMRealtimeProvider, error) {
	normalized, err := normalizeVLLMRealtimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = normalized.HandshakeTimeout
	return &VLLMRealtimeProvider{cfg: normalized, dialer: dialer}, nil
}

func (p *VLLMRealtimeProvider) Name() string { return p.cfg.Name }

func (p *VLLMRealtimeProvider) Model() string { return p.cfg.Model }

func (p *VLLMRealtimeProvider) ServerVADEnabled() bool { return false }

func (p *VLLMRealtimeProvider) StreamingCapabilities() StreamingCapabilities {
	return StreamingCapabilities{
		Formats:     []AudioFormat{AudioFormatRawPCM16},
		SampleRates: []int{vllmRealtimeSampleRate},
	}
}

func (p *VLLMRealtimeProvider) Start(
	ctx context.Context,
	request StreamingRequest,
) (ProviderStream, error) {
	if p == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	normalized, err := normalizeVLLMStreamingRequest(request)
	if err != nil {
		return nil, err
	}
	endpoint, err := vllmRealtimeEndpoint(p.cfg.Endpoint, p.cfg.AllowInsecureWebSocket)
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	if p.cfg.APIKey != "" {
		headers.Set("Authorization", "Bearer "+p.cfg.APIKey)
	}
	conn, response, err := p.dialer.DialContext(ctx, endpoint, headers)
	if response != nil && response.Body != nil {
		_, _ = io.CopyN(io.Discard, response.Body, 4<<10)
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, classifyVLLMRealtimeDialError(response, err)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream := &vllmRealtimeStream{
		provider: p,
		request:  normalized,
		conn:     conn,
		ctx:      streamCtx,
		cancel:   cancel,
		events:   make(chan ProviderStreamEvent, p.cfg.EventBuffer),
		done:     make(chan struct{}),
		ready:    make(chan error, 1),
	}
	go stream.readLoop()
	go stream.closeOnContext()
	if err := stream.waitForSessionCreated(ctx); err != nil {
		stream.Close()
		return nil, err
	}
	if err := stream.initialize(ctx); err != nil {
		stream.fail(err)
		stream.Close()
		return nil, err
	}
	return stream, nil
}

func (s *vllmRealtimeStream) WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error {
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
	if err := s.writeJSONLocked(ctx, map[string]any{
		qwenFieldType:  vllmEventAudioAppend,
		qwenFieldAudio: base64.StdEncoding.EncodeToString(chunk.Data),
	}); err != nil {
		s.fail(err)
		return err
	}
	s.expectedSeq++
	s.nextSample = chunk.EndSample
	return nil
}

func (s *vllmRealtimeStream) CloseInput(ctx context.Context) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeClosed {
		return ErrSessionClosed
	}
	s.writeClosed = true
	if err := s.writeJSONLocked(ctx, map[string]any{
		qwenFieldType: vllmEventAudioCommit,
		"final":       true,
	}); err != nil {
		s.fail(err)
		return err
	}
	s.startFinishTimer()
	return nil
}

func (s *vllmRealtimeStream) Events() <-chan ProviderStreamEvent { return s.events }

func (s *vllmRealtimeStream) Done() <-chan struct{} { return s.done }

func (s *vllmRealtimeStream) Wait(ctx context.Context) error {
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

func (s *vllmRealtimeStream) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.conn.Close()
	})
}

func (s *vllmRealtimeStream) waitForSessionCreated(ctx context.Context) error {
	timer := time.NewTimer(s.provider.cfg.HandshakeTimeout)
	defer timer.Stop()
	select {
	case readyErr := <-s.ready:
		return readyErr
	case <-timer.C:
		return errors.Join(ErrRequestTimeout, ErrProviderUnavailable)
	case <-ctx.Done():
		return errors.Join(ErrProviderUnavailable, ctx.Err())
	}
}

func (s *vllmRealtimeStream) initialize(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writeJSONLocked(ctx, map[string]any{
		qwenFieldType:         vllmEventSessionUpdate,
		defaultHTTPModelField: s.provider.cfg.Model,
	}); err != nil {
		return err
	}
	return s.writeJSONLocked(ctx, map[string]any{qwenFieldType: vllmEventAudioCommit})
}

func (s *vllmRealtimeStream) writeJSONLocked(ctx context.Context, event map[string]any) error {
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

func (s *vllmRealtimeStream) readLoop() {
	realtimeJSONReadLoop[vllmRealtimeServerEvent]{
		ctx:              s.ctx,
		conn:             s.conn,
		cancel:           s.cancel,
		events:           s.events,
		done:             s.done,
		hasWaitResult:    s.hasWaitResult,
		setWaitResult:    s.setWaitResult,
		currentWaitError: s.currentWaitError,
		signalUpdated:    s.signalReady,
		emit:             s.emit,
		handleEvent:      s.handleServerEvent,
	}.run()
}

func (s *vllmRealtimeStream) handleServerEvent(event vllmRealtimeServerEvent) {
	switch event.Type {
	case vllmEventSessionCreated:
		s.signalReady(nil)
	case vllmEventTranscriptionDelta:
		// Qwen3ASRRealtimeGeneration currently violates the append-only protocol:
		// it may expose "language ...<asr_text>" and repeat overlapping internal
		// audio segments (vllm-project/vllm#35767). Keep the provider payload
		// intact here because the protocol has no segment IDs or rollback metadata;
		// heuristic deduplication could remove words the speaker actually repeated.
		// A future upstream normalized delta/segment contract belongs at this
		// adapter boundary. Until then, use Voxtral for this realtime endpoint or
		// use Qwen3-ASR through the post-processed HTTP transcription endpoint.
		if event.Delta == "" {
			return
		}
		s.stateMu.Lock()
		s.partial += event.Delta
		text := s.partial
		started := !s.started
		s.started = true
		s.stateMu.Unlock()
		s.emit(ProviderStreamEvent{
			ResultID:      s.resultID(),
			Text:          text,
			ConfirmedText: text,
			Started:       started,
		})
	case vllmEventTranscriptionDone:
		if !s.trySetWaitResult(nil) {
			return
		}
		s.stateMu.Lock()
		partial := s.partial
		s.partial = ""
		s.stateMu.Unlock()
		text := event.Text
		if text == "" {
			text = partial
		}
		if strings.TrimSpace(text) != "" {
			s.emit(ProviderStreamEvent{
				ResultID:      s.resultID(),
				Text:          text,
				ConfirmedText: text,
				IsFinal:       true,
			})
		} else if strings.TrimSpace(partial) != "" {
			s.emit(ProviderStreamEvent{ResultID: s.resultID(), Discarded: true, IsFinal: true})
		}
		s.cancel()
	case vllmEventError:
		providerError := parseVLLMRealtimeError(event)
		err := errors.Join(classifyVLLMRealtimeServerError(providerError), providerError)
		if !s.trySetWaitResult(err) {
			return
		}
		s.emit(ProviderStreamEvent{Err: err, ResultID: s.resultID(), IsFinal: true})
		s.signalReady(err)
		s.cancel()
	}
}

func (s *vllmRealtimeStream) resultID() string {
	return "vllm-" + s.request.SessionID
}

func (s *vllmRealtimeStream) signalReady(err error) {
	s.readyOnce.Do(func() { s.ready <- err })
}

func (s *vllmRealtimeStream) emit(event ProviderStreamEvent) {
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *vllmRealtimeStream) fail(err error) {
	if err == nil || !s.trySetWaitResult(err) {
		return
	}
	s.emit(ProviderStreamEvent{Err: err, ResultID: s.resultID(), IsFinal: true})
	s.signalReady(err)
	s.cancel()
}

func (s *vllmRealtimeStream) hasWaitResult() bool {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitResultSet
}

func (s *vllmRealtimeStream) setWaitResult(err error) {
	s.trySetWaitResult(err)
}

func (s *vllmRealtimeStream) trySetWaitResult(err error) bool {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	if s.waitResultSet {
		return false
	}
	s.waitErr = err
	s.waitResultSet = true
	return true
}

func (s *vllmRealtimeStream) currentWaitError() error {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitErr
}

func (s *vllmRealtimeStream) closeOnContext() {
	<-s.ctx.Done()
	_ = s.conn.Close()
}

func (s *vllmRealtimeStream) startFinishTimer() {
	s.finishTimer.Do(func() {
		go func() {
			timer := time.NewTimer(s.provider.cfg.FinishTimeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				s.fail(ErrRequestTimeout)
			case <-s.done:
			case <-s.ctx.Done():
			}
		}()
	})
}

func normalizeVLLMRealtimeConfig(cfg VLLMRealtimeConfig) (VLLMRealtimeConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultVLLMRealtimeName
	}
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultVLLMRealtimeEndpoint
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = defaultVLLMRealtimeModel
	}
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	if strings.ContainsAny(cfg.APIKey, "\r\n") {
		return cfg, ErrInvalidConfig
	}
	if _, err := vllmRealtimeEndpoint(cfg.Endpoint, cfg.AllowInsecureWebSocket); err != nil {
		return cfg, err
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultVLLMRealtimeHandshakeTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultVLLMRealtimeWriteTimeout
	}
	if cfg.FinishTimeout <= 0 {
		cfg.FinishTimeout = defaultVLLMRealtimeFinishTimeout
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultVLLMRealtimeEventBuffer
	}
	return cfg, nil
}

func normalizeVLLMStreamingRequest(request StreamingRequest) (StreamingRequest, error) {
	if request.SessionID == "" || request.SampleRate != vllmRealtimeSampleRate ||
		request.Channels != 1 || request.Format != AudioFormatRawPCM16 {
		return request, ErrInvalidRequest
	}
	language, err := NormalizeLanguageTag(request.Language)
	if err != nil {
		return request, err
	}
	languageHints, err := normalizeLanguageHints(request.LanguageHints)
	if err != nil {
		return request, err
	}
	request.Language = language
	request.LanguageHints = languageHints
	request.Context = cloneRecognitionContext(request.Context)
	request.ServerVAD = false
	return request, nil
}

func vllmRealtimeEndpoint(endpoint string, allowInsecure bool) (string, error) {
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
		path = vllmRealtimeEndpointPath
	}
	if path != vllmRealtimeEndpointPath {
		return "", ErrInvalidConfig
	}
	parsed.Path = path
	return parsed.String(), nil
}

func parseVLLMRealtimeError(event vllmRealtimeServerEvent) vllmRealtimeError {
	detail := vllmRealtimeError{Code: strings.TrimSpace(event.Code)}
	if len(event.Error) == 0 || string(event.Error) == "null" {
		return detail
	}
	if err := json.Unmarshal(event.Error, &detail.Message); err == nil {
		return detail
	}
	var object struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	if json.Unmarshal(event.Error, &object) == nil {
		if detail.Code == "" {
			detail.Code = strings.TrimSpace(object.Code)
		}
		detail.Message = strings.TrimSpace(object.Message)
		if detail.Message == "" {
			detail.Message = strings.TrimSpace(object.Error)
		}
	}
	return detail
}

func classifyVLLMRealtimeServerError(providerError vllmRealtimeError) error {
	code := strings.ToLower(strings.TrimSpace(providerError.Code))
	switch code {
	case eventErrorUnauthorized, "authentication_error", "permission_denied":
		return ErrUnauthorized
	case "rate_limit", "rate_limited", "resource_exhausted":
		return ErrRateLimited
	case "model_not_found":
		return ErrInvalidRequest
	case "invalid_event", "invalid_audio", "model_not_validated", "unknown_event":
		return ErrProviderRequest
	case "processing_error":
		return ErrProviderResponse
	default:
		return ErrProviderResponse
	}
}

func classifyVLLMRealtimeDialError(response *http.Response, err error) error {
	if response == nil {
		return errors.Join(ErrProviderUnavailable, err)
	}
	classified := classifyHTTPStatus(response.StatusCode)
	if classified == nil {
		classified = ErrProviderRequest
	}
	return errors.Join(classified, err)
}
