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
	"time"
	"unicode"

	"github.com/gorilla/websocket"
)

const (
	defaultInworldRealtimeName     = "inworld-realtime"
	defaultInworldRealtimeEndpoint = "wss://api.inworld.ai/stt/v1/transcribe:streamBidirectional"
	defaultInworldRealtimeModel    = "inworld/inworld-stt-1"
	defaultInworldVADThreshold     = 0.5
	defaultInworldHandshakeTimeout = 10 * time.Second
	defaultInworldWriteTimeout     = 5 * time.Second
	defaultInworldFinishTimeout    = 20 * time.Second
	defaultInworldEventBuffer      = 128
	inworldAuthorizationHeader     = "Authorization"
	inworldAudioEncodingLinear16   = "LINEAR16"
	inworldEndpointPath            = "/stt/v1/transcribe:streamBidirectional"
	inworldGRPCInvalidArgument     = 3
	inworldGRPCDeadlineExceeded    = 4
	inworldGRPCPermissionDenied    = 7
	inworldGRPCResourceExhausted   = 8
	inworldGRPCUnavailable         = 14
	inworldGRPCUnauthenticated     = 16
)

type InworldRealtimeConfig struct {
	Name                         string
	Endpoint                     string
	Model                        string
	APIKey                       string
	InactivityTimeout            time.Duration
	EndOfTurnConfidenceThreshold *float64
	VADThreshold                 *float64
	MinEndOfTurnSilence          time.Duration
	DisableServerVAD             bool
	IncludeWordTimestamps        bool
	DisablePartials              bool
	HandshakeTimeout             time.Duration
	WriteTimeout                 time.Duration
	FinishTimeout                time.Duration
	EventBuffer                  int
	AllowInsecureWebSocket       bool
}

type InworldRealtimeProvider struct {
	cfg    InworldRealtimeConfig
	dialer websocket.Dialer
}

type inworldRealtimeStream struct {
	provider *InworldRealtimeProvider
	request  StreamingRequest
	conn     *websocket.Conn

	ctx    context.Context //nolint:containedctx // The stream owns the provider connection lifecycle.
	cancel context.CancelFunc

	events chan ProviderStreamEvent
	done   chan struct{}

	writeMu     sync.Mutex
	stateMu     sync.Mutex
	resultMu    sync.Mutex
	closeOnce   sync.Once
	finishTimer sync.Once

	writeClosed     bool
	wroteAudio      bool
	closeStreamSent bool
	expectedSeq     uint64
	nextSample      int64
	closing         bool
	turnNumber      uint64
	turnID          string
	waitErr         error
	waitResultSet   bool
}

type inworldClientEvent struct {
	TranscribeConfig *inworldTranscribeConfig `json:"transcribeConfig,omitempty"`
	AudioChunk       *inworldAudioChunk       `json:"audioChunk,omitempty"`
	EndTurn          *struct{}                `json:"endTurn,omitempty"`
	CloseStream      *struct{}                `json:"closeStream,omitempty"`
}

type inworldTranscribeConfig struct {
	ModelID                      string              `json:"modelId"`
	AudioEncoding                string              `json:"audioEncoding"`
	Language                     string              `json:"language,omitempty"`
	SampleRateHertz              int                 `json:"sampleRateHertz"`
	NumberOfChannels             int                 `json:"numberOfChannels"`
	InactivityTimeoutSeconds     int64               `json:"inactivityTimeoutSeconds,omitempty"`
	EndOfTurnConfidenceThreshold *float64            `json:"endOfTurnConfidenceThreshold,omitempty"`
	Prompts                      []string            `json:"prompts,omitempty"`
	IncludeWordTimestamps        bool                `json:"includeWordTimestamps,omitempty"`
	InworldSTTV1Config           *inworldSTTV1Config `json:"inworldSttV1Config,omitempty"`
}

type inworldSTTV1Config struct {
	MinEndOfTurnSilenceWhenConfident int64   `json:"minEndOfTurnSilenceWhenConfident,omitempty"`
	VADThreshold                     float64 `json:"vadThreshold"`
}

type inworldAudioChunk struct {
	Content string `json:"content"`
}

type inworldServerEvent struct {
	Result        *inworldServerResult  `json:"result,omitempty"`
	Transcription *inworldTranscription `json:"transcription,omitempty"`
	SpeechStarted *inworldSpeechStarted `json:"speechStarted,omitempty"`
	SpeechStopped *inworldSpeechStopped `json:"speechStopped,omitempty"`
	Usage         *inworldUsage         `json:"usage,omitempty"`
	Error         *inworldServerError   `json:"error,omitempty"`
	Code          int                   `json:"code,omitempty"`
	Message       string                `json:"message,omitempty"`

	// Some gateways expose the message payload without its oneof wrapper.
	Transcript     string                 `json:"transcript,omitempty"`
	IsFinal        *bool                  `json:"isFinal,omitempty"`
	WordTimestamps []inworldWordTimestamp `json:"wordTimestamps,omitempty"`
}

type inworldServerResult struct {
	Transcription *inworldTranscription `json:"transcription,omitempty"`
	SpeechStarted *inworldSpeechStarted `json:"speechStarted,omitempty"`
	SpeechStopped *inworldSpeechStopped `json:"speechStopped,omitempty"`
	Usage         *inworldUsage         `json:"usage,omitempty"`
}

type inworldTranscription struct {
	Transcript     string                 `json:"transcript"`
	IsFinal        bool                   `json:"isFinal"`
	WordTimestamps []inworldWordTimestamp `json:"wordTimestamps"`
}

type inworldWordTimestamp struct {
	Word        string  `json:"word"`
	Confidence  float64 `json:"confidence"`
	StartTimeMS int64   `json:"startTimeMs"`
	EndTimeMS   int64   `json:"endTimeMs"`
}

type inworldSpeechStarted struct {
	StartTimeMS int64   `json:"startTimeMs"`
	Confidence  float64 `json:"confidence"`
}

type inworldSpeechStopped struct {
	SilenceDurationMS int64 `json:"silenceDurationMs"`
}

type inworldUsage struct {
	TranscribedAudioMS int64  `json:"transcribedAudioMs"`
	ModelID            string `json:"modelId"`
}

type inworldServerError struct {
	Code    int    `json:"code"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type inworldProviderError struct {
	code    int
	status  string
	message string
}

func (e inworldProviderError) Error() string {
	detail := strings.TrimSpace(e.status)
	if e.code != 0 {
		if detail != "" {
			detail += " "
		}
		detail += strconv.Itoa(e.code)
	}
	if message := inworldErrorDetail(e.message); message != "" {
		if detail != "" {
			detail += ": "
		}
		detail += message
	}
	if detail == "" {
		return "inworld realtime provider error"
	}
	return "inworld realtime provider error: " + detail
}

func inworldErrorDetail(detail string) string {
	detail = strings.Join(strings.Fields(detail), " ")
	runes := []rune(detail)
	if len(runes) > 512 {
		detail = string(runes[:512])
	}
	return detail
}

func NewInworldRealtimeProvider(cfg InworldRealtimeConfig) (*InworldRealtimeProvider, error) {
	normalized, err := normalizeInworldRealtimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = normalized.HandshakeTimeout
	return &InworldRealtimeProvider{cfg: normalized, dialer: dialer}, nil
}

func (p *InworldRealtimeProvider) Name() string { return p.cfg.Name }

func (p *InworldRealtimeProvider) Model() string { return p.cfg.Model }

func (p *InworldRealtimeProvider) ServerVADEnabled() bool {
	return p != nil && !p.cfg.DisableServerVAD
}

func (p *InworldRealtimeProvider) StreamingCapabilities() StreamingCapabilities {
	return StreamingCapabilities{
		Formats:               []AudioFormat{AudioFormatRawPCM16},
		SampleRates:           []int{8000, 16000, 22050, 24000, 44100, 48000},
		SupportsPrompt:        false,
		SupportsTerms:         true,
		SupportsLanguageHints: false,
		SupportsServerVAD:     true,
	}
}

func (p *InworldRealtimeProvider) Start(
	ctx context.Context,
	request StreamingRequest,
) (ProviderStream, error) {
	if p == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	normalized, err := normalizeInworldStreamingRequest(request)
	if err != nil {
		return nil, err
	}
	normalized.ServerVAD = p.ServerVADEnabled()
	endpoint, err := inworldRealtimeEndpoint(p.cfg.Endpoint, p.cfg.AllowInsecureWebSocket)
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set(inworldAuthorizationHeader, "Basic "+p.cfg.APIKey)
	conn, response, err := p.dialer.DialContext(ctx, endpoint, headers)
	if response != nil && response.Body != nil {
		_, _ = io.CopyN(io.Discard, response.Body, 4<<10)
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, classifyInworldDialError(response, err)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream := &inworldRealtimeStream{
		provider: p,
		request:  normalized,
		conn:     conn,
		ctx:      streamCtx,
		cancel:   cancel,
		events:   make(chan ProviderStreamEvent, p.cfg.EventBuffer),
		done:     make(chan struct{}),
	}
	if err := stream.writeConfig(ctx); err != nil {
		cancel()
		_ = conn.Close()
		return nil, err
	}
	go stream.readLoop()
	go stream.closeOnContext()
	return stream, nil
}

func (s *inworldRealtimeStream) WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error {
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
	event := inworldClientEvent{AudioChunk: &inworldAudioChunk{
		Content: base64.StdEncoding.EncodeToString(chunk.Data),
	}}
	if err := s.writeEvent(ctx, event); err != nil {
		s.fail(err)
		return err
	}
	s.wroteAudio = true
	s.expectedSeq++
	s.nextSample = chunk.EndSample
	return nil
}

func (s *inworldRealtimeStream) CloseInput(ctx context.Context) error {
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
		if err := s.writeCloseStreamLocked(ctx); err != nil {
			s.fail(err)
			return err
		}
		s.complete()
		return nil
	}
	if err := s.writeEvent(ctx, inworldClientEvent{EndTurn: &struct{}{}}); err != nil {
		s.fail(err)
		return err
	}
	s.startFinishTimer()
	return nil
}

func (s *inworldRealtimeStream) Events() <-chan ProviderStreamEvent {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *inworldRealtimeStream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *inworldRealtimeStream) Wait(ctx context.Context) error {
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

func (s *inworldRealtimeStream) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.conn.Close()
	})
}

func (s *inworldRealtimeStream) writeConfig(ctx context.Context) error {
	config := &inworldTranscribeConfig{
		ModelID:                      s.provider.cfg.Model,
		AudioEncoding:                inworldAudioEncodingLinear16,
		Language:                     inworldRequestLanguage(s.request),
		SampleRateHertz:              s.request.SampleRate,
		NumberOfChannels:             s.request.Channels,
		EndOfTurnConfidenceThreshold: s.provider.cfg.EndOfTurnConfidenceThreshold,
		Prompts:                      inworldPrompts(s.request.Context),
		IncludeWordTimestamps:        s.provider.cfg.IncludeWordTimestamps,
	}
	if s.provider.cfg.InactivityTimeout > 0 {
		config.InactivityTimeoutSeconds = int64(s.provider.cfg.InactivityTimeout / time.Second)
	}
	if s.provider.cfg.Model == defaultInworldRealtimeModel {
		threshold := *s.provider.cfg.VADThreshold
		if s.provider.cfg.DisableServerVAD {
			threshold = 0
		}
		config.InworldSTTV1Config = &inworldSTTV1Config{
			MinEndOfTurnSilenceWhenConfident: s.provider.cfg.MinEndOfTurnSilence.Milliseconds(),
			VADThreshold:                     threshold,
		}
	}
	return s.writeEvent(ctx, inworldClientEvent{TranscribeConfig: config})
}

func (s *inworldRealtimeStream) writeEvent(ctx context.Context, event inworldClientEvent) error {
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

func (s *inworldRealtimeStream) readLoop() {
	realtimeJSONReadLoop[inworldServerEvent]{
		ctx:               s.ctx,
		conn:              s.conn,
		cancel:            s.cancel,
		events:            s.events,
		done:              s.done,
		hasWaitResult:     s.hasWaitResult,
		setWaitResult:     s.setWaitResult,
		currentWaitError:  s.currentWaitError,
		signalUpdated:     func(error) {},
		emit:              s.emit,
		handleEvent:       s.handleServerEvent,
		classifyReadError: s.classifyReadError,
	}.run()
}

func (s *inworldRealtimeStream) handleServerEvent(event inworldServerEvent) {
	if event.Error != nil {
		s.handleProviderError(*event.Error)
		return
	}
	if event.Code != 0 || strings.TrimSpace(event.Message) != "" {
		s.handleProviderError(inworldServerError{Code: event.Code, Message: event.Message})
		return
	}
	transcription := event.Transcription
	if event.Result != nil && event.Result.Transcription != nil {
		transcription = event.Result.Transcription
	}
	if transcription == nil && event.IsFinal != nil {
		transcription = &inworldTranscription{
			Transcript:     event.Transcript,
			IsFinal:        *event.IsFinal,
			WordTimestamps: event.WordTimestamps,
		}
	}
	if transcription != nil {
		s.handleTranscription(*transcription)
	}
}

func (s *inworldRealtimeStream) handleTranscription(transcription inworldTranscription) {
	text := strings.TrimSpace(transcription.Transcript)
	s.stateMu.Lock()
	resultID := s.turnID
	started := false
	discardedResultID := ""
	if text != "" {
		resultID, started = s.currentResultLocked()
	}
	if transcription.IsFinal {
		if text == "" && s.turnID != "" {
			discardedResultID = s.turnID
		}
		s.turnID = ""
	}
	s.stateMu.Unlock()
	if started {
		s.emit(ProviderStreamEvent{ResultID: resultID, Started: true})
	}
	if discardedResultID != "" {
		s.emit(ProviderStreamEvent{ResultID: discardedResultID, Discarded: true})
	}
	if text != "" && (transcription.IsFinal || !s.provider.cfg.DisablePartials) {
		startAt, endAt := inworldWordRange(transcription.WordTimestamps)
		event := ProviderStreamEvent{
			ResultID: resultID,
			Text:     text,
			StartAt:  startAt,
			EndAt:    endAt,
			IsFinal:  transcription.IsFinal,
		}
		if transcription.IsFinal {
			event.ConfirmedText = text
		}
		s.emit(event)
	}
	if transcription.IsFinal && s.isClosing() {
		s.finishAfterFinal()
	}
}

func (s *inworldRealtimeStream) handleProviderError(serverError inworldServerError) {
	providerErr := errors.Join(
		classifyInworldServerError(serverError),
		inworldProviderError{
			code: serverError.Code, status: serverError.Status, message: serverError.Message,
		},
	)
	s.emit(ProviderStreamEvent{Err: providerErr, IsFinal: true})
	s.setWaitResult(providerErr)
	_ = s.conn.Close()
}

func (s *inworldRealtimeStream) currentResultLocked() (string, bool) {
	if s.turnID != "" {
		return s.turnID, false
	}
	s.turnNumber++
	s.turnID = s.request.SessionID + "_turn_" + qwenSequence(s.turnNumber)
	return s.turnID, true
}

func (s *inworldRealtimeStream) classifyReadError(err error) error {
	if s.isClosing() && websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		return nil
	}
	if s.ctx.Err() != nil {
		return errors.Join(ErrSessionClosed, s.ctx.Err())
	}
	return errors.Join(ErrProviderUnavailable, err)
}

func (s *inworldRealtimeStream) isClosing() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.closing
}

func (s *inworldRealtimeStream) emit(event ProviderStreamEvent) {
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *inworldRealtimeStream) setWaitResult(err error) {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	if s.waitResultSet {
		return
	}
	s.waitErr = err
	s.waitResultSet = true
}

func (s *inworldRealtimeStream) hasWaitResult() bool {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitResultSet
}

func (s *inworldRealtimeStream) currentWaitError() error {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitErr
}

func (s *inworldRealtimeStream) fail(err error) {
	s.setWaitResult(err)
	_ = s.conn.Close()
}

func (s *inworldRealtimeStream) complete() {
	s.setWaitResult(nil)
	_ = s.conn.Close()
}

func (s *inworldRealtimeStream) finishAfterFinal() {
	s.writeMu.Lock()
	err := s.writeCloseStreamLocked(s.ctx)
	s.writeMu.Unlock()
	if err != nil {
		s.fail(err)
		return
	}
	s.complete()
}

func (s *inworldRealtimeStream) writeCloseStreamLocked(ctx context.Context) error {
	if s.closeStreamSent {
		return nil
	}
	if err := s.writeEvent(ctx, inworldClientEvent{CloseStream: &struct{}{}}); err != nil {
		return err
	}
	s.closeStreamSent = true
	return nil
}

func (s *inworldRealtimeStream) closeOnContext() {
	select {
	case <-s.ctx.Done():
		_ = s.conn.Close()
	case <-s.done:
	}
}

func (s *inworldRealtimeStream) startFinishTimer() {
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

func normalizeInworldRealtimeConfig(cfg InworldRealtimeConfig) (InworldRealtimeConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultInworldRealtimeName
	}
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultInworldRealtimeEndpoint
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = defaultInworldRealtimeModel
	}
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	if cfg.APIKey == "" || strings.ContainsAny(cfg.APIKey, "\r\n") {
		return cfg, ErrInvalidConfig
	}
	if _, err := inworldRealtimeEndpoint(cfg.Endpoint, cfg.AllowInsecureWebSocket); err != nil {
		return cfg, err
	}
	if cfg.EndOfTurnConfidenceThreshold != nil {
		threshold := *cfg.EndOfTurnConfidenceThreshold
		if threshold < 0 || threshold > 1 {
			return cfg, ErrInvalidConfig
		}
		cfg.EndOfTurnConfidenceThreshold = &threshold
	}
	if cfg.VADThreshold == nil {
		threshold := defaultInworldVADThreshold
		cfg.VADThreshold = &threshold
	} else {
		threshold := *cfg.VADThreshold
		if threshold < 0 || threshold > 1 {
			return cfg, ErrInvalidConfig
		}
		cfg.VADThreshold = &threshold
	}
	if cfg.DisableServerVAD && cfg.Model != defaultInworldRealtimeModel {
		return cfg, ErrInvalidConfig
	}
	if cfg.InactivityTimeout < 0 ||
		(cfg.InactivityTimeout > 0 && cfg.InactivityTimeout%time.Second != 0) ||
		cfg.MinEndOfTurnSilence < 0 ||
		(cfg.MinEndOfTurnSilence > 0 && cfg.MinEndOfTurnSilence%time.Millisecond != 0) {
		return cfg, ErrInvalidConfig
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultInworldHandshakeTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultInworldWriteTimeout
	}
	if cfg.FinishTimeout <= 0 {
		cfg.FinishTimeout = defaultInworldFinishTimeout
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultInworldEventBuffer
	}
	return cfg, nil
}

func normalizeInworldStreamingRequest(request StreamingRequest) (StreamingRequest, error) {
	if request.SessionID == "" || request.Channels != 1 || request.Format != AudioFormatRawPCM16 ||
		!inworldSampleRateSupported(request.SampleRate) {
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
	if _, err := normalizeInworldPrompts(request.Context); err != nil {
		return request, err
	}
	return request, nil
}

func inworldRealtimeEndpoint(endpoint string, allowInsecure bool) (string, error) {
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
		path = inworldEndpointPath
	}
	if path != inworldEndpointPath {
		return "", ErrInvalidConfig
	}
	parsed.Path = path
	return parsed.String(), nil
}

func inworldSampleRateSupported(sampleRate int) bool {
	switch sampleRate {
	case 8000, 16000, 22050, 24000, 44100, 48000:
		return true
	default:
		return false
	}
}

func inworldRequestLanguage(request StreamingRequest) string {
	language := request.Language
	if language == "" || language == automaticLanguage {
		return ""
	}
	return openAIRealtimeLanguage(language)
}

func inworldPrompts(context RecognitionContext) []string {
	prompts, _ := normalizeInworldPrompts(context)
	return prompts
}

func normalizeInworldPrompts(context RecognitionContext) ([]string, error) {
	candidates := context.Terms
	seen := make(map[string]struct{}, len(candidates))
	prompts := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		for _, character := range candidate {
			if unicode.IsControl(character) || strings.ContainsRune("#/@|", character) {
				return nil, ErrInvalidRequest
			}
		}
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		prompts = append(prompts, candidate)
	}
	return prompts, nil
}

func inworldWordRange(words []inworldWordTimestamp) (time.Duration, time.Duration) {
	var start time.Duration
	var end time.Duration
	found := false
	for _, word := range words {
		if word.StartTimeMS < 0 || word.EndTimeMS < word.StartTimeMS {
			continue
		}
		wordStart := time.Duration(word.StartTimeMS) * time.Millisecond
		wordEnd := time.Duration(word.EndTimeMS) * time.Millisecond
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

func classifyInworldDialError(response *http.Response, err error) error {
	if response == nil {
		return errors.Join(ErrProviderUnavailable, err)
	}
	classified := classifyHTTPStatus(response.StatusCode)
	if classified == nil {
		classified = ErrProviderRequest
	}
	return errors.Join(classified, err)
}

func classifyInworldServerError(serverError inworldServerError) error {
	status := strings.ToUpper(strings.TrimSpace(serverError.Status))
	switch {
	case serverError.Code == inworldGRPCUnauthenticated,
		serverError.Code == inworldGRPCPermissionDenied,
		strings.Contains(status, "UNAUTHENTICATED"), strings.Contains(status, "PERMISSION_DENIED"):
		return ErrUnauthorized
	case serverError.Code == inworldGRPCResourceExhausted, strings.Contains(status, "RESOURCE_EXHAUSTED"):
		return ErrRateLimited
	case serverError.Code == inworldGRPCDeadlineExceeded, strings.Contains(status, "DEADLINE_EXCEEDED"):
		return ErrRequestTimeout
	case serverError.Code == inworldGRPCUnavailable, strings.Contains(status, "UNAVAILABLE"):
		return ErrProviderUnavailable
	case serverError.Code == inworldGRPCInvalidArgument, strings.Contains(status, "INVALID_ARGUMENT"):
		return ErrProviderRequest
	default:
		return ErrProviderResponse
	}
}

var (
	_ StreamingProvider = (*InworldRealtimeProvider)(nil)
	_ ProviderStream    = (*inworldRealtimeStream)(nil)
)
