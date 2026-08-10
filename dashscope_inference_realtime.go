package asr

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

const (
	defaultDashScopeInferenceRealtimeName     = "dashscope-inference-realtime"
	defaultDashScopeInferenceRealtimeModel    = "qwen-audio-3.0-asr-flash-streaming"
	defaultDashScopeInferenceHandshakeTimeout = 10 * time.Second
	defaultDashScopeInferenceWriteTimeout     = 5 * time.Second
	defaultDashScopeInferenceFinishTimeout    = 20 * time.Second
	defaultDashScopeInferenceEventBuffer      = 128
	defaultDashScopeInferenceSentenceSilence  = 1300 * time.Millisecond
	defaultDashScopeInferenceVocabularyWeight = 5
	dashScopeInferencePath                    = "/api-ws/v1/inference"
	dashScopeActionRunTask                    = "run-task"
	dashScopeActionContinueTask               = "continue-task"
	dashScopeActionFinishTask                 = "finish-task"
	dashScopeEventTaskStarted                 = "task-started"
	dashScopeEventResultGenerated             = "result-generated"
	dashScopeEventTaskFinished                = "task-finished"
	dashScopeEventTaskFailed                  = "task-failed"
	dashScopeStreamingDuplex                  = "duplex"
	dashScopeTaskGroupAudio                   = "audio"
	dashScopeTaskASR                          = "asr"
	dashScopeFunctionRecognition              = "recognition"
	dashScopeAudioFormatPCM                   = "pcm"
	dashScopeContextRoleUser                  = "user"
	dashScopeContextInputText                 = "input_text"
	dashScopeMaxContextRunes                  = 400
	dashScopeMaxContextInputs                 = 5
	dashScopeMaxLanguageHints                 = 4
	dashScopeMaxSuperVocabulary               = 50
)

var dashScopeInferenceLanguages = map[string]struct{}{
	"ar": {}, "bg": {}, "cs": {}, "da": {}, "de": {}, "el": {}, "en": {}, "es": {},
	"fi": {}, "fr": {}, "hi": {}, "hr": {}, "hu": {}, "id": {}, "it": {}, "ja": {},
	"ko": {}, "ms": {}, "nl": {}, "no": {}, "pl": {}, "pt": {}, "ro": {}, "ru": {},
	"sk": {}, "sv": {}, "th": {}, "tl": {}, "vi": {}, "zh": {},
}

type DashScopeInferenceRealtimeConfig struct {
	Name                       string
	Model                      string
	Endpoint                   string
	APIKey                     string
	WorkspaceID                string
	UserAgent                  string
	VocabularyID               string
	VocabularyWeight           int
	SemanticPunctuationEnabled bool
	MaxSentenceSilence         time.Duration
	MultiThresholdModeEnabled  bool
	Heartbeat                  bool
	SpeechNoiseThreshold       *float64
	SpecialWordFilter          string
	HandshakeTimeout           time.Duration
	WriteTimeout               time.Duration
	FinishTimeout              time.Duration
	EventBuffer                int
	AllowInsecureWebSocket     bool
}

type DashScopeInferenceRealtimeProvider struct {
	cfg    DashScopeInferenceRealtimeConfig
	dialer websocket.Dialer
}

type dashScopeInferenceStream struct {
	provider *DashScopeInferenceRealtimeProvider
	request  StreamingRequest
	taskID   string
	conn     *websocket.Conn

	ctx    context.Context //nolint:containedctx // The stream owns the provider connection lifecycle.
	cancel context.CancelFunc

	events  chan ProviderStreamEvent
	done    chan struct{}
	started chan error

	writeMu     sync.Mutex
	resultMu    sync.Mutex
	closeOnce   sync.Once
	startedOnce sync.Once
	finishTimer sync.Once

	writeClosed   bool
	expectedSeq   uint64
	nextSample    int64
	waitErr       error
	waitResultSet bool
}

type dashScopeTaskHeader struct {
	Action    string `json:"action,omitempty"`
	TaskID    string `json:"task_id"`
	Streaming string `json:"streaming,omitempty"`
	Event     string `json:"event,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	ErrorText string `json:"error_message,omitempty"`
}

type dashScopeClientEvent struct {
	Header  dashScopeTaskHeader  `json:"header"`
	Payload dashScopeTaskPayload `json:"payload"`
}

type dashScopeTaskPayload struct {
	TaskGroup  string                   `json:"task_group,omitempty"`
	Task       string                   `json:"task,omitempty"`
	Function   string                   `json:"function,omitempty"`
	Model      string                   `json:"model,omitempty"`
	Parameters *dashScopeTaskParameters `json:"parameters,omitempty"`
	Input      dashScopeTaskInput       `json:"input"`
}

type dashScopeTaskParameters struct {
	Format                     string         `json:"format"`
	SampleRate                 int            `json:"sample_rate"`
	VocabularyID               string         `json:"vocabulary_id,omitempty"`
	Vocabulary                 map[string]int `json:"vocabulary,omitempty"`
	LanguageHints              []string       `json:"language_hints,omitempty"`
	SemanticPunctuationEnabled bool           `json:"semantic_punctuation_enabled,omitempty"`
	MaxSentenceSilence         int64          `json:"max_sentence_silence"`
	MultiThresholdModeEnabled  bool           `json:"multi_threshold_mode_enabled,omitempty"`
	Heartbeat                  bool           `json:"heartbeat,omitempty"`
	SpeechNoiseThreshold       *float64       `json:"speech_noise_threshold,omitempty"`
	SpecialWordFilter          string         `json:"special_word_filter,omitempty"`
}

type dashScopeTaskInput struct {
	Context []dashScopeContextMessage `json:"context,omitempty"`
}

type dashScopeContextMessage struct {
	Role    string                    `json:"role"`
	Content []dashScopeContextContent `json:"content"`
}

type dashScopeContextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type dashScopeServerEvent struct {
	Header  dashScopeTaskHeader    `json:"header"`
	Payload dashScopeServerPayload `json:"payload"`
}

type dashScopeServerPayload struct {
	Output dashScopeServerOutput `json:"output"`
}

type dashScopeServerOutput struct {
	Sentence *dashScopeSentence `json:"sentence,omitempty"`
}

type dashScopeSentence struct {
	BeginTime   int64           `json:"begin_time"`
	EndTime     int64           `json:"end_time"`
	Text        string          `json:"text"`
	Heartbeat   bool            `json:"heartbeat"`
	SentenceEnd bool            `json:"sentence_end"`
	SentenceID  int             `json:"sentence_id"`
	Words       []dashScopeWord `json:"words,omitempty"`
}

type dashScopeWord struct {
	BeginTime   int64  `json:"begin_time"`
	EndTime     int64  `json:"end_time"`
	Text        string `json:"text"`
	Punctuation string `json:"punctuation"`
}

type dashScopeProviderError struct {
	code    string
	message string
}

func (e dashScopeProviderError) Error() string {
	if e.code == "" {
		return "dashscope inference realtime provider error"
	}
	return "dashscope inference realtime provider error: " + e.code
}

func NewDashScopeInferenceRealtimeProvider(
	cfg DashScopeInferenceRealtimeConfig,
) (*DashScopeInferenceRealtimeProvider, error) {
	normalized, err := normalizeDashScopeInferenceRealtimeConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &DashScopeInferenceRealtimeProvider{
		cfg:    normalized,
		dialer: *websocket.DefaultDialer,
	}, nil
}

func (p *DashScopeInferenceRealtimeProvider) Name() string {
	if p == nil {
		return ""
	}
	return p.cfg.Name
}

func (p *DashScopeInferenceRealtimeProvider) Model() string {
	if p == nil {
		return ""
	}
	return p.cfg.Model
}

func (p *DashScopeInferenceRealtimeProvider) ServerVADEnabled() bool {
	return p != nil
}

func (p *DashScopeInferenceRealtimeProvider) StreamingCapabilities() StreamingCapabilities {
	return StreamingCapabilities{
		Formats:               []AudioFormat{AudioFormatRawPCM16},
		SampleRates:           []int{8000, 16000, 22050, 24000, 32000, 44100, 48000},
		SupportsPrompt:        true,
		SupportsTerms:         true,
		SupportsLanguageHints: true,
		SupportsServerVAD:     true,
		SupportsContextUpdate: true,
	}
}

func (p *DashScopeInferenceRealtimeProvider) Start(
	ctx context.Context,
	request StreamingRequest,
) (ProviderStream, error) {
	if p == nil || ctx == nil {
		return nil, ErrInvalidConfig
	}
	normalized, err := normalizeDashScopeInferenceRequest(request)
	if err != nil {
		return nil, err
	}
	taskID, err := newDashScopeTaskID()
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+p.cfg.APIKey)
	if p.cfg.UserAgent != "" {
		headers.Set("User-Agent", p.cfg.UserAgent)
	}
	if p.cfg.WorkspaceID != "" {
		headers.Set("X-DashScope-WorkSpace", p.cfg.WorkspaceID)
	}
	conn, response, err := p.dialer.DialContext(ctx, p.cfg.Endpoint, headers)
	if response != nil && response.Body != nil {
		_, _ = io.CopyN(io.Discard, response.Body, 4<<10)
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, classifyQwenDialError(response, err)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream := &dashScopeInferenceStream{
		provider: p,
		request:  normalized,
		taskID:   taskID,
		conn:     conn,
		ctx:      streamCtx,
		cancel:   cancel,
		events:   make(chan ProviderStreamEvent, p.cfg.EventBuffer),
		done:     make(chan struct{}),
		started:  make(chan error, 1),
	}
	go stream.readLoop()
	go stream.closeOnContext()
	if err := stream.sendRunTask(ctx); err != nil {
		stream.Close()
		return nil, err
	}
	timer := time.NewTimer(p.cfg.HandshakeTimeout)
	defer timer.Stop()
	select {
	case err := <-stream.started:
		if err != nil {
			stream.Close()
			return nil, err
		}
		return stream, nil
	case <-timer.C:
		err := errors.Join(ErrRequestTimeout, ErrProviderUnavailable)
		stream.fail(err)
		return nil, err
	case <-ctx.Done():
		stream.Close()
		return nil, errors.Join(ErrProviderUnavailable, ctx.Err())
	}
}

func (s *dashScopeInferenceStream) WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error {
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
	if err := s.writeMessage(ctx, websocket.BinaryMessage, chunk.Data); err != nil {
		s.fail(err)
		return err
	}
	s.expectedSeq++
	s.nextSample = chunk.EndSample
	return nil
}

func (s *dashScopeInferenceStream) CloseInput(ctx context.Context) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeClosed {
		return ErrSessionClosed
	}
	s.writeClosed = true
	event := dashScopeClientEvent{
		Header: dashScopeTaskHeader{
			Action:    dashScopeActionFinishTask,
			TaskID:    s.taskID,
			Streaming: dashScopeStreamingDuplex,
		},
		Payload: dashScopeTaskPayload{Input: dashScopeTaskInput{}},
	}
	if err := s.writeJSON(ctx, event); err != nil {
		s.fail(err)
		return err
	}
	s.startFinishTimer()
	return nil
}

func (s *dashScopeInferenceStream) UpdateContext(
	ctx context.Context,
	update StreamingContextUpdate,
) error {
	if s == nil {
		return ErrSessionClosed
	}
	input := dashScopeUpdatedInput(update)
	if len(input.Context) == 0 {
		return ErrInvalidRequest
	}
	event := dashScopeClientEvent{
		Header: dashScopeTaskHeader{
			Action:    dashScopeActionContinueTask,
			TaskID:    s.taskID,
			Streaming: dashScopeStreamingDuplex,
		},
		Payload: dashScopeTaskPayload{Input: input},
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeClosed {
		return ErrSessionClosed
	}
	if err := s.writeJSON(ctx, event); err != nil {
		s.fail(err)
		return err
	}
	return nil
}

func (s *dashScopeInferenceStream) Events() <-chan ProviderStreamEvent {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *dashScopeInferenceStream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *dashScopeInferenceStream) Wait(ctx context.Context) error {
	if s == nil {
		return ErrSessionClosed
	}
	select {
	case <-s.done:
		return s.currentWaitError()
	case <-ctx.Done():
		return errors.Join(ErrRequestTimeout, ctx.Err())
	}
}

func (s *dashScopeInferenceStream) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.conn.Close()
	})
}

func (s *dashScopeInferenceStream) sendRunTask(ctx context.Context) error {
	parameters := &dashScopeTaskParameters{
		Format:                     dashScopeAudioFormatPCM,
		SampleRate:                 s.request.SampleRate,
		VocabularyID:               s.provider.cfg.VocabularyID,
		Vocabulary:                 dashScopeVocabulary(s.request.Context.Terms, s.provider.cfg.VocabularyWeight),
		LanguageHints:              dashScopeLanguageHints(s.request),
		SemanticPunctuationEnabled: s.provider.cfg.SemanticPunctuationEnabled,
		MaxSentenceSilence:         s.provider.cfg.MaxSentenceSilence.Milliseconds(),
		MultiThresholdModeEnabled:  s.provider.cfg.MultiThresholdModeEnabled,
		Heartbeat:                  s.provider.cfg.Heartbeat,
		SpeechNoiseThreshold:       s.provider.cfg.SpeechNoiseThreshold,
		SpecialWordFilter:          s.provider.cfg.SpecialWordFilter,
	}
	event := dashScopeClientEvent{
		Header: dashScopeTaskHeader{
			Action:    dashScopeActionRunTask,
			TaskID:    s.taskID,
			Streaming: dashScopeStreamingDuplex,
		},
		Payload: dashScopeTaskPayload{
			TaskGroup:  dashScopeTaskGroupAudio,
			Task:       dashScopeTaskASR,
			Function:   dashScopeFunctionRecognition,
			Model:      s.provider.cfg.Model,
			Parameters: parameters,
			Input:      dashScopeInput(s.request.Context),
		},
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeJSON(ctx, event)
}

func (s *dashScopeInferenceStream) writeJSON(ctx context.Context, event dashScopeClientEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	return s.writeMessage(ctx, websocket.TextMessage, payload)
}

func (s *dashScopeInferenceStream) writeMessage(ctx context.Context, messageType int, payload []byte) error {
	deadline := time.Now().Add(s.provider.cfg.WriteTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	if err := s.conn.WriteMessage(messageType, payload); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	return nil
}

func (s *dashScopeInferenceStream) readLoop() {
	loop := realtimeJSONReadLoop[dashScopeServerEvent]{
		ctx:              s.ctx,
		conn:             s.conn,
		cancel:           s.cancel,
		events:           s.events,
		done:             s.done,
		hasWaitResult:    s.hasWaitResult,
		setWaitResult:    s.setWaitResult,
		currentWaitError: s.currentWaitError,
		signalUpdated:    s.signalStarted,
		emit:             s.emit,
		handleEvent:      s.handleEvent,
	}
	loop.run()
}

func (s *dashScopeInferenceStream) handleEvent(event dashScopeServerEvent) {
	if event.Header.TaskID != "" && event.Header.TaskID != s.taskID {
		err := errors.Join(ErrProviderResponse, dashScopeProviderError{code: "task_id_mismatch"})
		s.emit(ProviderStreamEvent{Err: err, IsFinal: true})
		s.setWaitResult(err)
		s.signalStarted(err)
		return
	}
	switch event.Header.Event {
	case dashScopeEventTaskStarted:
		s.signalStarted(nil)
	case dashScopeEventResultGenerated:
		s.handleResult(event.Payload.Output.Sentence)
	case dashScopeEventTaskFinished:
		s.setWaitResult(nil)
		s.signalStarted(nil)
	case dashScopeEventTaskFailed:
		err := classifyDashScopeTaskError(event.Header)
		s.emit(ProviderStreamEvent{Err: err, ResultID: s.taskID, IsFinal: true})
		s.setWaitResult(err)
		s.signalStarted(err)
	}
}

func (s *dashScopeInferenceStream) handleResult(sentence *dashScopeSentence) {
	if sentence == nil || sentence.Heartbeat {
		return
	}
	resultID := "sentence-" + strconv.Itoa(sentence.SentenceID)
	text := strings.TrimSpace(sentence.Text)
	if text == "" && sentence.SentenceEnd {
		s.emit(ProviderStreamEvent{ResultID: resultID, Discarded: true, IsFinal: true})
		return
	}
	if text == "" {
		return
	}
	event := ProviderStreamEvent{
		ResultID:      resultID,
		Text:          text,
		ConfirmedText: text,
		StartAt:       time.Duration(max(int64(0), sentence.BeginTime)) * time.Millisecond,
		EndAt:         time.Duration(max(int64(0), sentence.EndTime)) * time.Millisecond,
		Started:       true,
		IsFinal:       sentence.SentenceEnd,
	}
	s.emit(event)
}

func (s *dashScopeInferenceStream) emit(event ProviderStreamEvent) {
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}

func (s *dashScopeInferenceStream) signalStarted(err error) {
	s.startedOnce.Do(func() { s.started <- err })
}

func (s *dashScopeInferenceStream) setWaitResult(err error) {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	if s.waitResultSet {
		return
	}
	s.waitErr = err
	s.waitResultSet = true
}

func (s *dashScopeInferenceStream) hasWaitResult() bool {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitResultSet
}

func (s *dashScopeInferenceStream) currentWaitError() error {
	s.resultMu.Lock()
	defer s.resultMu.Unlock()
	return s.waitErr
}

func (s *dashScopeInferenceStream) fail(err error) {
	s.setWaitResult(err)
	s.signalStarted(err)
	_ = s.conn.Close()
}

func (s *dashScopeInferenceStream) closeOnContext() {
	select {
	case <-s.ctx.Done():
		_ = s.conn.Close()
	case <-s.done:
	}
}

func (s *dashScopeInferenceStream) startFinishTimer() {
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

func normalizeDashScopeInferenceRealtimeConfig(
	cfg DashScopeInferenceRealtimeConfig,
) (DashScopeInferenceRealtimeConfig, error) {
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		cfg.Name = defaultDashScopeInferenceRealtimeName
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model == "" {
		cfg.Model = defaultDashScopeInferenceRealtimeModel
	}
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.WorkspaceID = strings.TrimSpace(cfg.WorkspaceID)
	cfg.UserAgent = strings.TrimSpace(cfg.UserAgent)
	cfg.VocabularyID = strings.TrimSpace(cfg.VocabularyID)
	cfg.SpecialWordFilter = strings.TrimSpace(cfg.SpecialWordFilter)
	if cfg.Endpoint == "" || cfg.APIKey == "" ||
		strings.ContainsAny(cfg.APIKey+cfg.WorkspaceID+cfg.UserAgent, "\r\n") {
		return cfg, ErrInvalidConfig
	}
	parsed, err := url.Parse(cfg.Endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" ||
		parsed.Path != dashScopeInferencePath || parsed.RawQuery != "" ||
		(parsed.Scheme != webSocketSchemeSecure && parsed.Scheme != webSocketSchemeInsecure) {
		if err != nil {
			return cfg, errors.Join(ErrInvalidConfig, err)
		}
		return cfg, ErrInvalidConfig
	}
	if parsed.Scheme == webSocketSchemeInsecure && !cfg.AllowInsecureWebSocket &&
		!isLoopbackHost(parsed.Hostname()) {
		return cfg, ErrInvalidConfig
	}
	if cfg.VocabularyWeight == 0 {
		cfg.VocabularyWeight = defaultDashScopeInferenceVocabularyWeight
	}
	if !validDashScopeVocabularyWeight(cfg.VocabularyWeight) {
		return cfg, ErrInvalidConfig
	}
	if cfg.MaxSentenceSilence == 0 {
		cfg.MaxSentenceSilence = defaultDashScopeInferenceSentenceSilence
	}
	if cfg.MaxSentenceSilence < 200*time.Millisecond || cfg.MaxSentenceSilence > 6*time.Second {
		return cfg, ErrInvalidConfig
	}
	if cfg.SpeechNoiseThreshold != nil &&
		(*cfg.SpeechNoiseThreshold < -1 || *cfg.SpeechNoiseThreshold > 1) {
		return cfg, ErrInvalidConfig
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultDashScopeInferenceHandshakeTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultDashScopeInferenceWriteTimeout
	}
	if cfg.FinishTimeout <= 0 {
		cfg.FinishTimeout = defaultDashScopeInferenceFinishTimeout
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultDashScopeInferenceEventBuffer
	}
	return cfg, nil
}

func normalizeDashScopeInferenceRequest(request StreamingRequest) (StreamingRequest, error) {
	if request.SessionID == "" || request.Channels != 1 ||
		request.Format != AudioFormatRawPCM16 || request.SampleRate <= 0 {
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
	if _, err := normalizeDashScopeLanguageHints(request); err != nil {
		return request, err
	}
	return request, nil
}

func dashScopeLanguageHints(request StreamingRequest) []string {
	hints, _ := normalizeDashScopeLanguageHints(request)
	return hints
}

func normalizeDashScopeLanguageHints(request StreamingRequest) ([]string, error) {
	values := request.LanguageHints
	if request.Language != "" && request.Language != automaticLanguage {
		values = []string{request.Language}
	}
	seen := make(map[string]struct{}, len(values))
	hints := make([]string, 0, min(len(values), dashScopeMaxLanguageHints))
	for _, value := range values {
		primary := strings.ToLower(strings.SplitN(value, "-", 2)[0])
		if _, supported := dashScopeInferenceLanguages[primary]; !supported {
			return nil, ErrLanguageInvalid
		}
		if _, exists := seen[primary]; exists {
			continue
		}
		seen[primary] = struct{}{}
		hints = append(hints, primary)
		if len(hints) == dashScopeMaxLanguageHints {
			break
		}
	}
	return hints, nil
}

func dashScopeVocabulary(terms []string, weight int) map[string]int {
	limit := len(terms)
	if weight == 50 {
		limit = min(limit, dashScopeMaxSuperVocabulary)
	}
	vocabulary := make(map[string]int, limit)
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		vocabulary[term] = weight
		if len(vocabulary) == limit {
			break
		}
	}
	if len(vocabulary) == 0 {
		return nil
	}
	return vocabulary
}

func dashScopeInput(context RecognitionContext) dashScopeTaskInput {
	return dashScopeContextInput(context.Prompt, nil)
}

func dashScopeUpdatedInput(update StreamingContextUpdate) dashScopeTaskInput {
	return dashScopeContextInput(update.Context.Prompt, update.StableTranscripts)
}

func dashScopeContextInput(prompt string, stableTranscripts []string) dashScopeTaskInput {
	texts := dashScopeContextTexts(prompt, stableTranscripts)
	if len(texts) == 0 {
		return dashScopeTaskInput{}
	}
	messages := make([]dashScopeContextMessage, 0, len(texts))
	for _, text := range texts {
		messages = append(messages, dashScopeContextMessage{
			Role: dashScopeContextRoleUser,
			Content: []dashScopeContextContent{{
				Type: dashScopeContextInputText,
				Text: text,
			}},
		})
	}
	return dashScopeTaskInput{Context: messages}
}

func dashScopeContextTexts(prompt string, stableTranscripts []string) []string {
	prompt = strings.TrimSpace(prompt)
	historyLimit := dashScopeMaxContextInputs
	texts := make([]string, 0, historyLimit)
	remaining := dashScopeMaxContextRunes
	if prompt != "" {
		prompt = truncateRunes(prompt, remaining)
		texts = append(texts, prompt)
		remaining -= utf8.RuneCountInString(prompt)
		historyLimit--
	}
	history := make([]string, 0, min(len(stableTranscripts), historyLimit))
	for index := len(stableTranscripts) - 1; index >= 0 && len(history) < historyLimit && remaining > 0; index-- {
		text := strings.TrimSpace(stableTranscripts[index])
		if text == "" {
			continue
		}
		text = truncateRunes(text, remaining)
		history = append(history, text)
		remaining -= utf8.RuneCountInString(text)
	}
	for index := len(history) - 1; index >= 0; index-- {
		texts = append(texts, history[index])
	}
	return texts
}

func truncateRunes(value string, limit int) string {
	if limit <= 0 || utf8.RuneCountInString(value) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit])
}

func validDashScopeVocabularyWeight(value int) bool {
	return value >= 1 && value <= 5 || value == 50
}

func newDashScopeTaskID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", errors.Join(ErrInvalidConfig, err)
	}
	data[6] = data[6]&0x0f | 0x40
	data[8] = data[8]&0x3f | 0x80
	encoded := hex.EncodeToString(data)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" +
		encoded[16:20] + "-" + encoded[20:], nil
}

func classifyDashScopeTaskError(header dashScopeTaskHeader) error {
	providerErr := dashScopeProviderError{
		code:    strings.TrimSpace(header.ErrorCode),
		message: strings.TrimSpace(header.ErrorText),
	}
	value := strings.ToLower(providerErr.code + " " + providerErr.message)
	switch {
	case strings.Contains(value, "unauthor") || strings.Contains(value, "forbidden") ||
		strings.Contains(value, "permission"):
		return errors.Join(ErrUnauthorized, providerErr)
	case strings.Contains(value, "rate") || strings.Contains(value, "quota") ||
		strings.Contains(value, "limit"):
		return errors.Join(ErrRateLimited, providerErr)
	case strings.Contains(value, "timeout") || strings.Contains(value, "unavailable"):
		return errors.Join(ErrProviderUnavailable, providerErr)
	case strings.Contains(value, "overload") || strings.Contains(value, "busy"):
		return errors.Join(ErrOverloaded, providerErr)
	default:
		return errors.Join(ErrProviderResponse, providerErr)
	}
}

var (
	_ StreamingProvider      = (*DashScopeInferenceRealtimeProvider)(nil)
	_ ProviderStream         = (*dashScopeInferenceStream)(nil)
	_ ProviderContextUpdater = (*dashScopeInferenceStream)(nil)
)
