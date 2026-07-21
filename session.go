package asr

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultSessionEventBuffer      = 64
	defaultContextSilence          = 200 * time.Millisecond
	defaultTailFinalizeSilence     = 900 * time.Millisecond
	defaultTailFinalizeResultWait  = 20 * time.Second
	defaultMaxWindowDuration       = 30 * time.Second
	tailFinalizationTimeoutMessage = "ASR tail finalization timed out"
	eventErrorUnauthorized         = "unauthorized"
)

type SessionConfig struct {
	SessionID                string
	SegmentStrategy          SegmentRecognitionStrategy
	Language                 string
	LanguageHints            []string
	Context                  RecognitionContext
	SampleRate               int
	Channels                 int
	EventBuffer              int
	ContextSilence           time.Duration
	TailFinalizeSilence      time.Duration
	TailFinalizeResultWait   time.Duration
	ShortSegmentMaxDuration  time.Duration
	ShortSegmentNeighborWait time.Duration
	MaxWindowDuration        time.Duration
	TailAnchorEnabled        bool
}

type Session struct {
	recognizer Recognizer
	aligner    *Aligner
	cfg        SessionConfig

	ctx       context.Context //nolint:containedctx // Session owns this internal lifecycle context and exposes Close.
	cancel    context.CancelFunc
	commands  chan any
	results   chan windowCompletion
	events    chan Event
	completed chan struct{}
	done      chan struct{}
	wg        sync.WaitGroup
	complete  sync.Once
}

type addSegmentCommand struct {
	segment Segment
	result  chan error
}

type speechStartedCommand struct{}

type speechSplitCommand struct{}

type speechEndedCommand struct {
	streamDuration float64
}

type stopSessionCommand struct {
	result chan error
}

type tailDeadlineCommand struct {
	chainID    int
	generation uint64
	reason     FinalizationReason
}

type tailResultTimeoutCommand struct {
	chainID    int
	generation uint64
}

type windowKind string

const (
	windowKindRegular    windowKind = "regular"
	windowKindTailAnchor windowKind = "tail_anchor"
	windowKindFallback   windowKind = "segment_fallback"
	windowKindDirect     windowKind = "segment_direct"
)

type windowTask struct {
	chainID     int
	localIndex  int
	globalIndex int
	requestID   string
	kind        windowKind
	segments    []Segment
}

type windowCompletion struct {
	task   windowTask
	result TranscriptionResult
	err    error
}

type windowEvidence struct {
	task   windowTask
	result WindowResult
}

type chainState struct {
	id                  int
	segments            []Segment
	windows             map[int]windowEvidence
	segmentResults      map[int]SegmentResult
	fallbackSubmitted   map[int]struct{}
	tailAnchor          *windowEvidence
	tailTimer           *time.Timer
	tailResultTimer     *time.Timer
	tailGeneration      uint64
	pendingTasks        int
	tailRequested       bool
	waitingForNeighbor  bool
	tailAnchorSubmitted bool
	sealed              bool
	finalizationReason  FinalizationReason
}

type sessionActor struct {
	session          *Session
	current          *chainState
	chains           map[int]*chainState
	nextChainID      int
	nextWindowIndex  int
	lastSegmentIndex int
	sequence         uint64
	hasSegment       bool
	stopping         bool
	completed        bool
}

func NewSession(recognizer Recognizer, aligner *Aligner, cfg SessionConfig) (*Session, error) {
	if recognizer == nil || recognizer.ProviderName() == "" || recognizer.ProviderModel() == "" {
		return nil, ErrInvalidConfig
	}
	normalized, err := normalizeSessionConfig(cfg)
	if err != nil {
		return nil, err
	}
	if aligner == nil {
		aligner = NewAligner(nil, AlignmentConfig{})
	}
	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		recognizer: recognizer,
		aligner:    aligner,
		cfg:        normalized,
		ctx:        ctx,
		cancel:     cancel,
		commands:   make(chan any),
		results:    make(chan windowCompletion, normalized.EventBuffer),
		events:     make(chan Event, normalized.EventBuffer),
		completed:  make(chan struct{}),
		done:       make(chan struct{}),
	}
	go session.run()
	return session, nil
}

func (s *Session) Events() <-chan Event {
	if s == nil {
		return nil
	}
	return s.events
}

// Done is closed when the session has produced its terminal completion state.
// It is independent from whether a caller is currently consuming Events.
func (s *Session) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.completed
}

// Wait waits for terminal ASR completion without consuming the event stream.
func (s *Session) Wait(ctx context.Context) error {
	if s == nil {
		return ErrSessionClosed
	}
	select {
	case <-s.completed:
		return nil
	case <-ctx.Done():
		return errors.Join(ErrSessionClosed, ctx.Err())
	case <-s.done:
		select {
		case <-s.completed:
			return nil
		default:
			return ErrSessionClosed
		}
	}
}

func (s *Session) AddSegment(ctx context.Context, segment Segment) error {
	if s == nil {
		return ErrSessionClosed
	}
	command := addSegmentCommand{segment: cloneSegment(segment), result: make(chan error, 1)}
	if err := s.sendCommand(ctx, command); err != nil {
		return err
	}
	select {
	case err := <-command.result:
		return err
	case <-ctx.Done():
		return errors.Join(ErrSessionClosed, ctx.Err())
	case <-s.done:
		return ErrSessionClosed
	}
}

func (s *Session) SpeechStarted(ctx context.Context) error {
	return s.sendCommand(ctx, speechStartedCommand{})
}

// SpeechSplit finalizes the latest submitted segment and starts a new evidence
// chain for speech that continues after an internally promoted boundary. Unlike
// SpeechStarted, it never keeps a short segment open for neighboring context.
func (s *Session) SpeechSplit(ctx context.Context) error {
	return s.sendCommand(ctx, speechSplitCommand{})
}

// SpeechEnded rearms tail finalization for the latest submitted segment. It is
// useful when a long physical utterance has been split into multiple ASR
// segments before the upstream VAD emits its final speech_end event.
func (s *Session) SpeechEnded(ctx context.Context, streamDuration float64) error {
	if streamDuration < 0 {
		return ErrSegmentInvalid
	}
	return s.sendCommand(ctx, speechEndedCommand{streamDuration: streamDuration})
}

func (s *Session) Stop(ctx context.Context) error {
	if s == nil {
		return ErrSessionClosed
	}
	command := stopSessionCommand{result: make(chan error, 1)}
	if err := s.sendCommand(ctx, command); err != nil {
		return err
	}
	select {
	case err := <-command.result:
		return err
	case <-ctx.Done():
		return errors.Join(ErrSessionClosed, ctx.Err())
	case <-s.done:
		return ErrSessionClosed
	}
}

func (s *Session) Close() {
	if s == nil {
		return
	}
	s.cancel()
	s.wg.Wait()
	<-s.done
}

func (s *Session) sendCommand(ctx context.Context, command any) error {
	select {
	case s.commands <- command:
		return nil
	case <-ctx.Done():
		return errors.Join(ErrSessionClosed, ctx.Err())
	case <-s.done:
		return ErrSessionClosed
	}
}

func (s *Session) run() {
	actor := &sessionActor{session: s, chains: make(map[int]*chainState)}
	actor.emit(Event{Type: EventSessionReady, Provider: s.recognizer.ProviderName(), Model: s.recognizer.ProviderModel()})
	defer func() {
		for _, chain := range actor.chains {
			actor.stopChainTimers(chain)
		}
		close(s.events)
		close(s.done)
	}()
	for {
		select {
		case <-s.ctx.Done():
			return
		case command := <-s.commands:
			actor.handleCommand(command)
		case completion := <-s.results:
			actor.handleWindowCompletion(completion)
		}
	}
}

func (a *sessionActor) handleCommand(command any) {
	switch value := command.(type) {
	case addSegmentCommand:
		value.result <- a.addSegment(value.segment)
	case speechStartedCommand:
		a.speechStarted()
	case speechSplitCommand:
		a.speechSplit()
	case speechEndedCommand:
		a.speechEnded(value.streamDuration)
	case stopSessionCommand:
		value.result <- a.stop()
	case tailDeadlineCommand:
		a.requestTailFinalization(value.chainID, value.generation, value.reason)
	case tailResultTimeoutCommand:
		a.forceTailFinalization(value.chainID, value.generation)
	}
}

func (a *sessionActor) addSegment(segment Segment) error {
	if a.stopping {
		return ErrSessionClosed
	}
	if err := validateSegment(a.session.cfg, segment); err != nil {
		return err
	}
	if a.hasSegment && segment.Index <= a.lastSegmentIndex {
		return ErrSegmentInvalid
	}
	if a.session.cfg.SegmentStrategy == SegmentRecognitionStrategySingle {
		return a.addDirectSegment(segment)
	}
	if a.current == nil || a.current.sealed {
		a.current = a.newChain()
	}
	chain := a.current
	localIndex := len(chain.segments) + 1
	windowSegments := []Segment{segment}
	if localIndex > 1 {
		windowSegments = []Segment{chain.segments[localIndex-2], segment}
	}
	if windowDuration(windowSegments, a.session.cfg.ContextSilence) > a.session.cfg.MaxWindowDuration {
		return errors.Join(ErrSegmentInvalid, ErrWindowTooLong)
	}
	chain.segments = append(chain.segments, cloneSegment(segment))
	a.lastSegmentIndex = segment.Index
	a.hasSegment = true
	a.nextWindowIndex++
	task := windowTask{
		chainID:     chain.id,
		localIndex:  localIndex,
		globalIndex: a.nextWindowIndex,
		requestID:   a.windowRequestID(chain.id, a.nextWindowIndex, windowSegments),
		kind:        windowKindRegular,
		segments:    cloneSegments(windowSegments),
	}
	if localIndex > 1 {
		chain.segments[localIndex-2].Samples = nil
	}
	a.submit(task)
	a.scheduleTail(chain, segment)
	return nil
}

func (a *sessionActor) addDirectSegment(segment Segment) error {
	if windowDuration([]Segment{segment}, 0) > a.session.cfg.MaxWindowDuration {
		return errors.Join(ErrSegmentInvalid, ErrWindowTooLong)
	}
	chain := a.newChain()
	chain.segments = append(chain.segments, cloneSegment(segment))
	a.lastSegmentIndex = segment.Index
	a.hasSegment = true
	a.nextWindowIndex++
	a.submit(windowTask{
		chainID:     chain.id,
		localIndex:  1,
		globalIndex: a.nextWindowIndex,
		requestID:   a.session.cfg.SessionID + ":segment:" + strconv.Itoa(segment.Index),
		kind:        windowKindDirect,
		segments:    []Segment{cloneSegment(segment)},
	})
	return nil
}

func (a *sessionActor) speechStarted() {
	if a.session.cfg.SegmentStrategy == SegmentRecognitionStrategySingle {
		return
	}
	if a.current == nil || a.current.sealed {
		return
	}
	chain := a.current
	if chain.tailRequested {
		a.forceTailFinalization(chain.id, chain.tailGeneration)
		a.current = nil
		return
	}
	if !chain.waitingForNeighbor {
		a.requestTailFinalization(chain.id, chain.tailGeneration, FinalizationLongSegment)
		a.current = nil
		return
	}
	a.stopTailTimers(chain)
	chain.tailGeneration++
	chain.tailRequested = false
	chain.waitingForNeighbor = false
	chain.finalizationReason = ""
}

func (a *sessionActor) speechSplit() {
	if a.session.cfg.SegmentStrategy == SegmentRecognitionStrategySingle {
		return
	}
	if a.current == nil || a.current.sealed {
		return
	}
	chain := a.current
	if chain.tailRequested {
		a.forceTailFinalization(chain.id, chain.tailGeneration)
	} else {
		a.requestTailFinalization(chain.id, chain.tailGeneration, FinalizationLongSpeech)
	}
	a.current = nil
}

func (a *sessionActor) speechEnded(streamDuration float64) {
	if a.session.cfg.SegmentStrategy == SegmentRecognitionStrategySingle {
		return
	}
	if a.current == nil || a.current.sealed || len(a.current.segments) == 0 {
		return
	}
	latest := cloneSegment(a.current.segments[len(a.current.segments)-1])
	latest.StreamDuration = max(latest.EndAt, streamDuration)
	a.scheduleTail(a.current, latest)
}

func (a *sessionActor) stop() error {
	if a.stopping {
		return nil
	}
	a.stopping = true
	if a.session.cfg.SegmentStrategy == SegmentRecognitionStrategySingle {
		a.maybeComplete()
		return nil
	}
	for _, chain := range a.chains {
		if chain.sealed || len(chain.segments) == 0 {
			continue
		}
		if chain.tailRequested {
			continue
		}
		chain.tailGeneration++
		a.requestTailFinalization(chain.id, chain.tailGeneration, FinalizationAudioStop)
	}
	a.maybeComplete()
	return nil
}

func (a *sessionActor) newChain() *chainState {
	if a.chains == nil {
		a.chains = make(map[int]*chainState)
	}
	a.nextChainID++
	chain := &chainState{
		id:                a.nextChainID,
		windows:           make(map[int]windowEvidence),
		segmentResults:    make(map[int]SegmentResult),
		fallbackSubmitted: make(map[int]struct{}),
	}
	a.chains[chain.id] = chain
	return chain
}

func (a *sessionActor) submit(task windowTask) {
	if chain := a.chainByID(task.chainID); chain != nil {
		chain.pendingTasks++
	}
	a.session.wg.Add(1)
	go func() {
		defer a.session.wg.Done()
		samples := composeWindowSamples(task.segments, a.session.cfg.SampleRate, a.session.cfg.ContextSilence)
		result, err := a.session.recognizer.Transcribe(a.session.ctx, TranscriptionRequest{
			RequestID:     task.requestID,
			SessionID:     a.session.cfg.SessionID,
			Language:      a.session.cfg.Language,
			LanguageHints: append([]string(nil), a.session.cfg.LanguageHints...),
			Context:       cloneRecognitionContext(a.session.cfg.Context),
			Samples:       samples,
			AudioEndAt:    task.segments[len(task.segments)-1].EndAt,
			SampleRate:    a.session.cfg.SampleRate,
			Channels:      a.session.cfg.Channels,
			Authoritative: true,
		})
		completion := windowCompletion{task: task, result: result, err: err}
		select {
		case a.session.results <- completion:
		case <-a.session.ctx.Done():
		}
	}()
}

func (a *sessionActor) handleWindowCompletion(completion windowCompletion) {
	chain := a.chainByID(completion.task.chainID)
	if chain == nil || chain.sealed {
		return
	}
	chain.pendingTasks = max(0, chain.pendingTasks-1)
	if completion.task.kind == windowKindDirect {
		a.handleDirectCompletion(chain, completion)
		return
	}
	if completion.err != nil {
		if errors.Is(completion.err, ErrRequestSuperseded) {
			a.handleFailedTailTask(chain)
			return
		}
		a.emit(Event{
			Type: EventRecognitionError,
			Error: &EventError{
				Code:      classifyEventError(completion.err),
				Message:   "ASR request failed",
				RequestID: completion.task.requestID,
			},
		})
		a.handleFailedTailTask(chain)
		return
	}
	evidence := windowEvidence{
		task: taskForEvidence(completion.task),
		result: WindowResult{
			RequestID:        completion.task.requestID,
			WindowIndex:      completion.task.globalIndex,
			Segments:         segmentRefs(completion.task.segments),
			Text:             strings.TrimSpace(completion.result.Text),
			DetectedLanguage: completion.result.DetectedLanguage,
			Provider:         completion.result.Provider,
			Model:            completion.result.Model,
			Words:            append([]Word(nil), completion.result.Words...),
		},
	}
	if completion.task.kind == windowKindFallback {
		a.publishFallbackSegment(chain, evidence)
		if chain.tailRequested {
			a.tryFinalizeTail(chain)
		}
		return
	}
	if completion.task.kind == windowKindTailAnchor {
		chain.tailAnchor = &evidence
		a.tryFinalizeTail(chain)
		return
	}
	chain.windows[completion.task.localIndex] = evidence
	if completion.task.localIndex == 1 {
		a.publishPreview(chain, evidence)
	} else {
		window := evidence.result
		a.emit(Event{Type: EventWindowResult, Window: &window})
	}
	if completion.task.localIndex >= 3 {
		a.reconcileAdjacentWindows(chain, completion.task.localIndex-1, completion.task.localIndex)
	}
	if completion.task.localIndex >= 2 {
		a.reconcileAdjacentWindows(chain, completion.task.localIndex, completion.task.localIndex+1)
	}
	if chain.tailRequested {
		a.tryFinalizeTail(chain)
	}
}

func (a *sessionActor) handleDirectCompletion(chain *chainState, completion windowCompletion) {
	if chain == nil || len(completion.task.segments) != 1 {
		return
	}
	segment := completion.task.segments[0]
	if completion.err != nil {
		a.emit(Event{
			Type: EventRecognitionError,
			Error: &EventError{
				Code:         classifyEventError(completion.err),
				Message:      "ASR segment request failed",
				RequestID:    completion.task.requestID,
				SegmentIndex: segment.Index,
				Final:        true,
			},
		})
		a.sealChain(chain)
		return
	}
	text := strings.TrimSpace(completion.result.Text)
	if text == "" {
		a.emit(Event{
			Type: EventRecognitionError,
			Error: &EventError{
				Code:         "no_speech",
				Message:      "ASR segment contained no speech",
				RequestID:    completion.task.requestID,
				SegmentIndex: segment.Index,
				Final:        true,
			},
		})
		a.sealChain(chain)
		return
	}
	result := SegmentResult{
		SegmentIndex:       segment.Index,
		SourceWindowIndex:  completion.task.globalIndex,
		Revision:           1,
		State:              TranscriptStateStable,
		Text:               text,
		FinalizationReason: FinalizationProviderFinal,
		EvidenceQuality:    EvidenceProviderFinal,
	}
	a.emit(Event{
		Type:     EventSegmentResult,
		Provider: completion.result.Provider,
		Model:    completion.result.Model,
		Segment:  &result,
	})
	a.sealChain(chain)
}

func (a *sessionActor) handleFailedTailTask(chain *chainState) {
	if chain == nil || !chain.tailRequested {
		return
	}
	a.tryFinalizeTail(chain)
	if !chain.sealed && chain.pendingTasks == 0 {
		a.forceTailFinalization(chain.id, chain.tailGeneration)
	}
}

func taskForEvidence(task windowTask) windowTask {
	for index := range task.segments {
		if len(task.segments) == 2 && task.localIndex >= 3 && index == 0 {
			continue
		}
		task.segments[index].Samples = nil
	}
	return task
}

func (a *sessionActor) publishPreview(chain *chainState, evidence windowEvidence) {
	segment := evidence.task.segments[0]
	if existing, exists := chain.segmentResults[segment.Index]; exists &&
		(existing.State == TranscriptStateStable || existing.State == TranscriptStateDegraded ||
			existing.State == TranscriptStateProvisional) {
		return
	}
	result := SegmentResult{
		SegmentIndex:      segment.Index,
		SourceWindowIndex: evidence.result.WindowIndex,
		Revision:          0,
		State:             TranscriptStatePreview,
		Text:              evidence.result.Text,
		EvidenceQuality:   EvidenceStandalone,
	}
	chain.segmentResults[segment.Index] = result
	a.emit(Event{Type: EventSegmentResult, Segment: &result})
}

func (a *sessionActor) reconcileAdjacentWindows(chain *chainState, previousLocalIndex, currentLocalIndex int) {
	previous, previousOK := chain.windows[previousLocalIndex]
	current, currentOK := chain.windows[currentLocalIndex]
	if !previousOK || !currentOK || len(previous.task.segments) != 2 || len(current.task.segments) != 2 {
		return
	}
	if previous.task.segments[1].Index != current.task.segments[0].Index {
		return
	}
	sharedSegment := current.task.segments[0]
	defer a.releaseWindowFallbackSamples(chain, currentLocalIndex)
	alignment, err := a.session.aligner.AlignSuffixPrefix(
		previous.result.Text,
		current.result.Text,
		a.session.cfg.Context.Terms,
	)
	if err != nil {
		a.submitSegmentFallback(chain, sharedSegment)
		return
	}
	previousText := strings.TrimSpace(previous.result.Text[:alignment.PreviousStartByte])
	sharedText := strings.TrimSpace(current.result.Text[:alignment.CurrentEndByte])
	currentText := strings.TrimSpace(current.result.Text[alignment.CurrentEndByte:])
	if previousText == "" || sharedText == "" || currentText == "" {
		a.submitSegmentFallback(chain, sharedSegment)
		return
	}
	updates := make([]SegmentResult, 0, 3)
	updates = a.addStableUpdate(chain, updates, previous.task.segments[0], previous.result.WindowIndex, previousText)
	updates = a.addStableUpdate(chain, updates, current.task.segments[0], current.result.WindowIndex, sharedText)
	latestSegment := current.task.segments[1]
	latestState := TranscriptStateProvisional
	latestReason := FinalizationReason("")
	latestQuality := EvidenceQuality("")
	if chain.tailRequested && latestSegment.Index == chain.segments[len(chain.segments)-1].Index {
		latestState = TranscriptStateStable
		latestReason = chain.finalizationReason
		latestQuality = EvidenceCrossWindowHigh
	}
	latest := SegmentResult{
		SegmentIndex:       latestSegment.Index,
		SourceWindowIndex:  current.result.WindowIndex,
		Revision:           nextRevision(chain.segmentResults[latestSegment.Index]),
		State:              latestState,
		Text:               currentText,
		FinalizationReason: latestReason,
		EvidenceQuality:    latestQuality,
	}
	if existing, exists := chain.segmentResults[latest.SegmentIndex]; !exists || existing.State != TranscriptStateStable {
		chain.segmentResults[latest.SegmentIndex] = latest
		updates = append(updates, latest)
	}
	if len(updates) == 0 {
		return
	}
	batch := RevisionBatch{
		ID:                    a.session.cfg.SessionID + ":reconcile:" + strconv.Itoa(previous.result.WindowIndex) + ":" + strconv.Itoa(current.result.WindowIndex),
		EvidenceRequestIDs:    []string{previous.result.RequestID, current.result.RequestID},
		EvidenceWindowIndices: []int{previous.result.WindowIndex, current.result.WindowIndex},
		Segments:              updates,
		Alignment:             alignment.Info,
	}
	a.emit(Event{Type: EventRevisionBatch, RevisionBatch: &batch})
	if chain.tailRequested && latestState == TranscriptStateStable && chain.pendingTasks == 0 {
		a.sealChain(chain)
	}
}

func (a *sessionActor) submitSegmentFallback(chain *chainState, segment Segment) {
	if chain == nil || len(segment.Samples) == 0 {
		return
	}
	if existing, exists := chain.segmentResults[segment.Index]; exists &&
		(existing.State == TranscriptStateStable || existing.State == TranscriptStateDegraded) {
		return
	}
	if _, exists := chain.fallbackSubmitted[segment.Index]; exists {
		return
	}
	chain.fallbackSubmitted[segment.Index] = struct{}{}
	a.nextWindowIndex++
	a.submit(windowTask{
		chainID:     chain.id,
		globalIndex: a.nextWindowIndex,
		requestID:   a.session.cfg.SessionID + ":chain:" + strconv.Itoa(chain.id) + ":fallback:" + strconv.Itoa(segment.Index),
		kind:        windowKindFallback,
		segments:    []Segment{cloneSegment(segment)},
	})
}

func (a *sessionActor) publishFallbackSegment(chain *chainState, evidence windowEvidence) {
	if chain == nil || len(evidence.task.segments) != 1 || strings.TrimSpace(evidence.result.Text) == "" {
		return
	}
	segment := evidence.task.segments[0]
	if existing, exists := chain.segmentResults[segment.Index]; exists && existing.State == TranscriptStateStable {
		return
	}
	result := SegmentResult{
		SegmentIndex:       segment.Index,
		SourceWindowIndex:  evidence.result.WindowIndex,
		Revision:           nextRevision(chain.segmentResults[segment.Index]),
		State:              TranscriptStateDegraded,
		Text:               evidence.result.Text,
		FinalizationReason: FinalizationNextWindow,
		EvidenceQuality:    EvidenceDegraded,
	}
	chain.segmentResults[segment.Index] = result
	a.emit(Event{Type: EventSegmentResult, Segment: &result})
}

func (a *sessionActor) releaseWindowFallbackSamples(chain *chainState, localIndex int) {
	evidence, exists := chain.windows[localIndex]
	if !exists || len(evidence.task.segments) == 0 {
		return
	}
	evidence.task.segments[0].Samples = nil
	chain.windows[localIndex] = evidence
}

func (a *sessionActor) addStableUpdate(
	chain *chainState,
	updates []SegmentResult,
	segment Segment,
	windowIndex int,
	text string,
) []SegmentResult {
	if existing, exists := chain.segmentResults[segment.Index]; exists && existing.State == TranscriptStateStable {
		return updates
	}
	result := SegmentResult{
		SegmentIndex:       segment.Index,
		SourceWindowIndex:  windowIndex,
		Revision:           nextRevision(chain.segmentResults[segment.Index]),
		State:              TranscriptStateStable,
		Text:               text,
		FinalizationReason: FinalizationNextWindow,
		EvidenceQuality:    EvidenceCrossWindowHigh,
	}
	chain.segmentResults[segment.Index] = result
	return append(updates, result)
}

func (a *sessionActor) scheduleTail(chain *chainState, segment Segment) {
	a.stopTailTimers(chain)
	chain.tailGeneration++
	generation := chain.tailGeneration
	observedSilence := time.Duration(max(0, segment.StreamDuration-segment.EndAt) * float64(time.Second))
	targetSilence := a.session.cfg.TailFinalizeSilence
	segmentDuration := time.Duration((segment.EndAt - segment.StartAt) * float64(time.Second))
	chain.waitingForNeighbor = a.session.cfg.ShortSegmentMaxDuration > 0 &&
		segmentDuration < a.session.cfg.ShortSegmentMaxDuration
	if chain.waitingForNeighbor {
		targetSilence = max(targetSilence, a.session.cfg.ShortSegmentNeighborWait)
	}
	delay := max(time.Duration(0), targetSilence-observedSilence)
	chain.tailTimer = time.AfterFunc(delay, func() {
		a.sendInternalCommand(tailDeadlineCommand{
			chainID:    chain.id,
			generation: generation,
			reason:     FinalizationSilenceTimeout,
		})
	})
}

func (a *sessionActor) requestTailFinalization(chainID int, generation uint64, reason FinalizationReason) {
	chain := a.chainByID(chainID)
	if chain == nil || chain.sealed || generation != chain.tailGeneration {
		return
	}
	a.stopTailTimers(chain)
	chain.tailRequested = true
	chain.waitingForNeighbor = false
	chain.finalizationReason = reason
	chain.tailResultTimer = time.AfterFunc(a.session.cfg.TailFinalizeResultWait, func() {
		a.sendInternalCommand(tailResultTimeoutCommand{chainID: chain.id, generation: generation})
	})
	a.tryFinalizeTail(chain)
}

func (a *sessionActor) tryFinalizeTail(chain *chainState) {
	if chain == nil || chain.sealed || !chain.tailRequested || len(chain.segments) == 0 {
		return
	}
	if chain.pendingTasks > 0 {
		return
	}
	count := len(chain.segments)
	switch count {
	case 1:
		if evidence, ok := chain.windows[1]; ok {
			a.finalizeStandalone(chain, evidence)
		}
	default:
		latest := chain.segments[count-1]
		if existing, ok := chain.segmentResults[latest.Index]; ok {
			if existing.State == TranscriptStateProvisional {
				existing.State = TranscriptStateStable
				existing.Revision++
				existing.FinalizationReason = chain.finalizationReason
				existing.EvidenceQuality = EvidenceCrossWindowHigh
				chain.segmentResults[latest.Index] = existing
				a.emit(Event{Type: EventSegmentResult, Segment: &existing})
			}
			a.sealChain(chain)
			return
		}
		if !a.session.cfg.TailAnchorEnabled {
			return
		}
		if !chain.tailAnchorSubmitted {
			if chain.pendingTasks > 0 {
				return
			}
			a.submitTailAnchor(chain)
			return
		}
		if chain.tailAnchor != nil {
			if pair, ok := chain.windows[count]; ok {
				a.finalizeTailPair(chain, pair, *chain.tailAnchor)
				return
			}
			if chain.pendingTasks == 0 {
				a.finalizeTailFromStandalone(chain, *chain.tailAnchor)
			}
		}
	}
}

func (a *sessionActor) submitTailAnchor(chain *chainState) {
	chain.tailAnchorSubmitted = true
	latest := chain.segments[len(chain.segments)-1]
	a.nextWindowIndex++
	task := windowTask{
		chainID:     chain.id,
		localIndex:  len(chain.segments),
		globalIndex: a.nextWindowIndex,
		requestID:   a.session.cfg.SessionID + ":chain:" + strconv.Itoa(chain.id) + ":tail:" + strconv.Itoa(latest.Index),
		kind:        windowKindTailAnchor,
		segments:    []Segment{cloneSegment(latest)},
	}
	a.submit(task)
}

func (a *sessionActor) finalizeStandalone(chain *chainState, evidence windowEvidence) {
	segment := chain.segments[0]
	result := SegmentResult{
		SegmentIndex:       segment.Index,
		SourceWindowIndex:  evidence.result.WindowIndex,
		Revision:           nextRevision(chain.segmentResults[segment.Index]),
		State:              TranscriptStateStable,
		Text:               evidence.result.Text,
		FinalizationReason: chain.finalizationReason,
		EvidenceQuality:    EvidenceStandalone,
	}
	chain.segmentResults[segment.Index] = result
	a.emit(Event{Type: EventSegmentResult, Segment: &result})
	a.sealChain(chain)
}

func (a *sessionActor) finalizeTailPair(chain *chainState, pair, tail windowEvidence) {
	alignment, err := a.session.aligner.AlignSuffixPrefix(pair.result.Text, tail.result.Text, a.session.cfg.Context.Terms)
	if err != nil {
		a.finalizeTailFromStandalone(chain, tail)
		return
	}
	firstText := strings.TrimSpace(pair.result.Text[:alignment.PreviousStartByte])
	secondText := strings.TrimSpace(pair.result.Text[alignment.PreviousStartByte:])
	if firstText == "" || secondText == "" {
		a.finalizeTailFromStandalone(chain, tail)
		return
	}
	previous := pair.task.segments[0]
	latest := pair.task.segments[len(pair.task.segments)-1]
	updates := []SegmentResult{
		{
			SegmentIndex:       previous.Index,
			SourceWindowIndex:  pair.result.WindowIndex,
			Revision:           nextRevision(chain.segmentResults[previous.Index]),
			State:              TranscriptStateStable,
			Text:               firstText,
			FinalizationReason: chain.finalizationReason,
			EvidenceQuality:    EvidenceCrossWindowHigh,
		},
		{
			SegmentIndex:       latest.Index,
			SourceWindowIndex:  pair.result.WindowIndex,
			Revision:           nextRevision(chain.segmentResults[latest.Index]),
			State:              TranscriptStateStable,
			Text:               secondText,
			FinalizationReason: chain.finalizationReason,
			EvidenceQuality:    EvidenceCrossWindowHigh,
		},
	}
	for _, update := range updates {
		chain.segmentResults[update.SegmentIndex] = update
	}
	batch := RevisionBatch{
		ID:                    a.session.cfg.SessionID + ":tail:" + strconv.Itoa(chain.id),
		EvidenceRequestIDs:    []string{pair.result.RequestID, tail.result.RequestID},
		EvidenceWindowIndices: []int{pair.result.WindowIndex, tail.result.WindowIndex},
		Segments:              updates,
		Alignment:             alignment.Info,
	}
	a.emit(Event{Type: EventRevisionBatch, RevisionBatch: &batch})
	a.sealChain(chain)
}

func (a *sessionActor) finalizeTailFromStandalone(chain *chainState, tail windowEvidence) {
	latest := chain.segments[len(chain.segments)-1]
	text := strings.TrimSpace(tail.result.Text)
	if text == "" {
		return
	}
	result := SegmentResult{
		SegmentIndex:       latest.Index,
		SourceWindowIndex:  tail.result.WindowIndex,
		Revision:           nextRevision(chain.segmentResults[latest.Index]),
		State:              TranscriptStateDegraded,
		Text:               text,
		FinalizationReason: chain.finalizationReason,
		EvidenceQuality:    EvidenceDegraded,
	}
	chain.segmentResults[latest.Index] = result
	a.emit(Event{Type: EventSegmentResult, Segment: &result})
	a.finalizeExistingUnstableSegments(chain)
	a.sealChain(chain)
}

func (a *sessionActor) finalizeExistingUnstableSegments(chain *chainState) {
	for _, segment := range chain.segments {
		existing, exists := chain.segmentResults[segment.Index]
		if !exists || existing.Text == "" || existing.State == TranscriptStateStable ||
			existing.State == TranscriptStateDegraded {
			continue
		}
		existing.State = TranscriptStateDegraded
		existing.Revision++
		existing.FinalizationReason = chain.finalizationReason
		existing.EvidenceQuality = EvidenceDegraded
		chain.segmentResults[segment.Index] = existing
		a.emit(Event{Type: EventSegmentResult, Segment: &existing})
	}
}

func (a *sessionActor) forceTailFinalization(chainID int, generation uint64) {
	chain := a.chainByID(chainID)
	if chain == nil || chain.sealed || generation != chain.tailGeneration {
		return
	}
	if chain.pendingTasks > 0 {
		chain.tailResultTimer = time.AfterFunc(a.session.cfg.TailFinalizeResultWait, func() {
			a.sendInternalCommand(tailResultTimeoutCommand{chainID: chain.id, generation: generation})
		})
		return
	}
	latest := chain.segments[len(chain.segments)-1]
	if _, exists := chain.segmentResults[latest.Index]; !exists && chain.tailAnchor != nil && chain.tailAnchor.result.Text != "" {
		chain.segmentResults[latest.Index] = SegmentResult{
			SegmentIndex:      latest.Index,
			SourceWindowIndex: chain.tailAnchor.result.WindowIndex,
			State:             TranscriptStateProvisional,
			Text:              chain.tailAnchor.result.Text,
		}
	}
	resolvedLatest := false
	for _, segment := range chain.segments {
		existing, exists := chain.segmentResults[segment.Index]
		if !exists || existing.Text == "" {
			continue
		}
		if existing.State != TranscriptStateStable && existing.State != TranscriptStateDegraded {
			existing.State = TranscriptStateDegraded
			existing.Revision++
			existing.FinalizationReason = FinalizationRequestTimeout
			existing.EvidenceQuality = EvidenceDegraded
			chain.segmentResults[segment.Index] = existing
			a.emit(Event{Type: EventSegmentResult, Segment: &existing})
		}
		if segment.Index == latest.Index {
			resolvedLatest = true
		}
	}
	if !resolvedLatest {
		a.emit(Event{
			Type: EventRecognitionError,
			Error: &EventError{
				Code:         "request_timeout",
				Message:      tailFinalizationTimeoutMessage,
				SegmentIndex: latest.Index,
				Final:        true,
			},
		})
	}
	chain.finalizationReason = FinalizationRequestTimeout
	a.sealChain(chain)
}

func (a *sessionActor) sealChain(chain *chainState) {
	if chain == nil || chain.sealed {
		return
	}
	a.finalizeExistingUnstableSegments(chain)
	chain.sealed = true
	a.stopTailTimers(chain)
	delete(a.chains, chain.id)
	if a.current == chain {
		a.current = nil
	}
	chain.segments = nil
	chain.windows = nil
	chain.segmentResults = nil
	chain.fallbackSubmitted = nil
	chain.tailAnchor = nil
	a.maybeComplete()
}

func (a *sessionActor) chainByID(chainID int) *chainState {
	if a.chains == nil {
		return nil
	}
	return a.chains[chainID]
}

func (a *sessionActor) maybeComplete() {
	if !a.stopping || a.completed || len(a.chains) != 0 {
		return
	}
	a.completed = true
	a.emit(Event{Type: EventCompleted})
}

func (a *sessionActor) stopTailTimers(chain *chainState) {
	if chain == nil {
		return
	}
	if chain.tailTimer != nil {
		chain.tailTimer.Stop()
		chain.tailTimer = nil
	}
	if chain.tailResultTimer != nil {
		chain.tailResultTimer.Stop()
		chain.tailResultTimer = nil
	}
}

func (a *sessionActor) stopChainTimers(chain *chainState) {
	a.stopTailTimers(chain)
}

func (a *sessionActor) sendInternalCommand(command any) {
	select {
	case a.session.commands <- command:
	case <-a.session.ctx.Done():
	}
}

func (a *sessionActor) emit(event Event) {
	if event.Type == EventCompleted {
		a.session.complete.Do(func() { close(a.session.completed) })
	}
	a.sequence++
	event.SessionID = a.session.cfg.SessionID
	event.Sequence = a.sequence
	event.Timestamp = time.Now().UTC()
	select {
	case a.session.events <- event:
	case <-a.session.ctx.Done():
	}
}

func (a *sessionActor) windowRequestID(chainID, windowIndex int, segments []Segment) string {
	first := segments[0].Index
	last := segments[len(segments)-1].Index
	return a.session.cfg.SessionID + ":chain:" + strconv.Itoa(chainID) +
		":window:" + strconv.Itoa(windowIndex) + ":" + strconv.Itoa(first) + ":" + strconv.Itoa(last)
}

func normalizeSessionConfig(cfg SessionConfig) (SessionConfig, error) {
	if cfg.SessionID == "" || cfg.SampleRate <= 0 || cfg.Channels != 1 {
		return cfg, ErrInvalidConfig
	}
	languageTag, err := NormalizeLanguageTag(cfg.Language)
	if err != nil {
		return cfg, err
	}
	languageHints, err := normalizeLanguageHints(cfg.LanguageHints)
	if err != nil {
		return cfg, err
	}
	cfg.Language = languageTag
	cfg.LanguageHints = languageHints
	if cfg.SegmentStrategy == "" {
		cfg.SegmentStrategy = SegmentRecognitionStrategyContextual
	}
	if cfg.SegmentStrategy != SegmentRecognitionStrategyContextual &&
		cfg.SegmentStrategy != SegmentRecognitionStrategySingle {
		return cfg, ErrInvalidConfig
	}
	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultSessionEventBuffer
	}
	if cfg.ContextSilence <= 0 {
		cfg.ContextSilence = defaultContextSilence
	}
	if cfg.TailFinalizeSilence <= 0 {
		cfg.TailFinalizeSilence = defaultTailFinalizeSilence
	}
	if cfg.TailFinalizeResultWait <= 0 {
		cfg.TailFinalizeResultWait = defaultTailFinalizeResultWait
	}
	if cfg.ShortSegmentMaxDuration < 0 || cfg.ShortSegmentNeighborWait < 0 ||
		(cfg.ShortSegmentMaxDuration == 0) != (cfg.ShortSegmentNeighborWait == 0) ||
		(cfg.ShortSegmentMaxDuration > 0 && cfg.ShortSegmentNeighborWait < cfg.TailFinalizeSilence) {
		return cfg, ErrInvalidConfig
	}
	if cfg.MaxWindowDuration <= 0 {
		cfg.MaxWindowDuration = defaultMaxWindowDuration
	}
	return cfg, nil
}

func validateSegment(cfg SessionConfig, segment Segment) error {
	if segment.Index < 0 || segment.StartAt < 0 || segment.EndAt <= segment.StartAt ||
		segment.StreamDuration < segment.EndAt || len(segment.Samples) == 0 {
		return ErrSegmentInvalid
	}
	expected := int((segment.EndAt - segment.StartAt) * float64(cfg.SampleRate))
	if expected <= 0 || len(segment.Samples) < expected-cfg.SampleRate/20 || len(segment.Samples) > expected+cfg.SampleRate/20 {
		return ErrSegmentInvalid
	}
	return nil
}

func composeWindowSamples(segments []Segment, sampleRate int, silence time.Duration) []float32 {
	count := 0
	for _, segment := range segments {
		count += len(segment.Samples)
	}
	silenceSamples := 0
	if len(segments) > 1 {
		silenceSamples = int(silence.Seconds() * float64(sampleRate))
		for index := 1; index < len(segments); index++ {
			if segments[index].StartAt > segments[index-1].EndAt {
				count += silenceSamples
			}
		}
	}
	output := make([]float32, 0, count)
	for index, segment := range segments {
		if index > 0 && silenceSamples > 0 && segment.StartAt > segments[index-1].EndAt {
			output = append(output, make([]float32, silenceSamples)...)
		}
		output = append(output, segment.Samples...)
	}
	return output
}

func windowDuration(segments []Segment, silence time.Duration) time.Duration {
	duration := time.Duration(0)
	for _, segment := range segments {
		duration += time.Duration((segment.EndAt - segment.StartAt) * float64(time.Second))
	}
	for index := 1; index < len(segments); index++ {
		if segments[index].StartAt > segments[index-1].EndAt {
			duration += silence
		}
	}
	return duration
}

func segmentRefs(segments []Segment) []SegmentRef {
	refs := make([]SegmentRef, 0, len(segments))
	for _, segment := range segments {
		refs = append(refs, SegmentRef{Index: segment.Index, StartAt: segment.StartAt, EndAt: segment.EndAt})
	}
	return refs
}

func cloneSegment(segment Segment) Segment {
	segment.Samples = append([]float32(nil), segment.Samples...)
	return segment
}

func cloneSegments(segments []Segment) []Segment {
	cloned := make([]Segment, 0, len(segments))
	for _, segment := range segments {
		cloned = append(cloned, cloneSegment(segment))
	}
	return cloned
}

func nextRevision(existing SegmentResult) int {
	return existing.Revision + 1
}

func classifyEventError(err error) string {
	switch {
	case errors.Is(err, ErrNoSpeech):
		return "no_speech"
	case errors.Is(err, ErrRequestTimeout):
		return "request_timeout"
	case errors.Is(err, ErrRateLimited):
		return "rate_limited"
	case errors.Is(err, ErrUnauthorized):
		return eventErrorUnauthorized
	case errors.Is(err, ErrOverloaded):
		return "overloaded"
	default:
		return "provider_error"
	}
}
