package asr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultReconnectAttempts        = 3
	defaultReconnectBackoff         = 250 * time.Millisecond
	defaultReconnectMaxBackoff      = 2 * time.Second
	defaultReconnectTimeout         = 10 * time.Second
	defaultRecoveryOperationTimeout = 5 * time.Second
	defaultReplayAudio              = 120 * time.Second
	defaultReplayChunk              = 100 * time.Millisecond
)

// RealtimeRecoveryConfig enables bounded at-least-once audio delivery when set
// on RealtimeSessionConfig. Nil preserves the original direct stream behavior.
type RealtimeRecoveryConfig struct {
	MaxAttempts      int
	Backoff          time.Duration
	MaxBackoff       time.Duration
	ConnectTimeout   time.Duration
	OperationTimeout time.Duration
	FinishTimeout    time.Duration
	MaxBufferedAudio time.Duration
	ReadTimeout      time.Duration
}

type RecoveryInfo struct {
	Attempt       int     `json:"attempt"`
	ReplayStartAt float64 `json:"replay_start_at"`
	ReplayEndAt   float64 `json:"replay_end_at"`
}

type recoveringStream struct {
	ctx           context.Context //nolint:containedctx // Owns the recovery lifecycle.
	cancel        context.CancelCauseFunc
	provider      StreamingProvider
	request       StreamingRequest
	cfg           RealtimeRecoveryConfig
	events        chan ProviderStreamEvent
	done          chan struct{}
	wake          chan struct{}
	mu            sync.Mutex
	buffer        []byte
	base          int64
	end           int64
	expectedSeq   uint64
	finished      bool
	waitErr       error
	update        *StreamingContextUpdate
	updateVersion uint64
	finishTimer   *time.Timer
	// Only run owns the generation-specific state below.
	stream        ProviderStream
	streamCancel  context.CancelFunc
	generation    int
	offset        int64
	cursor        int64
	sequence      uint64
	contextSent   uint64
	closeSent     bool
	attempts      int
	active        map[string]bool
	usage         RecognitionUsage
	previousUsage RecognitionUsage
}

type recoveringContextStream struct{ *recoveringStream }

func normalizeRealtimeRecovery(cfg RealtimeRecoveryConfig) (RealtimeRecoveryConfig, error) {
	if cfg.MaxAttempts < 0 || cfg.MaxAttempts > 10 || cfg.Backoff < 0 || cfg.MaxBackoff < 0 ||
		cfg.ConnectTimeout < 0 || cfg.OperationTimeout < 0 || cfg.FinishTimeout < 0 || cfg.MaxBufferedAudio < 0 {
		return cfg, ErrInvalidConfig
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = defaultReconnectAttempts
	}
	if cfg.Backoff == 0 {
		cfg.Backoff = defaultReconnectBackoff
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = defaultReconnectMaxBackoff
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = defaultReconnectTimeout
	}
	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = defaultRecoveryOperationTimeout
	}
	if cfg.FinishTimeout == 0 {
		cfg.FinishTimeout = defaultRealtimeWaitTimeout
	}
	if cfg.MaxBufferedAudio == 0 {
		cfg.MaxBufferedAudio = defaultReplayAudio
	}
	if cfg.MaxBackoff < cfg.Backoff || cfg.MaxBufferedAudio < defaultReplayChunk {
		return cfg, ErrInvalidConfig
	}
	return cfg, nil
}

func newRecoveringStream(ctx context.Context, provider StreamingProvider, request StreamingRequest, cfg RealtimeRecoveryConfig) (ProviderStream, error) {
	cfg, err := normalizeRealtimeRecovery(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	request.Context = cloneRecognitionContext(request.Context)
	request.ReadTimeout = cfg.ReadTimeout
	s := &recoveringStream{
		ctx: ctx, cancel: cancel, provider: provider, request: request, cfg: cfg,
		events: make(chan ProviderStreamEvent, defaultRealtimeEventBuffer), done: make(chan struct{}), wake: make(chan struct{}, 1), active: make(map[string]bool),
	}
	// Initial failure is returned synchronously; no hidden retry of credentials.
	if err := s.connect(ctx); err != nil {
		cancel(err)
		return nil, err
	}
	_, updatesContext := s.stream.(ProviderContextUpdater)
	go s.run() //nolint:contextcheck // The worker owns s.ctx, not the caller's operation context.
	if updatesContext {
		return &recoveringContextStream{s}, nil
	}
	return s, nil
}

func (s *recoveringStream) connect(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	timer := time.AfterFunc(s.cfg.ConnectTimeout, cancel)
	stream, err := s.provider.Start(ctx, s.request)
	stopped := timer.Stop()
	if !stopped || ctx.Err() != nil {
		if stream != nil {
			stream.Close()
		}
		cancel()
		return errors.Join(ErrRequestTimeout, ctx.Err(), err)
	}
	if err != nil {
		cancel()
		return errors.Join(ErrProviderRequest, err)
	}
	s.stream, s.streamCancel = stream, cancel
	s.mu.Lock()
	s.offset, s.cursor = s.base, s.base
	s.mu.Unlock()
	s.sequence, s.contextSent, s.closeSent = 0, 0, false
	return nil
}

func (s *recoveringStream) WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error {
	if ctx.Err() != nil {
		return errors.Join(ErrRequestTimeout, ctx.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished || s.ctx.Err() != nil {
		return ErrSessionClosed
	}
	if chunk.Sequence != s.expectedSeq || chunk.StartSample != s.end || len(chunk.Data) == 0 || len(chunk.Data)%2 != 0 ||
		chunk.EndSample-chunk.StartSample != int64(len(chunk.Data)/2) {
		return ErrInvalidRequest
	}
	limit := durationSamples(s.cfg.MaxBufferedAudio, int64(s.request.SampleRate))
	if chunk.EndSample-s.base > limit {
		s.cancel(ErrPCMBufferLimit)
		return ErrPCMBufferLimit
	}
	s.buffer = append(s.buffer, chunk.Data...)
	s.end = chunk.EndSample
	s.expectedSeq++
	s.notify()
	return nil
}

func (s *recoveringStream) CloseInput(ctx context.Context) error {
	if ctx.Err() != nil {
		return errors.Join(ErrRequestTimeout, ctx.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return ErrSessionClosed
	}
	if !s.finished {
		s.finished = true
		s.finishTimer = time.AfterFunc(s.cfg.FinishTimeout, func() { s.cancel(ErrRequestTimeout) })
		s.notify()
	}
	return nil
}

func (s *recoveringContextStream) UpdateContext(ctx context.Context, update StreamingContextUpdate) error {
	if ctx.Err() != nil {
		return errors.Join(ErrRequestTimeout, ctx.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished || s.ctx.Err() != nil {
		return ErrSessionClosed
	}
	update.Context = cloneRecognitionContext(update.Context)
	update.StableTranscripts = append([]string(nil), update.StableTranscripts...)
	s.update = &update
	s.updateVersion++
	s.notify()
	return nil
}

func (s *recoveringStream) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *recoveringStream) Events() <-chan ProviderStreamEvent { return s.events }
func (s *recoveringStream) Done() <-chan struct{}              { return s.done }
func (s *recoveringStream) Close()                             { s.cancel(ErrSessionClosed) }
func (s *recoveringStream) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.waitErr
	case <-ctx.Done():
		return errors.Join(ErrRequestTimeout, ctx.Err())
	}
}

func (s *recoveringStream) run() {
	err := s.pump()
	if err != nil {
		s.emit(ProviderStreamEvent{Err: err, IsFinal: true})
	}
	s.streamCancel()
	s.stream.Close()
	s.mu.Lock()
	s.waitErr = err
	s.buffer = nil
	if s.finishTimer != nil {
		s.finishTimer.Stop()
	}
	s.mu.Unlock()
	s.cancel(err)
	close(s.events)
	close(s.done)
}

func (s *recoveringStream) pump() error {
	for {
		if s.ctx.Err() != nil {
			return errors.Join(ErrProviderRequest, context.Cause(s.ctx))
		}
		// Drain provider events before writing more PCM, so acknowledgements can
		// release replay memory even during fast file upload.
		select {
		case event, ok := <-s.stream.Events():
			if done, err := s.receive(event, ok); done || err != nil {
				return err
			}
			continue
		default:
		}
		worked, err := s.sendNext()
		if err != nil {
			if err = s.recover(err); err != nil {
				return err
			}
			continue
		}
		if worked {
			continue
		}
		select {
		case <-s.ctx.Done():
			return errors.Join(ErrProviderRequest, context.Cause(s.ctx))
		case <-s.wake:
		case event, ok := <-s.stream.Events():
			if done, err := s.receive(event, ok); done || err != nil {
				return err
			}
		}
	}
}

func (s *recoveringStream) sendNext() (bool, error) {
	s.mu.Lock()
	update, version := s.update, s.updateVersion
	end := min(s.end, s.cursor+durationSamples(defaultReplayChunk, int64(s.request.SampleRate)))
	var data []byte
	if end > s.cursor {
		data = bytes.Clone(s.buffer[(s.cursor-s.base)*2 : (end-s.base)*2])
	}
	finished := s.finished
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.OperationTimeout)
	defer cancel()
	if update != nil && version != s.contextSent {
		updater, ok := s.stream.(ProviderContextUpdater)
		if !ok {
			return false, ErrContextUpdateUnsupported
		}
		if err := updater.UpdateContext(ctx, *update); err != nil {
			return false, errors.Join(ErrProviderRequest, err)
		}
		s.contextSent = version
		return true, nil
	}
	if len(data) > 0 {
		err := s.stream.WriteAudio(ctx, StreamingAudioChunk{Data: data, Sequence: s.sequence, StartSample: s.cursor - s.offset, EndSample: end - s.offset})
		if err != nil {
			return false, errors.Join(ErrProviderRequest, err)
		}
		s.cursor = end
		s.sequence++
		return true, nil
	}
	if finished && !s.closeSent {
		if err := s.stream.CloseInput(ctx); err != nil {
			return false, errors.Join(ErrProviderRequest, err)
		}
		s.closeSent = true
		return true, nil
	}
	return false, nil
}

func (s *recoveringStream) receive(event ProviderStreamEvent, ok bool) (bool, error) {
	if !ok {
		ctx, cancel := context.WithTimeout(s.ctx, s.cfg.OperationTimeout)
		err := s.stream.Wait(ctx)
		cancel()
		if err == nil && s.closeSent {
			return true, nil
		}
		if err == nil {
			err = ErrProviderUnavailable
		}
		return false, s.recover(err)
	}
	if event.Err != nil && event.IsFinal {
		return false, s.recover(event.Err)
	}
	if event.Usage != nil {
		s.usage = *event.Usage
		combined := s.previousUsage
		addRecoveryUsage(&combined, s.usage)
		event.Usage = &combined
	}
	if event.ResultID != "" {
		id := strconv.Itoa(s.generation) + ":" + event.ResultID
		event.ResultID = id
		if event.IsFinal || event.Discarded {
			delete(s.active, id)
		} else if event.Text != "" || event.Started {
			s.active[id] = true
		}
	}
	if event.IsFinal && event.Err == nil && !event.Discarded {
		if event.EndAt > 0 {
			end := s.offset + durationSamples(event.EndAt, int64(s.request.SampleRate))
			s.mu.Lock()
			if end > s.base && end <= s.cursor {
				s.buffer = bytes.Clone(s.buffer[(end-s.base)*2:])
				s.base = end
				// Repeated finals without new acknowledged audio must not grant
				// an unlimited reconnect budget.
				s.attempts = 0
			}
			s.mu.Unlock()
		}
	}
	shift := time.Duration(s.offset) * time.Second / time.Duration(s.request.SampleRate)
	if event.StartAt > 0 || event.EndAt > 0 {
		event.StartAt += shift
		event.EndAt += shift
	}
	event.Sentences = cloneTranscriptSentences(event.Sentences)
	for i := range event.Sentences {
		event.Sentences[i].StartAt += shift.Seconds()
		event.Sentences[i].EndAt += shift.Seconds()
		for j := range event.Sentences[i].Words {
			event.Sentences[i].Words[j].StartAt += shift.Seconds()
			event.Sentences[i].Words[j].EndAt += shift.Seconds()
		}
	}
	s.emit(event)
	return false, nil
}

func (s *recoveringStream) recover(cause error) error {
	if s.ctx.Err() != nil {
		return errors.Join(ErrProviderRequest, context.Cause(s.ctx))
	}
	if !retryableStreamError(cause) {
		return cause
	}
	s.streamCancel()
	s.stream.Close()
	for id := range s.active {
		s.emit(ProviderStreamEvent{ResultID: id, Discarded: true, IsFinal: true})
	}
	clear(s.active)
	addRecoveryUsage(&s.previousUsage, s.usage)
	s.usage = RecognitionUsage{}
	for s.attempts < s.cfg.MaxAttempts {
		s.attempts++
		s.mu.Lock()
		info := &RecoveryInfo{Attempt: s.attempts, ReplayStartAt: float64(s.base) / float64(s.request.SampleRate), ReplayEndAt: float64(s.end) / float64(s.request.SampleRate)}
		s.mu.Unlock()
		s.emit(ProviderStreamEvent{Err: ErrStreamReconnecting, Recovery: info})
		delay := s.cfg.Backoff
		for i := 1; i < s.attempts; i++ {
			delay = min(delay*2, s.cfg.MaxBackoff)
		}
		timer := time.NewTimer(delay)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return errors.Join(ErrProviderRequest, context.Cause(s.ctx))
		case <-timer.C:
		}
		s.generation++
		if cause = s.connect(s.ctx); cause == nil {
			return nil
		}
		if !retryableStreamError(cause) {
			return cause
		}
	}
	return errors.Join(ErrReconnectExhausted, cause)
}

func retryableStreamError(err error) bool {
	if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrProviderResponse) {
		return false
	}
	_, network := errors.AsType[net.Error](err)
	_, closed := errors.AsType[*websocket.CloseError](err)
	return isRetryableProviderError(err) || network || closed || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}

func addRecoveryUsage(total *RecognitionUsage, value RecognitionUsage) {
	total.Duration += value.Duration
	total.InputTokens += value.InputTokens
	total.OutputTokens += value.OutputTokens
	total.TotalTokens += value.TotalTokens
}

func (s *recoveringStream) emit(event ProviderStreamEvent) {
	select {
	case s.events <- event:
	case <-s.ctx.Done():
	}
}
