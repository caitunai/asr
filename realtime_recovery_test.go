package asr

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recoveryTestProvider struct {
	start func(context.Context, StreamingRequest) (ProviderStream, error)
}

func (*recoveryTestProvider) Name() string  { return "recovery-test" }
func (*recoveryTestProvider) Model() string { return "test" }
func (*recoveryTestProvider) StreamingCapabilities() StreamingCapabilities {
	return StreamingCapabilities{}
}

func (p *recoveryTestProvider) Start(ctx context.Context, request StreamingRequest) (ProviderStream, error) {
	return p.start(ctx, request)
}

// Callbacks and event publication run on the recovery worker, like a serialized
// provider. Close may also be called by cleanup, so it is idempotent.
type recoveryTestStream struct {
	events chan ProviderStreamEvent
	done   chan struct{}
	once   sync.Once
	write  func(context.Context, StreamingAudioChunk) error
	finish func()
}

func newRecoveryTestStream() *recoveryTestStream {
	return &recoveryTestStream{events: make(chan ProviderStreamEvent, 32), done: make(chan struct{})}
}

func (s *recoveryTestStream) WriteAudio(ctx context.Context, chunk StreamingAudioChunk) error {
	if s.write != nil {
		return s.write(ctx, chunk)
	}
	return nil
}

func (s *recoveryTestStream) CloseInput(context.Context) error {
	if s.finish != nil {
		s.finish()
	} else {
		s.Close()
	}
	return nil
}
func (s *recoveryTestStream) Events() <-chan ProviderStreamEvent { return s.events }
func (s *recoveryTestStream) Done() <-chan struct{}              { return s.done }
func (s *recoveryTestStream) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return errors.Join(ErrRequestTimeout, ctx.Err())
	}
}

func (s *recoveryTestStream) Close() {
	s.once.Do(func() { close(s.events); close(s.done) })
}

func recoveryTestRequest() StreamingRequest {
	return StreamingRequest{SessionID: "recovery", SampleRate: 16000, Channels: 1, Format: AudioFormatRawPCM16}
}

func openRecoveryTest(t *testing.T, provider StreamingProvider, cfg RealtimeRecoveryConfig) ProviderStream {
	t.Helper()
	if cfg.Backoff == 0 {
		cfg.Backoff = time.Millisecond
	}
	stream, err := newRecoveringStream(t.Context(), provider, recoveryTestRequest(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stream.Close()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		_ = stream.Wait(ctx)
	})
	return stream
}

func waitRecoveryTest(t *testing.T, stream ProviderStream) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := stream.Wait(ctx); err != nil {
		return errors.Join(ErrProviderRequest, err)
	}
	return nil
}

func TestRealtimeRecoveryReplaysOnlyUnconfirmedAudio(t *testing.T) {
	var connections int
	var replay []byte
	provider := &recoveryTestProvider{start: func(_ context.Context, _ StreamingRequest) (ProviderStream, error) {
		connections++
		s := newRecoveryTestStream()
		generation := connections
		s.write = func(_ context.Context, chunk StreamingAudioChunk) error {
			if generation == 1 {
				if chunk.Sequence == 0 {
					s.events <- ProviderStreamEvent{ResultID: "one", Text: "first", IsFinal: true, EndAt: 100 * time.Millisecond}
				} else {
					s.events <- ProviderStreamEvent{ResultID: "two", Text: "draft"}
					s.events <- ProviderStreamEvent{Err: ErrProviderUnavailable, IsFinal: true}
				}
			} else {
				replay = append(replay, chunk.Data...)
				if chunk.StartSample != int64(chunk.Sequence)*1600 {
					t.Errorf("replay coordinates: %+v", chunk)
				}
			}
			return nil
		}
		if generation == 2 {
			s.finish = func() {
				s.events <- ProviderStreamEvent{ResultID: "one", Text: "second", IsFinal: true, EndAt: 200 * time.Millisecond}
				s.Close()
			}
		}
		return s, nil
	}}
	stream := openRecoveryTest(t, provider, RealtimeRecoveryConfig{})
	data := append(bytes.Repeat([]byte{1, 0}, 1600), bytes.Repeat([]byte{2, 0}, 3200)...)
	if err := stream.WriteAudio(t.Context(), StreamingAudioChunk{Data: data, EndSample: 4800}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseInput(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := waitRecoveryTest(t, stream); err != nil {
		t.Fatal(err)
	}
	if connections != 2 || !bytes.Equal(replay, data[3200:]) {
		t.Fatalf("connections=%d replay bytes=%d", connections, len(replay))
	}
	var discarded, recovery, shifted bool
	for event := range stream.Events() {
		if event.Discarded && event.ResultID == "0:two" {
			discarded = true
		}
		if event.Recovery != nil && event.Recovery.ReplayStartAt == 0.1 {
			recovery = true
		}
		if event.Text == "second" && event.ResultID == "1:one" && event.StartAt == 100*time.Millisecond && event.EndAt == 300*time.Millisecond {
			shifted = true
		}
	}
	if !discarded || !recovery || !shifted {
		t.Fatalf("discarded=%v recovery=%v shifted=%v", discarded, recovery, shifted)
	}
}

func TestRealtimeRecoveryRetryBudget(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failure, want error
		connections   int
	}{
		{"network", ErrProviderUnavailable, ErrReconnectExhausted, 3},
		{"unauthorized", ErrUnauthorized, ErrUnauthorized, 1},
		{"bad_response", ErrProviderResponse, ErrProviderResponse, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var connections atomic.Int32
			provider := &recoveryTestProvider{start: func(_ context.Context, _ StreamingRequest) (ProviderStream, error) {
				connections.Add(1)
				s := newRecoveryTestStream()
				// Untimed replayed finals are not proof of forward progress.
				s.events <- ProviderStreamEvent{ResultID: "repeated", Text: "same", IsFinal: true}
				s.events <- ProviderStreamEvent{Err: tc.failure, IsFinal: true}
				return s, nil
			}}
			stream := openRecoveryTest(t, provider, RealtimeRecoveryConfig{MaxAttempts: 2})
			if err := waitRecoveryTest(t, stream); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
			if int(connections.Load()) != tc.connections {
				t.Fatalf("connections=%d", connections.Load())
			}
		})
	}
}

func TestRealtimeRecoveryTimeoutsAndBufferBound(t *testing.T) {
	t.Run("connect", func(t *testing.T) {
		provider := &recoveryTestProvider{start: func(ctx context.Context, _ StreamingRequest) (ProviderStream, error) {
			<-ctx.Done()
			return nil, errors.Join(ErrProviderUnavailable, ctx.Err())
		}}
		_, err := newRecoveringStream(t.Context(), provider, recoveryTestRequest(), RealtimeRecoveryConfig{ConnectTimeout: 20 * time.Millisecond})
		if !errors.Is(err, ErrRequestTimeout) {
			t.Fatalf("error=%v", err)
		}
	})
	for _, mode := range []string{"finish", "write", "buffer"} {
		t.Run(mode, func(t *testing.T) {
			provider := &recoveryTestProvider{start: func(_ context.Context, _ StreamingRequest) (ProviderStream, error) {
				s := newRecoveryTestStream()
				s.finish = func() {}
				if mode == "write" {
					s.write = func(ctx context.Context, _ StreamingAudioChunk) error {
						<-ctx.Done()
						return errors.Join(ErrRequestTimeout, ctx.Err())
					}
				}
				return s, nil
			}}
			stream := openRecoveryTest(t, provider, RealtimeRecoveryConfig{FinishTimeout: 40 * time.Millisecond, OperationTimeout: 20 * time.Millisecond, MaxAttempts: 1, MaxBufferedAudio: 100 * time.Millisecond})
			want := ErrRequestTimeout
			samples := 1600
			if mode == "buffer" {
				samples++
				want = ErrPCMBufferLimit
			}
			err := stream.WriteAudio(t.Context(), StreamingAudioChunk{Data: make([]byte, samples*2), EndSample: int64(samples)})
			if mode == "buffer" {
				if !errors.Is(err, want) {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err = stream.CloseInput(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if err = waitRecoveryTest(t, stream); !errors.Is(err, want) {
				t.Fatalf("error=%v want=%v", err, want)
			}
		})
	}
}

type recoveryContextTestStream struct {
	*recoveryTestStream
	update func(StreamingContextUpdate)
}

func (s *recoveryContextTestStream) UpdateContext(_ context.Context, update StreamingContextUpdate) error {
	s.update(update)
	return nil
}

func TestRealtimeRecoveryRestoresLatestContext(t *testing.T) {
	var connections int
	var updates []string
	provider := &recoveryTestProvider{start: func(_ context.Context, _ StreamingRequest) (ProviderStream, error) {
		connections++
		s := newRecoveryTestStream()
		generation := connections
		s.write = func(_ context.Context, _ StreamingAudioChunk) error {
			if generation == 1 {
				return ErrProviderUnavailable
			}
			if len(updates) != 2 {
				t.Errorf("audio replayed before context: %v", updates)
			}
			return nil
		}
		return &recoveryContextTestStream{s, func(update StreamingContextUpdate) {
			updates = append(updates, update.Context.Prompt)
		}}, nil
	}}
	stream := openRecoveryTest(t, provider, RealtimeRecoveryConfig{})
	updater, ok := stream.(ProviderContextUpdater)
	if !ok {
		t.Fatal("context extension lost")
	}
	if err := updater.UpdateContext(t.Context(), StreamingContextUpdate{Context: RecognitionContext{Prompt: "latest"}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.WriteAudio(t.Context(), StreamingAudioChunk{Data: make([]byte, 3200), EndSample: 1600}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseInput(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := waitRecoveryTest(t, stream); err != nil {
		t.Fatal(err)
	}
	if len(updates) != 2 || updates[0] != "latest" || updates[1] != "latest" {
		t.Fatalf("updates=%v", updates)
	}
}

func TestRealtimeSessionRecoveryWarningDoesNotHideFinalTimeout(t *testing.T) {
	var calls atomic.Int32
	provider := &recoveryTestProvider{start: func(_ context.Context, _ StreamingRequest) (ProviderStream, error) {
		s := newRecoveryTestStream()
		s.finish = func() {}
		if calls.Add(1) == 1 {
			s.events <- ProviderStreamEvent{Err: ErrProviderUnavailable, IsFinal: true}
		}
		return s, nil
	}}
	session, err := NewRealtimeSession(t.Context(), provider, RealtimeSessionConfig{
		Request: recoveryTestRequest(), Recovery: &RealtimeRecoveryConfig{Backoff: time.Millisecond, FinishTimeout: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Close)
	if err = session.Finish(t.Context(), FinalAudioChunk{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err = session.Wait(ctx); !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("error=%v", err)
	}
	var warning, fatal, completed bool
	for event := range session.Events() {
		if event.Error != nil {
			warning = warning || (event.Error.Code == "stream_reconnecting" && !event.Error.Final)
			fatal = fatal || (event.Error.Code == "request_timeout" && event.Error.Final)
		}
		completed = completed || event.Type == EventCompleted
	}
	if !warning || !fatal || !completed {
		t.Fatalf("warning=%v fatal=%v completed=%v", warning, fatal, completed)
	}
}
