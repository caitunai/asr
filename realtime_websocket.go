package asr

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/gorilla/websocket"
)

const (
	realtimeFieldMessageType = "message_type"
	realtimeFieldSampleRate  = "sample_rate"
)

// realtimeJSONReadLoop owns only the shared WebSocket read/decode lifecycle.
// Provider-specific event interpretation remains in handleEvent.
type realtimeJSONReadLoop[T any] struct {
	ctx         context.Context //nolint:containedctx // The provider stream owns this lifecycle.
	conn        *websocket.Conn
	readTimeout time.Duration

	cancel            context.CancelFunc
	events            chan ProviderStreamEvent
	done              chan struct{}
	hasWaitResult     func() bool
	setWaitResult     func(error)
	currentWaitError  func() error
	signalUpdated     func(error)
	emit              func(ProviderStreamEvent)
	handleEvent       func(T)
	classifyReadError func(error) error
}

func (loop realtimeJSONReadLoop[T]) run() {
	defer loop.cancel()
	defer func() { _ = loop.conn.Close() }()
	defer close(loop.events)
	defer close(loop.done)
	stopHeartbeat := startRealtimeHeartbeat(loop.ctx, loop.conn, loop.readTimeout)
	defer stopHeartbeat()
	for {
		_, payload, err := readRealtimeMessage(loop.conn, loop.readTimeout)
		if err != nil {
			if !loop.hasWaitResult() {
				readErr := errors.Join(ErrProviderUnavailable, err)
				if loop.ctx.Err() != nil {
					readErr = errors.Join(ErrSessionClosed, loop.ctx.Err())
				}
				if loop.classifyReadError != nil {
					readErr = loop.classifyReadError(err)
				}
				loop.setWaitResult(readErr)
			}
			loop.signalUpdated(loop.currentWaitError())
			return
		}
		var event T
		if err := json.Unmarshal(payload, &event); err != nil {
			loop.emit(ProviderStreamEvent{Err: errors.Join(ErrProviderResponse, err)})
			continue
		}
		loop.handleEvent(event)
		if loop.hasWaitResult() {
			return
		}
	}
}

const defaultRealtimeReadTimeout = 45 * time.Second

func realtimeReadTimeout(value time.Duration) time.Duration {
	if value == 0 {
		return defaultRealtimeReadTimeout
	}
	return value
}

func readRealtimeMessage(conn *websocket.Conn, timeout time.Duration) (int, []byte, error) {
	if timeout = realtimeReadTimeout(timeout); timeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return 0, nil, errors.Join(ErrProviderUnavailable, err)
		}
	}
	kind, payload, err := conn.ReadMessage()
	if err != nil {
		return kind, payload, errors.Join(ErrProviderUnavailable, err)
	}
	return kind, payload, nil
}

func startRealtimeHeartbeat(ctx context.Context, conn *websocket.Conn, timeout time.Duration) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	timeout = realtimeReadTimeout(timeout)
	if timeout < 0 {
		return cancel
	}
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(timeout)) })
	go func() {
		ticker := time.NewTicker(max(time.Millisecond, timeout/3))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Gorilla permits WriteControl concurrently with the single data writer.
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(min(timeout/3, 5*time.Second))); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()
	return cancel
}

func realtimeDialer(timeout time.Duration) websocket.Dialer {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = timeout
	return dialer
}
