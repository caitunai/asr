package asr

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/gorilla/websocket"
)

const (
	realtimeFieldMessageType = "message_type"
	realtimeFieldSampleRate  = "sample_rate"
)

// realtimeJSONReadLoop owns only the shared WebSocket read/decode lifecycle.
// Provider-specific event interpretation remains in handleEvent.
type realtimeJSONReadLoop[T any] struct {
	ctx  context.Context //nolint:containedctx // The provider stream owns this lifecycle.
	conn *websocket.Conn

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
	for {
		_, payload, err := loop.conn.ReadMessage()
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
