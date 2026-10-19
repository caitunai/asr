package asr

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func recoveryWebSocketServer(t *testing.T, handle func(*websocket.Conn)) string {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		handle(conn)
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func TestRealtimeTransportHeartbeat(t *testing.T) {
	for _, responsive := range []bool{false, true} {
		name := "missing_pong"
		if responsive {
			name = "silence_with_pong"
		}
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			url := recoveryWebSocketServer(t, func(conn *websocket.Conn) {
				if !responsive {
					<-release
					return
				}
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			})
			defer close(release)
			conn, response, err := websocket.DefaultDialer.DialContext(t.Context(), url, nil)
			if response != nil {
				_ = response.Body.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			stop := startRealtimeHeartbeat(t.Context(), conn, 120*time.Millisecond)
			defer stop()
			result := make(chan error, 1)
			go func() { _, _, readErr := readRealtimeMessage(conn, 120*time.Millisecond); result <- readErr }()
			if responsive {
				select {
				case err = <-result:
					t.Fatalf("healthy silent connection closed: %v", err)
				case <-time.After(300 * time.Millisecond):
				}
				_ = conn.Close()
			}
			select {
			case err = <-result:
				if err == nil {
					t.Fatal("expected closed or timed out read")
				}
			case <-time.After(time.Second):
				t.Fatal("read did not terminate")
			}
		})
	}
}

func TestDashScopeRealtimeRecoversActualDisconnect(t *testing.T) {
	var connections atomic.Int32
	var replay []byte
	url := recoveryWebSocketServer(t, func(conn *websocket.Conn) {
		generation := connections.Add(1)
		var run dashScopeClientEvent
		if err := conn.ReadJSON(&run); err != nil {
			t.Error(err)
			return
		}
		id := run.Header.TaskID
		send := func(kind string, sentence *dashScopeSentence) error {
			return conn.WriteJSON(dashScopeServerEvent{Header: dashScopeTaskHeader{TaskID: id, Event: kind}, Payload: dashScopeServerPayload{Output: dashScopeServerOutput{Sentence: sentence}}})
		}
		if err := send(dashScopeEventTaskStarted, nil); err != nil {
			t.Error(err)
			return
		}
		for {
			kind, data, err := conn.ReadMessage()
			if err != nil {
				t.Error(err)
				return
			}
			if generation == 1 {
				return
			} // Drop after accepting PCM, before acknowledging it.
			if kind == websocket.BinaryMessage {
				replay = append(replay, data...)
				continue
			}
			if err = send(dashScopeEventResultGenerated, &dashScopeSentence{SentenceID: 1, Text: "recovered", SentenceEnd: true, EndTime: 200}); err != nil {
				t.Error(err)
				return
			}
			if err = send(dashScopeEventTaskFinished, nil); err != nil {
				t.Error(err)
			}
			return
		}
	})
	provider, err := NewDashScopeInferenceRealtimeProvider(DashScopeInferenceRealtimeConfig{Endpoint: url + dashScopeInferencePath, APIKey: "test", Model: QwenASRStreamingModel})
	if err != nil {
		t.Fatal(err)
	}
	stream := openRecoveryTest(t, provider, RealtimeRecoveryConfig{})
	data := bytes.Repeat([]byte{4, 0}, 3200)
	if err = stream.WriteAudio(t.Context(), StreamingAudioChunk{Data: data, EndSample: 3200}); err != nil {
		t.Fatal(err)
	}
	if err = stream.CloseInput(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = waitRecoveryTest(t, stream); err != nil {
		t.Fatal(err)
	}
	if connections.Load() != 2 || !bytes.Equal(data, replay) {
		t.Fatalf("connections=%d replay bytes=%d", connections.Load(), len(replay))
	}
	var final bool
	for event := range stream.Events() {
		if event.IsFinal && event.Text == "recovered" {
			final = true
		}
	}
	if !final {
		t.Fatal("missing recovered final")
	}
}

func TestInworldWrappedNormalClose(t *testing.T) {
	stream := &inworldRealtimeStream{ctx: t.Context(), closing: true}
	err := stream.classifyReadError(errors.Join(ErrProviderUnavailable, &websocket.CloseError{Code: websocket.CloseNormalClosure}))
	if err != nil {
		t.Fatalf("normal close: %v", err)
	}
}

func TestRealtimeRecoveryQueuesWhileReconnecting(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	provider := &recoveryTestProvider{start: func(ctx context.Context, _ StreamingRequest) (ProviderStream, error) {
		s := newRecoveryTestStream()
		if calls.Add(1) == 1 {
			s.events <- ProviderStreamEvent{Err: ErrProviderUnavailable, IsFinal: true}
			return s, nil
		}
		close(started)
		select {
		case <-release:
			return s, nil
		case <-ctx.Done():
			return nil, errors.Join(ErrRequestTimeout, ctx.Err())
		}
	}}
	stream := openRecoveryTest(t, provider, RealtimeRecoveryConfig{})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := stream.WriteAudio(ctx, StreamingAudioChunk{Data: make([]byte, 3200), EndSample: 1600}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := stream.CloseInput(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := waitRecoveryTest(t, stream); err != nil {
		t.Fatal(err)
	}
}
