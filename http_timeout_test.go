package asr

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientTimeoutIncludesConcurrencyQueue(t *testing.T) {
	provider := &testProvider{transcribe: func(context.Context, ProviderRequest) (ProviderResult, error) { return ProviderResult{Text: "ok"}, nil }}
	client, err := NewClient(provider, ClientConfig{RequestTimeout: 20 * time.Millisecond, MaxConcurrency: 1, AudioFormat: AudioFormatRawPCM16})
	if err != nil {
		t.Fatal(err)
	}
	request := TranscriptionRequest{RequestID: "request", SessionID: "session", Samples: []float32{0}, SampleRate: 16000, Channels: 1}
	client.semaphore <- struct{}{}
	_, err = client.Transcribe(t.Context(), request)
	if !errors.Is(err, ErrRequestTimeout) || !errors.Is(err, ErrOverloaded) {
		t.Fatalf("error=%v", err)
	}
	<-client.semaphore
	if _, err = client.Transcribe(t.Context(), request); err != nil {
		t.Fatalf("subsequent request: %v", err)
	}
}

func TestHTTPRequestFallbackPreservesCallerBudget(t *testing.T) {
	ctx, cancel := httpRequestContext(t.Context())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > defaultRequestTimeout || time.Until(deadline) < 7*time.Second {
		t.Fatal("missing default deadline")
	}
	parent, stop := context.WithTimeout(t.Context(), time.Minute)
	defer stop()
	ctx2, cancel2 := httpRequestContext(parent)
	defer cancel2()
	want, _ := parent.Deadline()
	got, _ := ctx2.Deadline()
	if !got.Equal(want) {
		t.Fatalf("deadline=%v want=%v", got, want)
	}
}

func TestHTTPTimeoutCoversHeadersAndBody(t *testing.T) {
	for _, body := range []bool{false, true} {
		name := "headers"
		if body {
			name = "body"
		}
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if body {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"text":"`))
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			provider, err := NewGenericHTTPProvider(GenericHTTPConfig{Name: "test", BaseURL: server.URL, Path: "/transcribe", Model: "test", APIKey: "test", AllowInsecureHTTP: true})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			_, err = provider.Transcribe(ctx, ProviderRequest{RequestID: "test", SessionID: "session", Audio: AudioPayload{Data: []byte{0, 0}, Format: AudioFormatRawPCM16, SampleRate: 16000, Channels: 1}})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
