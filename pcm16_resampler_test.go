package asr

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestPCM16StreamResamplerPreservesChunkContinuity(t *testing.T) {
	t.Parallel()
	resampler, err := newPCM16StreamResampler(16000, 24000)
	if err != nil {
		t.Fatalf("new resampler: %v", err)
	}

	first, err := resampler.Push(pcm16TestData([]int16{0, 1000}))
	if err != nil {
		t.Fatalf("push first chunk: %v", err)
	}
	second, err := resampler.Push(pcm16TestData([]int16{2000, 3000}))
	if err != nil {
		t.Fatalf("push second chunk: %v", err)
	}
	tail, err := resampler.Flush()
	if err != nil {
		t.Fatalf("flush resampler: %v", err)
	}

	combined := append(append(first, second...), tail...)
	if len(combined) != 6*2 {
		t.Fatalf("output bytes = %d, want 12", len(combined))
	}
	values := pcm16TestSamples(combined)
	if values[0] != 0 || values[len(values)-1] != 3000 {
		t.Fatalf("resampled endpoints = %v", values)
	}
	if _, err := resampler.Push(pcm16TestData([]int16{4000})); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("push after flush error = %v", err)
	}
}

func pcm16TestData(samples []int16) []byte {
	data := make([]byte, len(samples)*2)
	for index, sample := range samples {
		binary.LittleEndian.PutUint16(data[index*2:index*2+2], uint16(sample))
	}
	return data
}

func pcm16TestSamples(data []byte) []int16 {
	samples := make([]int16, len(data)/2)
	for index := range samples {
		samples[index] = int16(binary.LittleEndian.Uint16(data[index*2 : index*2+2]))
	}
	return samples
}
