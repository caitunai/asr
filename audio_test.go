package asr

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestEncodeAudioPCMAndWAV(t *testing.T) {
	samples := []float32{-2, -1, -0.5, 0, 0.5, 1, 2}
	raw, err := EncodeAudio(samples, 16_000, 1, AudioFormatRawPCM16)
	if err != nil {
		t.Fatalf("encode raw PCM: %v", err)
	}
	if len(raw.Data) != len(samples)*2 {
		t.Fatalf("raw PCM bytes = %d, want %d", len(raw.Data), len(samples)*2)
	}
	if got := int16(binary.LittleEndian.Uint16(raw.Data[:2])); got != math.MinInt16 {
		t.Fatalf("first PCM sample = %d, want %d", got, math.MinInt16)
	}
	if got := int16(binary.LittleEndian.Uint16(raw.Data[len(raw.Data)-2:])); got != math.MaxInt16 {
		t.Fatalf("last PCM sample = %d, want %d", got, math.MaxInt16)
	}

	wav, err := EncodeAudio(samples, 16_000, 1, AudioFormatWAVPCM16)
	if err != nil {
		t.Fatalf("encode WAV: %v", err)
	}
	if string(wav.Data[:4]) != "RIFF" || string(wav.Data[8:12]) != "WAVE" {
		t.Fatalf("invalid WAV header: %q", wav.Data[:12])
	}
	if got := binary.LittleEndian.Uint32(wav.Data[24:28]); got != 16_000 {
		t.Fatalf("WAV sample rate = %d, want 16000", got)
	}
	if got := binary.LittleEndian.Uint32(wav.Data[40:44]); got != uint32(len(raw.Data)) {
		t.Fatalf("WAV data bytes = %d, want %d", got, len(raw.Data))
	}
}

func TestEncodeAudioRejectsInvalidInput(t *testing.T) {
	_, err := EncodeAudio([]float32{0}, 16_000, 2, AudioFormatWAVPCM16)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}
}
