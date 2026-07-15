package asr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

func EncodeAudio(samples []float32, sampleRate, channels int, format AudioFormat) (AudioPayload, error) {
	if len(samples) == 0 || sampleRate <= 0 || channels != 1 {
		return AudioPayload{}, ErrInvalidRequest
	}
	pcm := encodePCM16LE(samples)
	switch format {
	case AudioFormatRawPCM16:
		return AudioPayload{
			Data:       pcm,
			Format:     format,
			SampleRate: sampleRate,
			Channels:   channels,
		}, nil
	case AudioFormatWAVPCM16:
		wav, err := encodeWAVPCM16(pcm, sampleRate, channels)
		if err != nil {
			return AudioPayload{}, err
		}
		return AudioPayload{
			Data:       wav,
			Format:     format,
			SampleRate: sampleRate,
			Channels:   channels,
		}, nil
	default:
		return AudioPayload{}, ErrInvalidRequest
	}
}

func encodePCM16LE(samples []float32) []byte {
	data := make([]byte, len(samples)*2)
	for index, sample := range samples {
		sample = min(max(sample, -1), 1)
		var value int16
		if sample <= -1 {
			value = math.MinInt16
		} else {
			value = int16(math.Round(float64(sample) * math.MaxInt16))
		}
		binary.LittleEndian.PutUint16(data[index*2:index*2+2], uint16(value))
	}
	return data
}

func encodeWAVPCM16(pcm []byte, sampleRate, channels int) ([]byte, error) {
	if len(pcm) > math.MaxUint32-36 || sampleRate <= 0 || channels <= 0 {
		return nil, ErrInvalidRequest
	}
	var output bytes.Buffer
	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], uint32(36+len(pcm))) //nolint:gosec // checked against MaxUint32 above.
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16)
	binary.LittleEndian.PutUint16(header[20:22], 1)
	binary.LittleEndian.PutUint16(header[22:24], uint16(channels))   //nolint:gosec // channels is validated above.
	binary.LittleEndian.PutUint32(header[24:28], uint32(sampleRate)) //nolint:gosec // sample rate is validated above.
	byteRate := sampleRate * channels * 2
	binary.LittleEndian.PutUint32(header[28:32], uint32(byteRate))   //nolint:gosec // validated audio values are small.
	binary.LittleEndian.PutUint16(header[32:34], uint16(channels*2)) //nolint:gosec // validated audio values are small.
	binary.LittleEndian.PutUint16(header[34:36], 16)
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], uint32(len(pcm))) //nolint:gosec // checked against MaxUint32 above.
	if _, err := output.Write(header); err != nil {
		return nil, errors.Join(ErrInvalidRequest, err)
	}
	if _, err := output.Write(pcm); err != nil {
		return nil, errors.Join(ErrInvalidRequest, err)
	}
	return output.Bytes(), nil
}
