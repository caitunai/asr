package asr

import "encoding/binary"

// pcm16StreamResampler performs continuous linear interpolation across chunk
// boundaries. It is intentionally small because realtime providers receive
// already denoised mono speech rather than music-quality source audio.
type pcm16StreamResampler struct {
	inputRate           int64
	outputRate          int64
	inputSamples        int64
	outputSamples       int64
	nextOutputNumerator int64
	previous            int16
	havePrevious        bool
	closed              bool
}

func newPCM16StreamResampler(inputRate, outputRate int) (*pcm16StreamResampler, error) {
	if inputRate <= 0 || outputRate <= 0 {
		return nil, ErrInvalidConfig
	}
	return &pcm16StreamResampler{
		inputRate:  int64(inputRate),
		outputRate: int64(outputRate),
	}, nil
}

func (r *pcm16StreamResampler) Push(data []byte) ([]byte, error) {
	if r == nil || r.closed {
		return nil, ErrSessionClosed
	}
	if len(data) == 0 {
		return nil, nil
	}
	if len(data)%2 != 0 {
		return nil, ErrInvalidRequest
	}

	estimated := int((int64(len(data)/2)*r.outputRate+r.inputRate-1)/r.inputRate) + 2
	output := make([]int16, 0, estimated)
	for offset := 0; offset < len(data); offset += 2 {
		current := int16(binary.LittleEndian.Uint16(data[offset : offset+2]))
		currentIndex := r.inputSamples
		if !r.havePrevious {
			r.previous = current
			r.havePrevious = true
			output = append(output, current)
			r.outputSamples++
			r.nextOutputNumerator += r.inputRate
			r.inputSamples++
			continue
		}

		intervalStart := (currentIndex - 1) * r.outputRate
		intervalEnd := currentIndex * r.outputRate
		for r.nextOutputNumerator <= intervalEnd {
			fraction := r.nextOutputNumerator - intervalStart
			value := (int64(r.previous)*(r.outputRate-fraction) +
				int64(current)*fraction) / r.outputRate
			output = append(output, int16(value))
			r.outputSamples++
			r.nextOutputNumerator += r.inputRate
		}
		r.previous = current
		r.inputSamples++
	}
	return encodePCM16Samples(output), nil
}

func (r *pcm16StreamResampler) Flush() ([]byte, error) {
	if r == nil || r.closed {
		return nil, ErrSessionClosed
	}
	r.closed = true
	if !r.havePrevious {
		return nil, nil
	}
	target := (r.inputSamples*r.outputRate + r.inputRate/2) / r.inputRate
	if target <= r.outputSamples {
		return nil, nil
	}
	output := make([]int16, target-r.outputSamples)
	for index := range output {
		output[index] = r.previous
	}
	r.outputSamples = target
	return encodePCM16Samples(output), nil
}

func encodePCM16Samples(samples []int16) []byte {
	if len(samples) == 0 {
		return nil
	}
	data := make([]byte, len(samples)*2)
	for index, sample := range samples {
		binary.LittleEndian.PutUint16(data[index*2:index*2+2], uint16(sample))
	}
	return data
}
