package asr

import (
	"errors"
	"strings"
	"testing"
)

func TestAlignerMultilingualSuffixPrefix(t *testing.T) {
	tests := []struct {
		name             string
		previous         string
		current          string
		wantPreviousHead string
		wantShared       string
	}{
		{
			name:             "Chinese and Arabic",
			previous:         "今天发布星河系统",
			current:          "星河系统 يدعم العربية",
			wantPreviousHead: "今天发布",
			wantShared:       "星河系统",
		},
		{
			name:             "Thai without spaces",
			previous:         "เริ่มต้นสวัสดีโลก",
			current:          "สวัสดีโลกต่อไป",
			wantPreviousHead: "เริ่มต้น",
			wantShared:       "สวัสดีโลก",
		},
		{
			name:             "Unicode canonical equivalence",
			previous:         "intro Café世界",
			current:          "Cafe\u0301世界 next",
			wantPreviousHead: "intro ",
			wantShared:       "Cafe\u0301世界",
		},
	}
	aligner := NewAligner(nil, AlignmentConfig{})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			alignment, err := aligner.AlignSuffixPrefix(test.previous, test.current, nil)
			if err != nil {
				t.Fatalf("align text: %v (%+v)", err, alignment.Info)
			}
			if got := test.previous[:alignment.PreviousStartByte]; got != test.wantPreviousHead {
				t.Fatalf("previous head = %q, want %q", got, test.wantPreviousHead)
			}
			if got := test.current[:alignment.CurrentEndByte]; got != test.wantShared {
				t.Fatalf("shared text = %q, want %q", got, test.wantShared)
			}
			if !alignment.Info.Reliable {
				t.Fatalf("alignment should be reliable: %+v", alignment.Info)
			}
		})
	}
}

func TestAlignerRejectsWeakOrProtectedOverlap(t *testing.T) {
	aligner := NewAligner(nil, AlignmentConfig{})
	_, err := aligner.AlignSuffixPrefix("alpha 1234", "1235 omega", []string{"1234", "1235"})
	if !errors.Is(err, ErrAlignmentRejected) {
		t.Fatalf("error = %v, want ErrAlignmentRejected", err)
	}

	_, err = aligner.AlignSuffixPrefix("abc", strings.Repeat("x", 5), nil)
	if !errors.Is(err, ErrAlignmentRejected) {
		t.Fatalf("weak overlap error = %v, want ErrAlignmentRejected", err)
	}
}
