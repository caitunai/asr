package asr

import (
	"errors"
	"testing"
)

func TestNormalizeLanguageTag(t *testing.T) {
	tests := map[string]string{
		"":           "auto",
		"AUTO":       "auto",
		"zh-hans-cn": "zh-Hans-CN",
		"EN-us":      "en-US",
		"sr-Cyrl":    "sr-Cyrl",
	}
	for input, want := range tests {
		got, err := NormalizeLanguageTag(input)
		if err != nil {
			t.Fatalf("normalize %q: %v", input, err)
		}
		if got != want {
			t.Fatalf("normalize %q = %q, want %q", input, got, want)
		}
	}
	if _, err := NormalizeLanguageTag("not_a_language"); !errors.Is(err, ErrLanguageInvalid) {
		t.Fatalf("invalid tag error = %v, want ErrLanguageInvalid", err)
	}
}
