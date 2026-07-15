package asr

import (
	"errors"
	"strings"

	"golang.org/x/text/language"
)

const automaticLanguage = "auto"

// NormalizeLanguageTag canonicalizes a BCP 47 tag. The ASR-specific "auto"
// sentinel is preserved for providers that support automatic detection.
func NormalizeLanguageTag(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, automaticLanguage) {
		return automaticLanguage, nil
	}
	if strings.ContainsAny(value, "_ \t\r\n") {
		return "", ErrLanguageInvalid
	}
	tag, err := language.Parse(value)
	if err != nil {
		return "", errors.Join(ErrLanguageInvalid, err)
	}
	return tag.String(), nil
}

func normalizeLanguageHints(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(values))
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		tag, err := NormalizeLanguageTag(value)
		if err != nil || tag == automaticLanguage {
			if err != nil {
				return nil, err
			}
			return nil, ErrLanguageInvalid
		}
		if _, exists := seen[tag]; exists {
			continue
		}
		seen[tag] = struct{}{}
		normalized = append(normalized, tag)
	}
	return normalized, nil
}
