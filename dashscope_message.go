package asr

import (
	"strings"
	"time"
	"unicode/utf8"
)

const (
	QwenASRMessageModel       = "qwen-audio-3.1-asr-flash-message"
	defaultQwenMessageSilence = 800 * time.Millisecond
	defaultQwenMessageVAD     = "far_field_meeting_16k"
)

func isQwenMessageModel(model string) bool {
	return model == QwenASRMessageModel || strings.HasPrefix(model, QwenASRMessageModel+"-")
}

func normalizeDashScopeMessageConfig(cfg DashScopeInferenceRealtimeConfig) (DashScopeInferenceRealtimeConfig, error) {
	cfg.VADModel = strings.TrimSpace(cfg.VADModel)
	if !isQwenMessageModel(cfg.Model) {
		if cfg.IntermediateResultEnabled != nil || cfg.DisfluencyRemovalEnabled != nil ||
			cfg.KeepDialect != nil || cfg.VADModel != "" {
			return cfg, ErrInvalidConfig
		}
		return cfg, nil
	}
	// These options belong to the older Streaming protocol, not Message.
	if cfg.SemanticPunctuationEnabled || cfg.MultiThresholdModeEnabled || strings.TrimSpace(cfg.SpecialWordFilter) != "" {
		return cfg, ErrInvalidConfig
	}
	if cfg.MaxSentenceSilence == 0 {
		cfg.MaxSentenceSilence = defaultQwenMessageSilence
	}
	if cfg.VADModel == "" {
		cfg.VADModel = defaultQwenMessageVAD
	}
	if cfg.VADModel != defaultQwenMessageVAD && cfg.VADModel != "near_meeting_16k" {
		return cfg, ErrInvalidConfig
	}
	if cfg.IntermediateResultEnabled == nil {
		// The service defaults to final-only; the SDK enables previews by default.
		cfg.IntermediateResultEnabled = new(true)
	} else {
		cfg.IntermediateResultEnabled = cloneOptionalBool(cfg.IntermediateResultEnabled)
	}
	cfg.DisfluencyRemovalEnabled = cloneOptionalBool(cfg.DisfluencyRemovalEnabled)
	cfg.KeepDialect = cloneOptionalBool(cfg.KeepDialect)
	return cfg, nil
}

func dashScopeMessageInput(value RecognitionContext, history []string) (dashScopeTaskInput, error) {
	messages := make([]RecognitionContextMessage, 0, len(value.Messages)+len(history)+1)
	if prompt := strings.TrimSpace(value.Prompt); prompt != "" {
		messages = append(messages, RecognitionContextMessage{Role: RecognitionContextRoleUser, Text: prompt})
	}
	messages = append(messages, value.Messages...)
	for _, text := range history {
		messages = append(messages, RecognitionContextMessage{Role: RecognitionContextRoleUser, Text: text})
	}
	rounds, err := qwenHTTPContextRounds(messages)
	if err != nil {
		return dashScopeTaskInput{}, err
	}
	input := dashScopeTaskInput{}
	for _, round := range rounds {
		remaining := dashScopeMaxContextRunes
		for _, message := range round {
			if remaining == 0 {
				break
			}
			text := truncateRunes(message.Text, remaining)
			remaining -= utf8.RuneCountInString(text)
			kind := dashScopeContextInputText
			if message.Role == RecognitionContextRoleAssistant {
				kind = "text"
			}
			input.Context = append(input.Context, dashScopeContextMessage{
				Role: message.Role, Content: []dashScopeContextContent{{Type: kind, Text: text}},
			})
		}
	}
	return input, nil
}

func dashScopeMessageSentences(sentence *dashScopeSentence) []TranscriptSentence {
	words := make([]SentenceWord, 0, len(sentence.Words))
	for _, word := range sentence.Words {
		words = append(words, SentenceWord{
			Text: word.Text, Punctuation: word.Punctuation,
			StartAt: float64(word.BeginTime) / 1000, EndAt: float64(word.EndTime) / 1000,
			Fixed: word.Fixed,
		})
	}
	return []TranscriptSentence{{
		ID: sentence.SentenceID, Text: sentence.Text, Final: sentence.SentenceEnd,
		StartAt: float64(sentence.BeginTime) / 1000, EndAt: float64(sentence.EndTime) / 1000,
		Words: words,
	}}
}
