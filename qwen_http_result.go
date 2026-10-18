package asr

import "slices"

func qwenHTTPResultSentences(output qwenHTTPNativeOutput) []TranscriptSentence {
	source := output.Sentences
	if len(source) == 0 {
		switch {
		case output.Sentence.Text != "":
			source = []qwenHTTPNativeSentence{output.Sentence}
		case output.Output.Sentence.Text != "":
			source = []qwenHTTPNativeSentence{output.Output.Sentence}
		}
	}
	result := make([]TranscriptSentence, 0, len(source))
	for _, sentence := range source {
		words := make([]SentenceWord, 0, len(sentence.Words))
		for _, word := range sentence.Words {
			words = append(words, SentenceWord{
				Text: word.Text, Punctuation: word.Punctuation,
				StartAt: float64(word.BeginTime) / 1000, EndAt: float64(word.EndTime) / 1000,
				Fixed: word.Fixed, SpeakerID: word.SpeakerID,
			})
		}
		result = append(result, TranscriptSentence{
			ID: sentence.SentenceID, Text: sentence.Text,
			StartAt: float64(sentence.BeginTime) / 1000, EndAt: float64(sentence.EndTime) / 1000,
			Final: sentence.SentenceEnd, ChannelID: sentence.ChannelID,
			SpeakerID: sentence.SpeakerID, Words: words,
		})
	}
	return result
}

func cloneTranscriptSentences(value []TranscriptSentence) []TranscriptSentence {
	result := slices.Clone(value)
	for i := range result {
		if result[i].SpeakerID != nil {
			result[i].SpeakerID = new(*result[i].SpeakerID)
		}
		result[i].Words = slices.Clone(result[i].Words)
		for j := range result[i].Words {
			if result[i].Words[j].SpeakerID != nil {
				result[i].Words[j].SpeakerID = new(*result[i].Words[j].SpeakerID)
			}
		}
	}
	return result
}

func cloneRecognitionUsage(value *RecognitionUsage) *RecognitionUsage {
	if value == nil {
		return nil
	}
	return new(*value)
}
