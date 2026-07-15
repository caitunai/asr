package asr

import (
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/rivo/uniseg"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	defaultAlignmentMaxTokens  = 512
	defaultAlignmentMinAnchors = 4
	defaultAlignmentMinScore   = 0.82
	defaultAlignmentCoverage   = 0.60
	alignmentMatchScore        = 3
	alignmentMismatchPenalty   = -3
	alignmentGapPenalty        = -2
)

type AlignmentConfig struct {
	MaxTokens   int
	MinAnchors  int
	MinScore    float64
	MinCoverage float64
}

type TextToken struct {
	Original  string
	Key       string
	StartByte int
	EndByte   int
	Protected bool
}

type Tokenizer interface {
	Tokenize(text string, protectedTerms []string) []TextToken
}

type GraphemeTokenizer struct{}

type TextAlignment struct {
	PreviousStartByte int
	CurrentEndByte    int
	Info              AlignmentInfo
}

type Aligner struct {
	tokenizer Tokenizer
	cfg       AlignmentConfig
}

type alignmentCell struct {
	score int
	op    byte
}

const (
	alignmentOpNone byte = iota
	alignmentOpDiagonal
	alignmentOpUp
	alignmentOpLeft
)

func NewAligner(tokenizer Tokenizer, cfg AlignmentConfig) *Aligner {
	if tokenizer == nil {
		tokenizer = GraphemeTokenizer{}
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = defaultAlignmentMaxTokens
	}
	if cfg.MinAnchors <= 0 {
		cfg.MinAnchors = defaultAlignmentMinAnchors
	}
	if cfg.MinScore <= 0 || cfg.MinScore > 1 {
		cfg.MinScore = defaultAlignmentMinScore
	}
	if cfg.MinCoverage <= 0 || cfg.MinCoverage > 1 {
		cfg.MinCoverage = defaultAlignmentCoverage
	}
	return &Aligner{tokenizer: tokenizer, cfg: cfg}
}

func (a *Aligner) AlignSuffixPrefix(previous, current string, protectedTerms []string) (TextAlignment, error) {
	if a == nil || strings.TrimSpace(previous) == "" || strings.TrimSpace(current) == "" {
		return rejectedAlignment("empty_text"), ErrAlignmentRejected
	}
	previousTokens := a.tokenizer.Tokenize(previous, protectedTerms)
	currentTokens := a.tokenizer.Tokenize(current, protectedTerms)
	if len(previousTokens) == 0 || len(currentTokens) == 0 {
		return rejectedAlignment("no_meaningful_tokens"), ErrAlignmentRejected
	}
	if len(previousTokens) > a.cfg.MaxTokens {
		previousTokens = previousTokens[len(previousTokens)-a.cfg.MaxTokens:]
	}
	if len(currentTokens) > a.cfg.MaxTokens {
		currentTokens = currentTokens[:a.cfg.MaxTokens]
	}
	return a.align(previousTokens, currentTokens)
}

func (a *Aligner) align(previous, current []TextToken) (TextAlignment, error) {
	rows := len(previous) + 1
	columns := len(current) + 1
	cells := make([]alignmentCell, rows*columns)
	cell := func(row, column int) *alignmentCell {
		return &cells[row*columns+column]
	}
	for row := 1; row < rows; row++ {
		cell(row, 0).score = 0
		cell(row, 0).op = alignmentOpNone
	}
	for column := 1; column < columns; column++ {
		cell(0, column).score = column * alignmentGapPenalty
		cell(0, column).op = alignmentOpLeft
	}
	for row := 1; row < rows; row++ {
		for column := 1; column < columns; column++ {
			matchScore := alignmentMismatchPenalty
			if previous[row-1].Key == current[column-1].Key {
				matchScore = alignmentMatchScore
			}
			diagonal := cell(row-1, column-1).score + matchScore
			up := cell(row-1, column).score + alignmentGapPenalty
			left := cell(row, column-1).score + alignmentGapPenalty
			best := diagonal
			op := alignmentOpDiagonal
			if up > best {
				best = up
				op = alignmentOpUp
			}
			if left > best {
				best = left
				op = alignmentOpLeft
			}
			cell(row, column).score = best
			cell(row, column).op = op
		}
	}

	bestColumn := 1
	bestScore := cell(len(previous), bestColumn).score
	for column := 2; column <= len(current); column++ {
		if score := cell(len(previous), column).score; score > bestScore {
			bestScore = score
			bestColumn = column
		}
	}

	row := len(previous)
	column := bestColumn
	anchors := 0
	alignedSteps := 0
	protectedConflict := false
	for column > 0 && row > 0 {
		currentCell := cell(row, column)
		switch currentCell.op {
		case alignmentOpDiagonal:
			previousToken := previous[row-1]
			currentToken := current[column-1]
			if previousToken.Key == currentToken.Key {
				anchors++
			} else if previousToken.Protected || currentToken.Protected {
				protectedConflict = true
			}
			row--
			column--
			alignedSteps++
		case alignmentOpUp:
			if previous[row-1].Protected {
				protectedConflict = true
			}
			row--
			alignedSteps++
		case alignmentOpLeft:
			if current[column-1].Protected {
				protectedConflict = true
			}
			column--
			alignedSteps++
		default:
			column = 0
		}
	}
	previousStart := row
	if bestColumn <= 0 || previousStart >= len(previous) {
		return rejectedAlignment("boundary_not_found"), ErrAlignmentRejected
	}
	previousSpan := len(previous) - previousStart
	denominator := max(previousSpan, bestColumn)
	if denominator <= 0 {
		return rejectedAlignment("empty_overlap"), ErrAlignmentRejected
	}
	score := float64(anchors) / float64(denominator)
	coverage := float64(anchors) / float64(max(1, alignedSteps))
	reliable := !protectedConflict && anchors >= a.cfg.MinAnchors &&
		score >= a.cfg.MinScore && coverage >= a.cfg.MinCoverage
	rejectReason := ""
	switch {
	case protectedConflict:
		rejectReason = "protected_token_conflict"
	case anchors < a.cfg.MinAnchors:
		rejectReason = "insufficient_anchors"
	case score < a.cfg.MinScore:
		rejectReason = "score_below_threshold"
	case coverage < a.cfg.MinCoverage:
		rejectReason = "coverage_below_threshold"
	}
	result := TextAlignment{
		PreviousStartByte: previous[previousStart].StartByte,
		CurrentEndByte:    current[bestColumn-1].EndByte,
		Info: AlignmentInfo{
			Strategy:     "grapheme_semiglobal",
			Score:        roundAlignmentMetric(score),
			Coverage:     roundAlignmentMetric(coverage),
			AnchorCount:  anchors,
			Reliable:     reliable,
			RejectReason: rejectReason,
		},
	}
	if !reliable {
		return result, ErrAlignmentRejected
	}
	return result, nil
}

func (GraphemeTokenizer) Tokenize(text string, protectedTerms []string) []TextToken {
	protectedSpans := findProtectedSpans(text, protectedTerms)
	graphemes := uniseg.NewGraphemes(text)
	tokens := make([]TextToken, 0, utf8.RuneCountInString(text))
	for graphemes.Next() {
		value := graphemes.Str()
		start, end := graphemes.Positions()
		first, _ := utf8.DecodeRuneInString(value)
		if unicode.IsSpace(first) || unicode.IsPunct(first) || unicode.IsSymbol(first) {
			continue
		}
		key := cases.Fold().String(norm.NFC.String(value))
		if key == "" {
			continue
		}
		tokens = append(tokens, TextToken{
			Original:  value,
			Key:       key,
			StartByte: start,
			EndByte:   end,
			Protected: unicode.IsDigit(first) || spanProtected(start, end, protectedSpans),
		})
	}
	return tokens
}

type textSpan struct {
	start int
	end   int
}

func findProtectedSpans(text string, terms []string) []textSpan {
	spans := make([]textSpan, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		remaining := text
		offset := 0
		for {
			index := strings.Index(remaining, term)
			if index < 0 {
				break
			}
			start := offset + index
			spans = append(spans, textSpan{start: start, end: start + len(term)})
			next := index + len(term)
			offset += next
			remaining = remaining[next:]
		}
	}
	return spans
}

func spanProtected(start, end int, spans []textSpan) bool {
	for _, span := range spans {
		if start < span.end && end > span.start {
			return true
		}
	}
	return false
}

func rejectedAlignment(reason string) TextAlignment {
	return TextAlignment{
		Info: AlignmentInfo{
			Strategy:     "grapheme_semiglobal",
			RejectReason: reason,
		},
	}
}

func roundAlignmentMetric(value float64) float64 {
	return math.Round(value*10_000) / 10_000
}
