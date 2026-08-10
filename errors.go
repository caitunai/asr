package asr

import "errors"

var (
	ErrInvalidConfig            = errors.New("asr config invalid")
	ErrInvalidRequest           = errors.New("asr request invalid")
	ErrProviderUnavailable      = errors.New("asr provider unavailable")
	ErrProviderRequest          = errors.New("asr provider request failed")
	ErrProviderResponse         = errors.New("asr provider response invalid")
	ErrNoSpeech                 = errors.New("asr no speech detected")
	ErrRequestTimeout           = errors.New("asr request timeout")
	ErrRequestSuperseded        = errors.New("asr request superseded by newer audio")
	ErrRateLimited              = errors.New("asr provider rate limited")
	ErrUnauthorized             = errors.New("asr provider unauthorized")
	ErrOverloaded               = errors.New("asr service overloaded")
	ErrSessionClosed            = errors.New("asr session closed")
	ErrSegmentInvalid           = errors.New("asr segment invalid")
	ErrWindowTooLong            = errors.New("asr window duration exceeded")
	ErrAlignmentRejected        = errors.New("asr text alignment rejected")
	ErrLanguageInvalid          = errors.New("asr language tag invalid")
	ErrPCMBufferLimit           = errors.New("asr pcm buffer limit exceeded")
	ErrStreamingBackpressure    = errors.New("asr streaming backpressure")
	ErrContextUpdateUnsupported = errors.New("asr context update unsupported")
)
