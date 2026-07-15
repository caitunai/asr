package asr

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

// SchedulerStats is a lock-free snapshot of one ScheduledRecognizer.
type SchedulerStats struct {
	Superseded               uint64
	AuthoritativePending     int64
	AuthoritativeOutstanding int64
	PreviewPending           bool
}

// ScheduledRecognizer serializes requests while preserving authoritative work
// and coalescing pending preview work to the newest audio boundary.
type ScheduledRecognizer struct {
	base                     Recognizer
	cancel                   context.CancelFunc
	requests                 chan *schedulerJob
	done                     chan struct{}
	superseded               atomic.Uint64
	authoritativePending     atomic.Int64
	authoritativeOutstanding atomic.Int64
	close                    sync.Once
	previewPending           atomic.Bool
}

type schedulerJob struct {
	ctx      context.Context //nolint:containedctx // Job keeps caller cancellation until execution.
	request  *TranscriptionRequest
	response chan schedulerResponse
}

type schedulerResponse struct {
	err    error
	result TranscriptionResult
}

type schedulerCompletion struct {
	job      *schedulerJob
	response schedulerResponse
}

// NewScheduledRecognizer wraps base with a session-scoped request scheduler.
// Canceling ctx or calling Close stops the scheduler and its active request.
func NewScheduledRecognizer(ctx context.Context, base Recognizer) (*ScheduledRecognizer, error) {
	if ctx == nil || base == nil || base.ProviderName() == "" || base.ProviderModel() == "" {
		return nil, ErrInvalidConfig
	}
	schedulerCtx, cancel := context.WithCancel(ctx)
	recognizer := &ScheduledRecognizer{
		base:     base,
		cancel:   cancel,
		requests: make(chan *schedulerJob),
		done:     make(chan struct{}),
	}
	go recognizer.run(schedulerCtx)
	return recognizer, nil
}

// ProviderName returns the wrapped recognizer's provider name.
func (r *ScheduledRecognizer) ProviderName() string {
	if r == nil || r.base == nil {
		return ""
	}
	return r.base.ProviderName()
}

// ProviderModel returns the wrapped recognizer's model name.
func (r *ScheduledRecognizer) ProviderModel() string {
	if r == nil || r.base == nil {
		return ""
	}
	return r.base.ProviderModel()
}

// Transcribe schedules one request and waits for its result.
func (r *ScheduledRecognizer) Transcribe(
	ctx context.Context,
	request TranscriptionRequest,
) (TranscriptionResult, error) {
	if r == nil {
		return TranscriptionResult{}, ErrSessionClosed
	}
	if request.Authoritative {
		r.authoritativeOutstanding.Add(1)
		defer r.authoritativeOutstanding.Add(-1)
	}
	job := &schedulerJob{
		ctx:      ctx,
		request:  &request,
		response: make(chan schedulerResponse, 1),
	}
	select {
	case r.requests <- job:
	case <-ctx.Done():
		return TranscriptionResult{}, errors.Join(ErrSessionClosed, ctx.Err())
	case <-r.done:
		return TranscriptionResult{}, ErrSessionClosed
	}
	select {
	case response := <-job.response:
		return response.result, response.err
	case <-ctx.Done():
		return TranscriptionResult{}, errors.Join(ErrSessionClosed, ctx.Err())
	case <-r.done:
		return TranscriptionResult{}, ErrSessionClosed
	}
}

// Stats returns the current scheduler counters and pending state.
func (r *ScheduledRecognizer) Stats() SchedulerStats {
	if r == nil {
		return SchedulerStats{}
	}
	return SchedulerStats{
		Superseded:               r.superseded.Load(),
		AuthoritativePending:     r.authoritativePending.Load(),
		AuthoritativeOutstanding: r.authoritativeOutstanding.Load(),
		PreviewPending:           r.previewPending.Load(),
	}
}

// Close stops the scheduler and waits for its scheduling loop to exit.
func (r *ScheduledRecognizer) Close() {
	if r == nil {
		return
	}
	r.close.Do(r.cancel)
	<-r.done
}

func (r *ScheduledRecognizer) run(ctx context.Context) {
	defer close(r.done)
	completions := make(chan schedulerCompletion, 1)
	var active *schedulerJob
	var activeCancel context.CancelFunc
	var activeSuperseded bool
	var pendingAuthoritative []*schedulerJob
	var pendingPreview *schedulerJob
	start := func(job *schedulerJob) {
		active = job
		activeSuperseded = false
		executionCtx, cancel := context.WithCancel(job.ctx)
		activeCancel = cancel
		go func() {
			result, err := r.base.Transcribe(executionCtx, *job.request)
			completion := schedulerCompletion{job: job, response: schedulerResponse{result: result, err: err}}
			select {
			case completions <- completion:
			case <-ctx.Done():
			}
		}()
	}
	respond := func(job *schedulerJob, response schedulerResponse) {
		if job == nil {
			return
		}
		select {
		case job.response <- response:
		default:
		}
	}
	discardPendingPreview := func() {
		if pendingPreview == nil {
			return
		}
		r.superseded.Add(1)
		r.previewPending.Store(false)
		respond(pendingPreview, schedulerResponse{err: ErrRequestSuperseded})
		pendingPreview = nil
	}
	startNext := func() {
		if len(pendingAuthoritative) > 0 {
			next := pendingAuthoritative[0]
			pendingAuthoritative[0] = nil
			pendingAuthoritative = pendingAuthoritative[1:]
			r.authoritativePending.Add(-1)
			start(next)
			return
		}
		if pendingPreview != nil {
			next := pendingPreview
			pendingPreview = nil
			r.previewPending.Store(false)
			start(next)
		}
	}
	for {
		select {
		case <-ctx.Done():
			if activeCancel != nil {
				activeCancel()
			}
			for _, job := range pendingAuthoritative {
				respond(job, schedulerResponse{err: ErrSessionClosed})
			}
			respond(pendingPreview, schedulerResponse{err: ErrSessionClosed})
			return
		case job := <-r.requests:
			if active == nil {
				start(job)
				continue
			}
			if job.request.Authoritative {
				discardPendingPreview()
				pendingAuthoritative = append(pendingAuthoritative, job)
				sort.SliceStable(pendingAuthoritative, func(left, right int) bool {
					return pendingAuthoritative[left].request.AudioEndAt < pendingAuthoritative[right].request.AudioEndAt
				})
				r.authoritativePending.Add(1)
				if !active.request.Authoritative && activeCancel != nil {
					activeSuperseded = true
					activeCancel()
				}
				continue
			}
			if pendingPreview == nil || schedulerRequestIsNewer(*job.request, *pendingPreview.request) {
				if pendingPreview != nil {
					r.superseded.Add(1)
				}
				respond(pendingPreview, schedulerResponse{err: ErrRequestSuperseded})
				pendingPreview = job
				r.previewPending.Store(true)
			} else {
				r.superseded.Add(1)
				respond(job, schedulerResponse{err: ErrRequestSuperseded})
			}
		case completion := <-completions:
			if active != completion.job {
				continue
			}
			if activeCancel != nil {
				activeCancel()
				activeCancel = nil
			}
			if activeSuperseded {
				r.superseded.Add(1)
				completion.response = schedulerResponse{err: ErrRequestSuperseded}
			}
			respond(active, completion.response)
			active = nil
			activeSuperseded = false
			startNext()
		}
	}
}

func schedulerRequestIsNewer(candidate, current TranscriptionRequest) bool {
	if candidate.AudioEndAt != current.AudioEndAt {
		return candidate.AudioEndAt > current.AudioEndAt
	}
	return candidate.Authoritative || !current.Authoritative
}
