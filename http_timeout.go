package asr

import "context"

// Direct provider callers also get a finite timeout. A Client-supplied deadline
// takes precedence, including explicitly configured budgets longer than 8s.
func httpRequestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, defaultRequestTimeout)
}
