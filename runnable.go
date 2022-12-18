package flow

import (
	"context"
	"sync"
)

type Runnable interface {
	Await() error
	AwaitWithContext(context.Context) error
	Run() <-chan any
	RunWithContext(context.Context) <-chan any
	Execute() (any, error)
	ExecuteWithContext(context.Context) (any, error)
}

type StartFunc func() (<-chan any, context.CancelFunc)

func runWithContext(ctx context.Context, start StartFunc) <-chan any {
	out := make(chan any, 1)
	emits, cancel := start()

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				defer cancel()
				out <- ctx.Err()
				return
			case e, open := <-emits:
				if !open {
					return
				}
				out <- e
			}
		}
	}()

	return out
}

func execute(emits <-chan any) (any, error) {
	results := make(chan any, 1)
	go func() {
		defer close(results)
		result := make([]interface{}, 0)
		for e := range emits {
			if err, ok := e.(error); ok {
				results <- err
				return
			}
			result = append(result, e)
		}
		results <- result
	}()

	switch res := (<-results).(type) {
	case error:
		return nil, res
	case []interface{}:
		if len(res) == 0 {
			return struct{}{}, nil
		}
		if len(res) == 1 {
			return res[0], nil
		}
		return res, nil
	default:
		return res, nil
	}
}

// ===== emitter =====

type emitter struct {
	sync.Mutex
	Inline
	emits   chan any
	started bool
	closed  bool
}

func newEmitter(inline Inline) *emitter {
	return &emitter{
		Inline: inline,
		emits:  make(chan any, 1),
	}
}

func (e *emitter) start() (<-chan any, context.CancelFunc) {
	e.Lock()
	defer e.Unlock()
	if !e.started {
		e.started = true
		e.Pull()
	}
	return e.emits, e.kill
}

func (e *emitter) kill() {
	e.Lock()
	defer e.Unlock()
	if e.started && !e.closed {
		e.Cancel()
	}
}

func (e *emitter) Emit(v any) {
	e.Lock()
	defer e.Unlock()
	if !e.closed {
		e.emits <- v
	}
}

func (e *emitter) Close() {
	e.Lock()
	defer e.Unlock()
	if !e.closed {
		e.closed = true
		close(e.emits)
		e.Inline.Close()
	}
}

func (e *emitter) Await() error {
	return e.AwaitWithContext(context.Background())
}

func (e *emitter) AwaitWithContext(ctx context.Context) error {
	_, err := e.ExecuteWithContext(ctx)
	return err
}

func (e *emitter) Run() <-chan any {
	return e.RunWithContext(context.Background())
}

func (e *emitter) RunWithContext(ctx context.Context) <-chan any {
	return runWithContext(ctx, e.start)
}

func (e *emitter) Execute() (any, error) {
	return e.ExecuteWithContext(context.Background())
}

func (e *emitter) ExecuteWithContext(ctx context.Context) (any, error) {
	return execute(e.RunWithContext(ctx))
}
