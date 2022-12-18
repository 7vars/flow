package flow

import "fmt"

// ===== handler =====

type SinkHandler interface {
	OnPush(EmittableInlet, any)
	OnError(EmittableInlet, error)
	OnComplete(Emittable)
}

type SinkHandlerFunc[T any] func(T) error

func (shf SinkHandlerFunc[T]) OnPush(inlet EmittableInlet, v any) {
	if t, ok := v.(T); ok {
		if err := shf(t); err != nil {
			shf.OnError(inlet, err)
			return
		}
		inlet.Pull()
		return
	}
	var t0 T
	shf.OnError(inlet, fmt.Errorf("sink: expect type %T got %T", t0, v))
}

func (SinkHandlerFunc[T]) OnError(inlet EmittableInlet, e error) {
	inlet.Emit(e)
	inlet.Close()
}

func (SinkHandlerFunc[T]) OnComplete(emittable Emittable) {
	emittable.Close()
}

// ===== builder =====

type SinkBuilder interface {
	Build(Inline) Runnable
}

type SinkBuilderFunc func(Inline) Runnable

func (sbf SinkBuilderFunc) Build(in Inline) Runnable {
	return sbf(in)
}

type BuildSink func() SinkHandler

func (bs BuildSink) Build(inline Inline) Runnable {
	emitter := newEmitter(inline)

	go func(inline EmittableInline, handler SinkHandler) {
		// defer fmt.Println("DEBUG: sink closed")
		for evt := range inline.Events() {
			switch evt.Type() {
			case PUSH:
				handler.OnPush(inline, evt.Data)
			case ERROR:
				handler.OnError(inline, evt.Error)
			case COMPLETE:
				handler.OnComplete(inline)
			}
		}
	}(emitter, bs())

	return emitter
}

// ===== sinks =====

func SinkFunc[T any](f func(T) error) SinkBuilder {
	return BuildSink(func() SinkHandler {
		return SinkHandlerFunc[T](f)
	})
}

func Empty() SinkBuilder {
	return SinkFunc(func(any) error { return nil })
}

func ForEach[T any](f func(T)) SinkBuilder {
	return SinkFunc(func(t T) error {
		f(t)
		return nil
	})
}

func Println() SinkBuilder {
	return ForEach(func(a any) {
		fmt.Println(a)
	})
}

func Printf(format string) SinkBuilder {
	return ForEach(func(a any) {
		fmt.Printf(format, a)
	})
}
