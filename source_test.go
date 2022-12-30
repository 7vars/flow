package flow

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testOutline struct {
	commands <-chan Command
	events   chan<- Event
}

func (to testOutline) Commands() <-chan Command { return to.commands }

func (to testOutline) Push(v any) { to.events <- Event{Data: v} }

func (to testOutline) Error(e error) { to.events <- Event{Error: e} }

func (to testOutline) Complete() { to.events <- Event{Complete: true} }

func testingOutline() (Outline, chan<- Command, <-chan Event) {
	commands := make(chan Command, 1)
	events := make(chan Event, 1)
	return testOutline{commands, events}, commands, events
}

func TestSourceStruct(t *testing.T) {
	source := Source{}
	outline, _, events := testingOutline()
	go source.OnPull(outline)
	evt := <-events
	assert.Equal(t, COMPLETE, evt.Type())

	go source.OnCancel(outline)
	evt = <-events
	assert.Equal(t, COMPLETE, evt.Type())

	source.DoPull = func(o Outlet) {
		o.Push(42)
	}
	go source.OnPull(outline)
	evt = <-events
	assert.Equal(t, PUSH, evt.Type())
	assert.Equal(t, 42, evt.Data)

	source.DoCancel = func(o Outlet) {
		o.Error(io.EOF)
	}
	go source.OnCancel(outline)
	evt = <-events
	assert.Equal(t, ERROR, evt.Type())
	assert.Error(t, evt.Error)
}

func TestSourceProcessorCancelViaContext(t *testing.T) {
	proc := SourceProcessor(SourceBuilderFunc(func() SourceHandler {
		return SourceHandlerFunc[int](func() (int, error) {
			return 42, nil
		})
	}))

	outline, commands, events := testingOutline()
	ctx, cancel := context.WithCancel(context.Background())
	proc.Process(ctx, nil, []Outline{outline})
	commands <- PULL
	evt := <-events
	assert.Equal(t, PUSH, evt.Type())
	assert.Equal(t, 42, evt.Data)
	cancel()
	time.Sleep(250 * time.Millisecond)
	commands <- PULL
	evt = <-events
	assert.Equal(t, ERROR, evt.Type())
	assert.Error(t, evt.Error)
}

func TestSourceProcessorComplete(t *testing.T) {
	proc := SourceProcessor(SourceBuilderFunc(func() SourceHandler {
		value := 42
		return SourceHandlerFunc[int](func() (int, error) {
			defer func() { value++ }()
			if value != 42 {
				return 0, io.EOF
			}
			return value, nil
		})
	}))

	outline, commands, events := testingOutline()
	proc.Process(context.Background(), nil, []Outline{outline})
	commands <- PULL
	evt := <-events
	assert.Equal(t, PUSH, evt.Type())
	assert.Equal(t, 42, evt.Data)
	commands <- PULL
	evt = <-events
	assert.Equal(t, COMPLETE, evt.Type())
}

func TestSourceProcessorCancel(t *testing.T) {
	proc := SourceProcessor(SourceBuilderFunc(func() SourceHandler {
		value := 42
		return SourceHandlerFunc[int](func() (int, error) {
			defer func() { value++ }()
			return value, nil
		})
	}))

	outline, commands, events := testingOutline()
	proc.Process(context.Background(), nil, []Outline{outline})
	commands <- PULL
	evt := <-events
	assert.Equal(t, PUSH, evt.Type())
	assert.Equal(t, 42, evt.Data)
	commands <- CANCEL
	evt = <-events
	assert.Equal(t, COMPLETE, evt.Type())
}

func TestSourceProcessorError(t *testing.T) {
	proc := SourceProcessor(SourceBuilderFunc(func() SourceHandler {
		value := 42
		return SourceHandlerFunc[int](func() (int, error) {
			defer func() { value++ }()
			if value != 42 {
				return 0, errors.New("error for test")
			}
			return value, nil
		})
	}))

	outline, commands, events := testingOutline()
	proc.Process(context.Background(), nil, []Outline{outline})
	commands <- PULL
	evt := <-events
	assert.Equal(t, PUSH, evt.Type())
	assert.Equal(t, 42, evt.Data)
	commands <- PULL
	evt = <-events
	assert.Equal(t, ERROR, evt.Type())
	assert.Error(t, evt.Error)
}

func TestSliceSource(t *testing.T) {
	source := SliceSource([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	outline, commands, events := testingOutline()
	source.Process(context.Background(), nil, []Outline{outline})

	for i := 0; i < 10; i++ {
		commands <- PULL
		evt := <-events
		assert.Equal(t, PUSH, evt.Type())
		assert.Equal(t, i, evt.Data)
	}

	commands <- PULL
	evt := <-events
	assert.Equal(t, COMPLETE, evt.Type())
	assert.True(t, evt.Complete)
}
