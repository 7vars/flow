package flow

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testInline struct {
	commands chan<- Command
	events   <-chan Event
}

func (ti testInline) Events() <-chan Event {
	return ti.events
}

func (ti testInline) Pull() {
	ti.commands <- PULL
}

func (ti testInline) Cancel() {
	ti.commands <- CANCEL
}

func testingInline() (Inline, <-chan Command, chan<- Event) {
	commands := make(chan Command, 1)
	events := make(chan Event, 1)
	return testInline{commands, events}, commands, events
}

func TestSinkStruct(t *testing.T) {
	sink := Sink{}
	inline, commands, _ := testingInline()
	sink.OnPush(inline, 42)
	assert.Equal(t, PULL, <-commands)
	sink.OnError(inline, io.EOF)
	assert.Equal(t, CANCEL, <-commands)

	sink.DoPush = func(i Inlet, a any) {
		assert.Equal(t, 42, a)
	}
	sink.DoError = func(i Inlet, err error) {
		assert.Error(t, err)
	}
	var complete bool
	sink.DoComplete = func() {
		complete = true
	}

	sink.OnPush(inline, 42)
	sink.OnError(inline, io.EOF)
	sink.OnComplete()
	assert.True(t, complete)
}

func TestSinkProcessorCancelViaContext(t *testing.T) {
	var wg sync.WaitGroup
	t.Parallel()
	proc := SinkProcessor(SinkBuilderFunc(func() SinkHandler {
		return Sink{
			DoPush: func(i Inlet, a any) {
				assert.Equal(t, 42, a)
			},
		}
	}))

	inline, commands, events := testingInline()
	ctx, cancel := context.WithCancel(context.Background())
	proc.Process(ctx, []Inline{inline}, nil)
	events <- Event{Data: 42}

	cancel()
	time.Sleep(150 * time.Millisecond)
	events <- Event{Data: 42}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cmd := <-commands
		assert.Equal(t, CANCEL, cmd)
	}()

	wg.Wait()
}
