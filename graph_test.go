package flow

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventType(t *testing.T) {
	evt := Event{Data: 1}
	assert.Equal(t, PUSH, evt.Type())
	evt = Event{Error: Next}
	assert.Equal(t, ERROR, evt.Type())
	evt = Event{Complete: true}
	assert.Equal(t, COMPLETE, evt.Type())
}

func TestOutletChan(t *testing.T) {
	events := make(chan Event, 1)
	outlet := OutletChan(events)

	outlet.Push(10)
	evt := <-events
	assert.Equal(t, 10, evt.Data)

	outlet.Error(io.EOF)
	evt = <-events
	assert.Error(t, evt.Error)

	outlet.Complete()
	evt = <-events
	assert.True(t, evt.Complete)
}

func TestInletChanAndInline(t *testing.T) {
	commands := make(chan Command, 1)
	inlet := InletChan(commands)

	inlet.Pull()
	assert.Equal(t, PULL, <-commands)

	inlet.Cancel()
	assert.Equal(t, CANCEL, <-commands)

	var called bool
	il := inline{inlet, func() { called = true }, make(chan Event)}
	il.Close()
	assert.True(t, called)
	assert.NotNil(t, il.Events())
}

func TestStraightGraph(t *testing.T) {
	graph := newStraightGraph(ZeroSource(), Empty())

	assert.Panics(t, func() {
		graph.To(Empty())
	})
	assert.Panics(t, func() {
		graph.Via(Map(func(v any) any { return v }))
	})

	assert.NoError(t, graph.Await())

	result, err := graph.Execute()
	assert.NoError(t, err)
	assert.Equal(t, struct{}{}, result)

	assert.Nil(t, <-graph.Run())
}
