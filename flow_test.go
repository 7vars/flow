package flow

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFlowHandlerStruct(t *testing.T) {
	inline, incmd, _ := testingInline()
	outline, _, outevt := testingOutline()
	p := pipe{inline, outline}
	hdl := Flow{}

	hdl.OnPull(p)
	assert.Equal(t, PULL, <-incmd)

	hdl.OnCancel(p)
	assert.Equal(t, CANCEL, <-incmd)

	hdl.OnPush(p, 42)
	evt := <-outevt
	assert.Equal(t, PUSH, evt.Type())
	assert.Equal(t, 42, evt.Data)

	hdl.OnError(p, io.EOF)
	evt = <-outevt
	assert.Equal(t, ERROR, evt.Type())
	assert.Error(t, evt.Error)

	hdl.OnComplete(p)
	evt = <-outevt
	assert.Equal(t, COMPLETE, evt.Type())
	assert.True(t, evt.Complete)

	var pulled, canceled, completed, pushed, errored bool
	hdl.DoPull = func(i IOlet) {
		assert.NotNil(t, i)
		pulled = true
	}
	hdl.DoCancel = func(i IOlet) {
		assert.NotNil(t, i)
		canceled = true
	}
	hdl.DoComplete = func(o Outlet) {
		assert.NotNil(t, o)
		completed = true
	}
	hdl.DoPush = func(i IOlet, a any) {
		assert.NotNil(t, i)
		assert.Equal(t, 42, a)
		pushed = true
	}
	hdl.DoError = func(i IOlet, err error) {
		assert.NotNil(t, i)
		assert.Error(t, err)
		errored = true
	}

	hdl.OnPull(p)
	assert.True(t, pulled)
	hdl.OnCancel(p)
	assert.True(t, canceled)
	hdl.OnComplete(p)
	assert.True(t, completed)
	hdl.OnPush(p, 42)
	assert.True(t, pushed)
	hdl.OnError(p, io.EOF)
	assert.True(t, errored)
}

func testingFlowHandler() FlowHandler {
	return FlowHandlerFunc[int, string](func(i int) (string, error) {
		if i == 73 {
			return "", errors.New("sheldon's prime")
		}
		if i < 42 {
			return "", Next
		}
		if i > 42 {
			return "", io.EOF
		}
		return "meaning of live", nil
	})
}

func TestFlowHandlerFunc(t *testing.T) {
	inline, incmd, _ := testingInline()
	outline, _, outevt := testingOutline()
	p := pipe{inline, outline}
	hdl := testingFlowHandler()

	hdl.OnPull(p)
	assert.Equal(t, PULL, <-incmd)

	hdl.OnCancel(p)
	assert.Equal(t, CANCEL, <-incmd)

	hdl.OnError(p, io.EOF)
	evt := <-outevt
	assert.Equal(t, ERROR, evt.Type())
	assert.Error(t, evt.Error)

	hdl.OnComplete(p)
	evt = <-outevt
	assert.Equal(t, COMPLETE, evt.Type())
	assert.True(t, evt.Complete)

	hdl.OnPush(p, 37)
	assert.Equal(t, PULL, <-incmd)

	hdl.OnPush(p, 43)
	assert.Equal(t, CANCEL, <-incmd)

	hdl.OnPush(p, 73)
	evt = <-outevt
	assert.Equal(t, ERROR, evt.Type())
	assert.Error(t, evt.Error)

	hdl.OnPush(p, "73")
	evt = <-outevt
	assert.Equal(t, ERROR, evt.Type())
	assert.Error(t, evt.Error)

	hdl.OnPush(p, 42)
	evt = <-outevt
	assert.Equal(t, PUSH, evt.Type())
	assert.Equal(t, "meaning of live", evt.Data)
}

func TestFlowProcessPanics(t *testing.T) {
	proc := FlowProcess(func(ctx context.Context, i []Inline, o []Outline) {})
	assert.Panics(t, func() {
		proc.Process(context.Background(), nil, nil)
	})
	assert.Panics(t, func() {
		proc.Process(context.Background(), make([]Inline, 1), nil)
	})
	assert.Panics(t, func() {
		proc.Process(context.Background(), make([]Inline, 2), make([]Outline, 3))
	})
}

func TestFlowProcessor(t *testing.T) {
	proc := FlowProcessor(FlowBuilderFunc(func() FlowHandler {
		return testingFlowHandler()
	}))
	inline, incmd, inevt := testingInline()
	outline, outcmd, outevt := testingOutline()

	ctx, cancel := context.WithCancel(context.Background())
	proc.Process(ctx, []Inline{inline}, []Outline{outline})

	outcmd <- PULL
	assert.Equal(t, PULL, <-incmd)
	inevt <- Event{Data: 42}
	evt := <-outevt
	assert.Equal(t, PUSH, evt.Type())
	assert.Equal(t, "meaning of live", evt.Data)

	inevt <- Event{Data: 41}
	assert.Equal(t, PULL, <-incmd)

	outcmd <- CANCEL
	assert.Equal(t, CANCEL, <-incmd)

	inevt <- Event{Error: io.EOF}
	evt = <-outevt
	assert.Equal(t, ERROR, evt.Type())
	assert.Error(t, evt.Error)

	inevt <- Event{Complete: true}
	evt = <-outevt
	assert.Equal(t, COMPLETE, evt.Type())
	assert.True(t, evt.Complete)

	cancel()
	time.Sleep(100 * time.Millisecond)
	outcmd <- PULL
	evt = <-outevt
	assert.Equal(t, ERROR, evt.Type())
	assert.Error(t, evt.Error)

	close(inevt)
	close(outcmd)
}
