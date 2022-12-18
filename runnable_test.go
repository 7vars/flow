package flow

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunWithContext(t *testing.T) {
	var canceled bool
	emits := make(chan any, 1)
	ctx, cancel := context.WithCancel(context.Background())

	results := runWithContext(ctx, func() (<-chan any, context.CancelFunc) {
		return emits, func() { canceled = true }
	})

	expect := 42
	emits <- expect
	assert.Equal(t, expect, <-results)

	cancel()
	assert.Error(t, (<-results).(error))
	assert.True(t, canceled)
}

func TestExecute(t *testing.T) {
	channel := make(chan any)
	go func() {
		defer close(channel)
		channel <- 1
		channel <- 2
	}()
	result, err := execute(channel)
	assert.NoError(t, err)
	assert.Contains(t, result.([]any), 1)
	assert.Contains(t, result.([]any), 2)

	channel = make(chan any)
	go func() {
		defer close(channel)
		channel <- 42
	}()
	result, err = execute(channel)
	assert.NoError(t, err)
	assert.Equal(t, 42, result)

	channel = make(chan any)
	go func() {
		defer close(channel)
		channel <- io.EOF
	}()
	result, err = execute(channel)
	assert.Error(t, err)
	assert.Nil(t, result)
}
