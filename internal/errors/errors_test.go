package errors

import (
	"errors"
	"testing"
)

func TestOmniErrorBasics(t *testing.T) {
	t.Parallel()
	cause := errors.New("boom")
	e := Wrap(CodeHTTP, "Server error.", cause, Retryable(true), Details("502"))
	if e.Code != CodeHTTP || !e.Retryable || e.Details != "502" {
		t.Fatalf("unexpected: %+v", e)
	}
	if !IsCode(e, CodeHTTP) {
		t.Fatalf("expected IsCode true")
	}
	if got, ok := As(e); !ok || got.Code != CodeHTTP {
		t.Fatalf("As failed: %v %v", got, ok)
	}
}

