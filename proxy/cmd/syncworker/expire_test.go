package main

import (
	"testing"

	"statemig/internal/redisx"
)

func TestExpireAtAddsRemainingTimeToSourceClock(t *testing.T) {
	now := redisx.Value{Arr: []redisx.Value{{Str: []byte("1700000000")}, {Str: []byte("250999")}}}
	got, err := expireAt(now, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(1700000000250 + 1500); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	if _, err := expireAt(redisx.Value{}, 1500); err == nil {
		t.Fatal("no TIME reply should be an error")
	}
}
