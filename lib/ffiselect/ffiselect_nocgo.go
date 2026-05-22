//go:build !cgo
// +build !cgo

// Non-CGo companion to ffiselect.go. Under CGO_ENABLED=0 the full
// reflection-driven GPU dispatch in ffiselect.go is excluded (it
// transitively depends on filecoin-ffi). PDP-only consumers (Curio Core)
// don't drive the GPU selector but lib/ffi keeps WithLogCtx in use, so
// this file preserves that single symbol.

package ffiselect

import "context"

type logCtxKt struct{}

var logCtxKey = logCtxKt{}

func WithLogCtx(ctx context.Context, kvs ...any) context.Context {
	return context.WithValue(ctx, logCtxKey, kvs)
}
