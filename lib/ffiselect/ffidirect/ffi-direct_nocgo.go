//go:build !cgo
// +build !cgo

// Under CGO_ENABLED=0 this package is intentionally empty. ffi-direct.go
// (the //go:build cgo file) provides the FFI{} reflection wrapper used
// by Curio's GPU selector when sealing-pipeline code paths are compiled
// in. PDP-only consumers (Curio Core) never load that reflection target,
// so a no-CGo build is fine without any stub types here.
package ffidirect
