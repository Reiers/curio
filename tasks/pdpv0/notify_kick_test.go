package pdpv0

import (
	"testing"
)

// TestNotifyKickCoalesces verifies Kick is non-blocking and coalescing:
// the buffered depth-1 channel absorbs the first kick, and subsequent
// kicks while one is pending are dropped rather than blocking the caller.
func TestNotifyKickCoalesces(t *testing.T) {
	tk := &PDPNotifyTask{kick: make(chan struct{}, 1)}

	// First kick lands in the buffer.
	tk.Kick()
	// Several more kicks while one is pending must not block.
	for i := 0; i < 100; i++ {
		tk.Kick()
	}

	// Exactly one pending signal should be readable.
	select {
	case <-tk.kick:
	default:
		t.Fatal("expected one pending kick signal, got none")
	}

	// No second signal should be buffered (coalesced).
	select {
	case <-tk.kick:
		t.Fatal("expected kicks to coalesce to a single pending signal")
	default:
	}
}

// TestNotifyKickNilChannelSafe verifies Kick on a zero-value task (nil
// kick channel, e.g. constructed without NewPDPNotifyTask) is a no-op
// rather than a panic.
func TestNotifyKickNilChannelSafe(t *testing.T) {
	tk := &PDPNotifyTask{}
	// Must not panic.
	tk.Kick()
}
