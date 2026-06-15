package pdpv0

import (
	"database/sql"
	"math/big"
	"testing"
)

// nn builds a valid sql.NullInt64.
func nn(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }

// null is an invalid (NULL) sql.NullInt64.
var null = sql.NullInt64{}

func TestProofClearsLocalFailure(t *testing.T) {
	cases := []struct {
		name            string
		lastProven      *big.Int
		proveAt         sql.NullInt64
		nextAttempt     sql.NullInt64
		prevChallenge   sql.NullInt64
		failures        int
		want            bool
	}{
		{
			name:       "nil lastProven -> never clears",
			lastProven: nil,
			proveAt:    nn(100),
			want:       false,
		},
		{
			name:       "anchor1 prove_at_epoch: proof at window clears",
			lastProven: big.NewInt(150),
			proveAt:    nn(150),
			failures:   2,
			want:       true,
		},
		{
			name:       "anchor1 prove_at_epoch: proof before window does not clear",
			lastProven: big.NewInt(149),
			proveAt:    nn(150),
			failures:   2,
			want:       false,
		},
		{
			name:        "anchor2 next_prove_attempt_at: proof after last failure clears",
			lastProven:  big.NewInt(5000),
			proveAt:     null,
			nextAttempt: nn(4000 + int64(CalculateBackoffBlocks(2))), // lastFailure=4000
			failures:    2,
			want:        true,
		},
		{
			name:        "anchor2 next_prove_attempt_at: proof at/before last failure does not clear",
			lastProven:  big.NewInt(4000),
			proveAt:     null,
			nextAttempt: nn(4000 + int64(CalculateBackoffBlocks(2))), // lastFailure=4000, strict >
			failures:    2,
			want:        false,
		},
		{
			// curio-core#65 drift state: prove_at_epoch AND next_prove_attempt_at
			// both NULL, failures>0, only prev_challenge_request_epoch available.
			name:          "anchor3 drift: proof at/after prev challenge clears",
			lastProven:    big.NewInt(3795509),
			proveAt:       null,
			nextAttempt:   null,
			prevChallenge: nn(3795509),
			failures:      4,
			want:          true,
		},
		{
			name:          "anchor3 drift: proof after prev challenge clears",
			lastProven:    big.NewInt(3795600),
			proveAt:       null,
			nextAttempt:   null,
			prevChallenge: nn(3795509),
			failures:      4,
			want:          true,
		},
		{
			name:          "anchor3 drift: proof before prev challenge does not clear",
			lastProven:    big.NewInt(3795508),
			proveAt:       null,
			nextAttempt:   null,
			prevChallenge: nn(3795509),
			failures:      4,
			want:          false,
		},
		{
			name:          "anchor3 requires failures>0",
			lastProven:    big.NewInt(3795600),
			proveAt:       null,
			nextAttempt:   null,
			prevChallenge: nn(3795509),
			failures:      0,
			want:          false,
		},
		{
			name:       "no anchor available -> false",
			lastProven: big.NewInt(3795600),
			proveAt:    null,
			failures:   3,
			want:       false,
		},
		{
			// prove_at_epoch takes priority even when prevChallenge would also match.
			name:          "anchor priority: prove_at_epoch wins over prev challenge",
			lastProven:    big.NewInt(149),
			proveAt:       nn(150),
			prevChallenge: nn(100),
			failures:      4,
			want:          false, // anchored on prove_at_epoch=150, 149<150
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := proofClearsLocalFailure(tc.lastProven, tc.proveAt, tc.nextAttempt, tc.prevChallenge, tc.failures)
			if got != tc.want {
				t.Errorf("proofClearsLocalFailure() = %v, want %v", got, tc.want)
			}
		})
	}
}
