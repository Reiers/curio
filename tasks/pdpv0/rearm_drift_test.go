package pdpv0

import (
	"math/big"
	"testing"
)

func TestDriftedDataSetIsProving(t *testing.T) {
	cases := []struct {
		name       string
		live       bool
		lastProven *big.Int
		want       bool
	}{
		{"live + proven => re-arm candidate", true, big.NewInt(3800000), true},
		{"live + lastProven==0 => first-init, skip", true, big.NewInt(0), false},
		{"live + lastProven negative => skip", true, big.NewInt(-1), false},
		{"live + nil lastProven => skip", true, nil, false},
		{"not live => skip regardless", false, big.NewInt(3800000), false},
		{"not live + nil => skip", false, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := driftedDataSetIsProving(tc.live, tc.lastProven); got != tc.want {
				t.Errorf("driftedDataSetIsProving(%v, %v) = %v, want %v", tc.live, tc.lastProven, got, tc.want)
			}
		})
	}
}

func TestNextProveEpochIsArmable(t *testing.T) {
	cases := []struct {
		name string
		next *big.Int
		want bool
	}{
		{"positive => armable", big.NewInt(3800500), true},
		{"zero => not armable", big.NewInt(0), false},
		{"negative => not armable", big.NewInt(-5), false},
		{"nil => not armable", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextProveEpochIsArmable(tc.next); got != tc.want {
				t.Errorf("nextProveEpochIsArmable(%v) = %v, want %v", tc.next, got, tc.want)
			}
		})
	}
}
