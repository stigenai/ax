// Package cigateproof holds a scratch test for ax-kky.3: proving the
// build-vet-test check on stigenai/deploy goes red on a failing test.
// A later commit on this branch deletes the package before the proof
// PR closes. The branch never merges.
package cigateproof

import "testing"

func TestGateProofDeliberateFailure(t *testing.T) {
	t.Fatal("ax-kky.3 gate proof: this test must fail on purpose")
}
