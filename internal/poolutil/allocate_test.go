/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package poolutil

import (
	"fmt"
	"net/netip"
	"testing"

	"go4.org/netipx"
)

func mustIPSet(t *testing.T, addresses ...string) *netipx.IPSet {
	t.Helper()
	set, err := AddressesToIPSet(addresses)
	if err != nil {
		t.Fatalf("failed to build IPSet from %v: %v", addresses, err)
	}
	return set
}

func mustFreeSet(t *testing.T, poolAddrs, inUse []string) *netipx.IPSet {
	t.Helper()
	free, err := FreeAddressSet(mustIPSet(t, poolAddrs...), mustIPSet(t, inUse...))
	if err != nil {
		t.Fatalf("failed to compute free set: %v", err)
	}
	return free
}

func TestCandidateAddressFirstAttemptIsLowest(t *testing.T) {
	free := mustFreeSet(t, []string{"10.0.0.1-10.0.0.10"}, []string{"10.0.0.1", "10.0.0.2"})

	got, err := CandidateAddress(free, "ns/claim-a", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := netip.MustParseAddr("10.0.0.3"); got != want {
		t.Errorf("attempt 0 = %s, want lowest free %s", got, want)
	}
}

func TestCandidateAddressPreferred(t *testing.T) {
	free := mustFreeSet(t, []string{"10.0.0.1-10.0.0.10"}, nil)

	preferred := netip.MustParseAddr("10.0.0.7")
	got, err := CandidateAddress(free, "ns/claim-a", 0, &preferred)
	if err != nil {
		t.Fatal(err)
	}
	if got != preferred {
		t.Errorf("attempt 0 with preferred = %s, want %s", got, preferred)
	}

	// Attempt 1 falls back to the lowest free address.
	got, err = CandidateAddress(free, "ns/claim-a", 1, &preferred)
	if err != nil {
		t.Fatal(err)
	}
	if want := netip.MustParseAddr("10.0.0.1"); got != want {
		t.Errorf("attempt 1 with preferred = %s, want lowest free %s", got, want)
	}

	// A preferred address that is no longer free is ignored.
	free = mustFreeSet(t, []string{"10.0.0.1-10.0.0.10"}, []string{"10.0.0.7"})
	got, err = CandidateAddress(free, "ns/claim-a", 0, &preferred)
	if err != nil {
		t.Fatal(err)
	}
	if want := netip.MustParseAddr("10.0.0.1"); got != want {
		t.Errorf("attempt 0 with taken preferred = %s, want lowest free %s", got, want)
	}
}

func TestCandidateAddressScatteringIsDeterministicAndDistinct(t *testing.T) {
	free := mustFreeSet(t, []string{"10.0.0.1-10.0.0.254"}, nil)

	first, err := CandidateAddress(free, "ns/claim-a", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		again, err := CandidateAddress(free, "ns/claim-a", 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if again != first {
			t.Fatalf("scattered candidate is not deterministic: %s != %s", again, first)
		}
	}
	if !free.Contains(first) {
		t.Errorf("scattered candidate %s is not in the free set", first)
	}

	// Different claims must scatter across the free set rather than all pick
	// the same address.
	distinct := map[netip.Addr]bool{}
	for i := range 100 {
		c, err := CandidateAddress(free, fmt.Sprintf("ns/claim-%d", i), 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !free.Contains(c) {
			t.Fatalf("candidate %s outside free set", c)
		}
		distinct[c] = true
	}
	if len(distinct) < 50 {
		t.Errorf("expected claims to scatter widely across the free set, got %d distinct addresses", len(distinct))
	}
}

func TestCandidateAddressEmptyFreeSet(t *testing.T) {
	free := mustFreeSet(t, []string{"10.0.0.1"}, []string{"10.0.0.1"})
	if _, err := CandidateAddress(free, "ns/claim-a", 0, nil); err == nil {
		t.Error("expected an error for an empty free set")
	}
}

func TestCandidateAddressIPv6(t *testing.T) {
	free := mustFreeSet(t, []string{"2001:db8::/120"}, []string{"2001:db8::1"})

	got, err := CandidateAddress(free, "ns/claim-a", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := netip.MustParseAddr("2001:db8::"); got != want {
		t.Errorf("attempt 0 = %s, want %s", got, want)
	}

	// A scattered attempt must stay inside the free set and be a real address of
	// the pool family.
	got, err = CandidateAddress(free, "ns/claim-a", 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !free.Contains(got) || !got.Is6() {
		t.Errorf("scattered IPv6 candidate %s is invalid", got)
	}
}

func TestScatterIndexRange(t *testing.T) {
	for attempt := range 20 {
		idx := ScatterIndex("ns/claim", attempt, 10)
		if idx < 0 || idx >= 10 {
			t.Fatalf("ScatterIndex out of range: %d", idx)
		}
	}
	if got := ScatterIndex("ns/claim", 0, 0); got != 0 {
		t.Errorf("ScatterIndex with empty window = %d, want 0", got)
	}
}
