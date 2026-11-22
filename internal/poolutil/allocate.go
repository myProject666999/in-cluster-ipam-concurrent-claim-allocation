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
	"errors"
	"hash/fnv"
	"math/big"
	"net/netip"
	"strconv"

	"go4.org/netipx"
)

const (
	// MaxAllocationAttempts bounds how many IPAddress create attempts a single
	// allocation round performs. Each attempt costs at most one API write, and
	// create conflicts are expected under concurrency, so the bound keeps a
	// reconcile from hammering the API server under extreme contention. When
	// the bound is reached, the caller returns a retriable error and the
	// workqueue retries the whole reconcile with backoff.
	MaxAllocationAttempts = 10
)

var (
	// ErrNoFreeAddress reports that the (possibly stale) in-use snapshot leaves
	// no allocatable address in the pool. It is a statement about the snapshot,
	// not about the future: callers refresh the snapshot once before treating it
	// as pool exhaustion.
	ErrNoFreeAddress = errors.New("no free address available")

	// ErrAllocationContention reports that allocation failed because every
	// candidate lost its create race to a competing allocator. This is a
	// transient, retriable condition and is deliberately distinct from pool
	// exhaustion: the pool still has free addresses, this allocator just kept
	// losing races.
	ErrAllocationContention = errors.New("address allocation repeatedly lost the create race, retry later")
)

// FreeAddressSet returns the addresses in poolSet that are not in inUseSet.
// The computation is set arithmetic, so it is cheap even for very large (e.g.
// IPv6) pools.
func FreeAddressSet(poolSet, inUseSet *netipx.IPSet) (*netipx.IPSet, error) {
	builder := &netipx.IPSetBuilder{}
	builder.AddSet(poolSet)
	builder.RemoveSet(inUseSet)
	return builder.IPSet()
}

// CandidateAddress deterministically picks the candidate address for the given
// allocation attempt in freeSet.
//
// The strategy balances three goals:
//
//   - Predictable, compact allocation: attempt 0 is the lowest free address -
//     exactly the choice the previous sequential implementation made. In the
//     uncontended case allocation order is unchanged, and freed addresses are
//     reused from the bottom of the pool, so addresses stay contiguous instead
//     of fragmenting.
//   - Collision avoidance under contention: once the first choice loses a
//     create race, later attempts start from an offset derived from a hash of
//     the claim key and the attempt number. Concurrent claims scatter across
//     the free space instead of all piling onto the same lowest free address.
//   - Stability for one claim: the claim key (namespace/name) and the attempt
//     number are the only inputs besides the free set, so two replicas
//     processing the same claim from the same snapshot pick the same candidate
//     and converge on a single IPAddress object (the loser adopts the winner's
//     object, see the claim handler).
//
// If preferred is non-nil and still free, it is returned for attempt 0. A
// claim uses this to re-acquire the address recorded in its status when the
// IPAddress object was lost, keeping allocations stable across object
// recreation.
func CandidateAddress(freeSet *netipx.IPSet, claimKey string, attempt int, preferred *netip.Addr) (netip.Addr, error) {
	if freeSet == nil || len(freeSet.Ranges()) == 0 {
		return netip.Addr{}, ErrNoFreeAddress
	}

	offset := attempt
	if preferred != nil && freeSet.Contains(*preferred) {
		if attempt == 0 {
			return *preferred, nil
		}
		// The preferred address consumed attempt 0.
		offset = attempt - 1
	}

	if offset == 0 {
		return freeSet.Ranges()[0].From(), nil
	}
	return scatteredCandidate(freeSet, claimKey, offset)
}

// ScatterIndex returns a deterministic index in [0, window) derived from the
// claim key and attempt number. It is used by strategies that cannot express
// their free space as an IPSet (prefix pools) to probe a claim-specific
// position instead of always starting at the first free slot, which would make
// all competing allocators collide on the same candidate.
func ScatterIndex(claimKey string, attempt, window int) int {
	if window <= 1 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(claimKey))
	_, _ = h.Write([]byte("/"))
	_, _ = h.Write([]byte(strconv.Itoa(attempt)))
	return int(h.Sum64() % uint64(window)) //nolint:gosec // modular reduction keeps the value in range
}

// scatteredCandidate picks a deterministic "random" free address by hashing the
// claim key and attempt number into an offset in the free address set.
func scatteredCandidate(freeSet *netipx.IPSet, claimKey string, attempt int) (netip.Addr, error) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(claimKey))
	_, _ = h.Write([]byte("/"))
	_, _ = h.Write([]byte(strconv.Itoa(attempt)))

	count := big.NewInt(0)
	for _, r := range freeSet.Ranges() {
		count.Add(count, rangeSize(r))
	}
	if count.Sign() <= 0 {
		return netip.Addr{}, ErrNoFreeAddress
	}

	offset := big.NewInt(0).SetUint64(h.Sum64())
	offset.Mod(offset, count)

	for _, r := range freeSet.Ranges() {
		size := rangeSize(r)
		if offset.Cmp(size) < 0 {
			return addressAtOffset(r.From(), offset), nil
		}
		offset.Sub(offset, size)
	}
	// Unreachable: offset < count and the ranges sum to count.
	return netip.Addr{}, ErrNoFreeAddress
}

// rangeSize returns the number of addresses in r as a big.Int, because a range
// can exceed int64 (e.g. an IPv6 /64).
func rangeSize(r netipx.IPRange) *big.Int {
	from := addressToBig(r.From())
	size := addressToBig(r.To())
	return size.Sub(size, from).Add(size, big.NewInt(1))
}

// addressAtOffset returns the address offset positions after from in the same
// address family.
func addressAtOffset(from netip.Addr, offset *big.Int) netip.Addr {
	sum := addressToBig(from)
	sum.Add(sum, offset)
	b := sum.Bytes()
	if from.Is4() {
		var a4 [4]byte
		copy(a4[4-len(b):], b)
		return netip.AddrFrom4(a4)
	}
	var a16 [16]byte
	copy(a16[16-len(b):], b)
	return netip.AddrFrom16(a16)
}

// addressToBig converts an address to a big.Int in its own address family.
func addressToBig(addr netip.Addr) *big.Int {
	if addr.Is4() {
		b := addr.As4()
		return big.NewInt(0).SetBytes(b[:])
	}
	b := addr.As16()
	return big.NewInt(0).SetBytes(b[:])
}
