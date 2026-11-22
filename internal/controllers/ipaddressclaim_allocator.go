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

package controllers

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/poolutil"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/pkg/ipamutil"
)

const (
	// allocationRelistInterval controls how often the in-use snapshot is
	// refreshed during an allocation loop. Between refreshes, addresses lost to
	// create conflicts are tracked locally, avoiding one List per attempt.
	allocationRelistInterval = 3

	// poolExhaustedRequeueAfter is how long an exhausted claim waits before it
	// is reconciled again. This is only a backstop: claims are also enqueued as
	// soon as the pool reports free addresses again (see poolNoLongerEmpty).
	// The delay avoids busy-waiting while capacity is unavailable.
	poolExhaustedRequeueAfter = 30 * time.Second
)

// allocationCandidate is one possible allocation: an address plus the length of
// the allocated prefix. For IP pools the length is the pool's prefix.
type allocationCandidate struct {
	address      netip.Addr
	prefixLength int32
}

// allocationStrategy captures the pool-specific parts of the allocation
// algorithm; the concurrency handling is shared by all pool types.
type allocationStrategy interface {
	// poolRef is recorded in IPAddress.spec.poolRef.
	poolRef() ipamv1.IPPoolReference
	// preferredCandidate interprets the object name recorded in the claim
	// status. It returns nil when the name does not encode an address (e.g.
	// objects created by older controller versions, named after the claim).
	preferredCandidate(name string) *allocationCandidate
	// chooseCandidate returns the candidate to try for the given attempt.
	// inUse is the pool-wide snapshot plus addresses lost to conflicts in this
	// round. It returns poolutil.ErrNoFreeAddress when nothing is free.
	chooseCandidate(inUse []ipamv1.IPAddress, conflicted []netip.Addr, attempt int, preferred *allocationCandidate) (allocationCandidate, error)
	// specFor builds the IPAddress spec for a candidate.
	specFor(c allocationCandidate) ipamv1.IPAddressSpec
}

// claimAddressAllocator allocates an address for one IPAddressClaim.
//
// Correctness under concurrency comes from the storage layer, not from
// process-level locking: IPAddress objects are named after the allocated
// address (see poolutil.AddressToObjectName), so the API server's create
// uniqueness serializes competing allocators. Two allocators that pick the same
// address race on Create; the loser receives AlreadyExists and moves on to the
// next candidate. This holds for any number of reconciler goroutines and
// controller replicas, including an old and a new leader overlapping during a
// leader-election handover.
//
// The allocator is idempotent: if an IPAddress for the claim already exists it
// is reused, so repeated reconciliation of a claim returns the same address.
type claimAddressAllocator struct {
	client client.Client
	// reader performs uncached reads of the claim's own address objects, so a
	// stale informer cache cannot make the allocator believe the claim has no
	// address and hand out a second one. May be nil, in which case client is
	// used.
	reader   client.Reader
	scheme   *runtime.Scheme
	claim    *ipamv1.IPAddressClaim
	pool     client.Object
	strategy allocationStrategy
}

// readerOrDefault returns the uncached reader, falling back to the client.
func (a *claimAddressAllocator) readerOrDefault() client.Reader {
	if a.reader != nil {
		return a.reader
	}
	return a.client
}

// clusterScopedPool reports whether the pool is cluster-scoped. IPAddress
// objects are namespaced, so for cluster-scoped pools create uniqueness alone
// does not serialize claims in different namespaces, and the deterministic
// post-create tie-break is required.
func (a *claimAddressAllocator) clusterScopedPool() bool {
	return a.pool.GetNamespace() == ""
}

// ensure guarantees that exactly one IPAddress object exists for the claim and
// returns it.
//
// A non-nil result without an error means no address could be allocated yet
// because the pool is exhausted; the caller must requeue after the given delay.
// Allocation contention is reported as an error wrapping
// poolutil.ErrAllocationContention so the workqueue retries with backoff.
func (a *claimAddressAllocator) ensure(ctx context.Context) (*ipamv1.IPAddress, *ctrl.Result, error) {
	mine, err := a.claimAddresses(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list addresses for claim: %w", err)
	}

	if len(mine) > 0 {
		// Reuse the address the claim already owns. This covers normal
		// re-reconciles, recovery after a failed status write, and duplicates
		// created by two replicas racing for the same claim.
		winner, err := a.convergeClaimAddresses(ctx, mine)
		if err != nil {
			return nil, nil, err
		}

		if a.clusterScopedPool() {
			lost, err := a.verifyExclusiveOwnership(ctx, winner)
			if err != nil {
				return nil, nil, err
			}
			if lost {
				// Another claim in a different namespace holds this address and
				// won the tie-break. Our object was released; allocate another.
				if addr, parseErr := netip.ParseAddr(winner.Spec.Address); parseErr == nil {
					return a.allocate(ctx, []netip.Addr{addr})
				}
				return a.allocate(ctx, nil)
			}
		}

		if err := a.ensureMetadata(ctx, winner); err != nil {
			if apierrors.IsNotFound(err) {
				// The winner was released concurrently (a racing replica
				// converged on a different object); re-evaluate from scratch.
				return a.allocate(ctx, nil)
			}
			return nil, nil, fmt.Errorf("failed to ensure address metadata: %w", err)
		}
		return winner, nil, nil
	}

	return a.allocate(ctx, nil)
}

// allocate claims a free address by creating its IPAddress object, retrying
// with different candidates when a create conflicts.
func (a *claimAddressAllocator) allocate(ctx context.Context, conflicted []netip.Addr) (*ipamv1.IPAddress, *ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	claimKey := fmt.Sprintf("%s/%s", a.claim.Namespace, a.claim.Name)

	var preferred *allocationCandidate
	if name := a.claim.Status.AddressRef.Name; name != "" {
		preferred = a.strategy.preferredCandidate(name)
	}

	addressesInUse, err := a.listPoolAddresses(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list addresses: %w", err)
	}
	relisted := true // addressesInUse was listed just before the loop

	for attempt := range poolutil.MaxAllocationAttempts {
		if attempt > 0 && attempt%allocationRelistInterval == 0 {
			if addressesInUse, err = a.listPoolAddresses(ctx); err != nil {
				return nil, nil, fmt.Errorf("failed to list addresses: %w", err)
			}
			relisted = true
		}

		candidate, err := a.strategy.chooseCandidate(addressesInUse, conflicted, attempt, preferred)
		if errors.Is(err, poolutil.ErrNoFreeAddress) {
			if !relisted {
				// The snapshot may be stale: refresh once before declaring the
				// pool exhausted, so a transient conflict is not misreported as
				// exhaustion.
				if addressesInUse, err = a.listPoolAddresses(ctx); err != nil {
					return nil, nil, fmt.Errorf("failed to list addresses: %w", err)
				}
				relisted = true
				continue
			}
			log.Info("Pool is exhausted, will retry when capacity becomes available",
				"pool", a.pool.GetName(), "claim", claimKey)
			return nil, &ctrl.Result{RequeueAfter: poolExhaustedRequeueAfter}, nil
		}
		if err != nil {
			return nil, nil, err
		}
		relisted = false

		address, err := a.newAddressObject(candidate)
		if err != nil {
			return nil, nil, err
		}

		if err := a.client.Create(ctx, address); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return nil, nil, fmt.Errorf("failed to create IPAddress %s: %w", address.Name, err)
			}
			// Lost the race for this candidate. If the object belongs to this
			// very claim, a concurrent replica of this reconcile created it -
			// adopt it so the same claim always converges on one address.
			existing, ok := a.getAddressIfOwnedByClaim(ctx, address.Name)
			if !ok {
				// Some other claim holds this address.
				conflicted = append(conflicted, candidate.address)
				continue
			}
			return a.convergeAndReturn(ctx, existing)
		}

		if a.clusterScopedPool() {
			// The create succeeded, but create uniqueness is per namespace, so a
			// claim in another namespace may have created an object for the same
			// address. Resolve the duplicate deterministically.
			lost, err := a.verifyExclusiveOwnership(ctx, address)
			if err != nil {
				return nil, nil, err
			}
			if lost {
				conflicted = append(conflicted, candidate.address)
				continue
			}
		}

		// A concurrent replica of the same claim may have created a second
		// object for it - either because it started from a snapshot that did not
		// yet contain this object, or because it lost the race for the first
		// candidate and moved on. Converge on the deterministic winner so the
		// claim ends up with exactly one address regardless of interleaving.
		return a.convergeAndReturn(ctx, address)
	}

	return nil, nil, fmt.Errorf("%w (claim %s, pool %s)", poolutil.ErrAllocationContention, claimKey, a.pool.GetName())
}

// convergeAndReturn reduces the address objects that currently reference this
// claim - including created, which this allocator just wrote - to one object and
// returns it. The deterministic winner is kept; the rest are released. This is
// the point where a claim that two replicas allocated concurrently (to two
// different addresses) converges back to a single address.
func (a *claimAddressAllocator) convergeAndReturn(ctx context.Context, created *ipamv1.IPAddress) (*ipamv1.IPAddress, *ctrl.Result, error) {
	// Re-list a few times: while converging, a concurrent replica of this same
	// claim can both create and release objects, so the winner we pick may
	// vanish (released by that replica) or a new duplicate may appear. Each pass
	// only releases losers, so the set shrinks monotonically. released records
	// the objects this allocator deleted, so a concurrent replica that picked
	// one of them as winner re-evaluates instead of returning a dead object.
	released := map[string]bool{}
	for range maxConvergencePasses {
		known, err := a.claimAddresses(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to list addresses for claim: %w", err)
		}

		// Drop objects this allocator already deleted. If that empties the set,
		// a racing replica of this same claim released everything concurrently;
		// retry the whole allocation rather than failing.
		live := known[:0:0]
		for _, address := range known {
			if !released[address.Namespace+"/"+address.Name] {
				live = append(live, address)
			}
		}
		if len(live) == 0 {
			// Every object this claim had is gone. If this allocator released
			// them (a racing replica of the same claim released the objects this
			// pass would have kept), re-evaluate once; otherwise the objects were
			// release concurrently before we ever saw them, so retry the whole
			// reconcile. Returning an error rather than recursing keeps the
			// retry bounded.
			if len(released) > 0 {
				released = map[string]bool{}
				continue
			}
			return nil, nil, fmt.Errorf("%w (claim %s/%s: all address objects vanished)", poolutil.ErrAllocationContention, a.claim.Namespace, a.claim.Name)
		}

		sortAddresses(live)
		winner := live[0]
		if winner.Name != created.Name {
			// Another replica's object won; release ours so the claim converges
			// on a single address.
			if err := a.release(ctx, created); err != nil && !apierrors.IsNotFound(err) {
				return nil, nil, fmt.Errorf("failed to release superseded address %s/%s: %w", created.Namespace, created.Name, err)
			}
			released[created.Namespace+"/"+created.Name] = true
		}
		for i := 1; i < len(live); i++ {
			loser := live[i]
			if err := a.release(ctx, &loser); err != nil && !apierrors.IsNotFound(err) {
				return nil, nil, fmt.Errorf("failed to release duplicate address %s/%s: %w", loser.Namespace, loser.Name, err)
			}
			released[loser.Namespace+"/"+loser.Name] = true
		}

		err = a.ensureMetadata(ctx, &winner)
		if apierrors.IsNotFound(err) {
			// The winner was deleted concurrently; re-evaluate.
			released[winner.Namespace+"/"+winner.Name] = true
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to ensure address metadata: %w", err)
		}
		return &winner, nil, nil
	}
	return nil, nil, fmt.Errorf("%w (claim %s/%s failed to converge)", poolutil.ErrAllocationContention, a.claim.Namespace, a.claim.Name)
}

// claimAddresses returns the IPAddress objects that belong to this claim and
// pool, sorted deterministically (by creation time, then namespace/name) so
// every replica converges on the same object when duplicates exist. The search
// uses uncached reads: a stale cache must never make the allocator believe the
// claim has no address and hand out a second one.
func (a *claimAddressAllocator) claimAddresses(ctx context.Context) ([]ipamv1.IPAddress, error) {
	reader := a.readerOrDefault()

	// Fast path: the name recorded in the claim status. In steady state this is
	// the only read needed to reuse an allocation.
	if name := a.claim.Status.AddressRef.Name; name != "" {
		address := &ipamv1.IPAddress{}
		err := reader.Get(ctx, types.NamespacedName{Namespace: a.claim.Namespace, Name: name}, address)
		if err == nil && address.DeletionTimestamp.IsZero() && a.belongsToClaim(address) {
			return []ipamv1.IPAddress{*address}, nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}
	}

	// Slow path: scan the claim's namespace. The IPAddress carries its ownership
	// trace (spec.claimRef and owner references), so the allocation can be
	// recovered even when the claim status write failed.
	addresses := &ipamv1.IPAddressList{}
	if err := reader.List(ctx, addresses, client.InNamespace(a.claim.Namespace)); err != nil {
		return nil, err
	}
	var mine []ipamv1.IPAddress
	for _, address := range addresses.Items {
		if address.DeletionTimestamp.IsZero() && a.belongsToClaim(&address) {
			mine = append(mine, address)
		}
	}
	sortAddresses(mine)
	return mine, nil
}

// belongsToClaim reports whether the address was allocated for this claim from
// this pool. The namespace check is essential: an IPAddress's claimRef carries
// only a name, and two claims with the same name may exist in different
// namespaces when they reference a cluster-scoped pool.
func (a *claimAddressAllocator) belongsToClaim(address *ipamv1.IPAddress) bool {
	poolRef := a.strategy.poolRef()
	return address.Namespace == a.claim.Namespace &&
		address.Spec.ClaimRef.Name == a.claim.Name &&
		address.Spec.PoolRef.Name == poolRef.Name &&
		address.Spec.PoolRef.Kind == poolRef.Kind &&
		address.Spec.PoolRef.APIGroup == poolRef.APIGroup
}

// getAddressIfOwnedByClaim fetches the address with the given name and returns
// it only if it belongs to this claim and pool and is not being deleted. Used
// after a create conflict to distinguish "another replica of this claim won the
// race" (adopt the object) from "another claim holds this address" (try the
// next candidate).
func (a *claimAddressAllocator) getAddressIfOwnedByClaim(ctx context.Context, name string) (*ipamv1.IPAddress, bool) {
	existing := &ipamv1.IPAddress{}
	if err := a.readerOrDefault().Get(ctx, types.NamespacedName{Namespace: a.claim.Namespace, Name: name}, existing); err != nil {
		return nil, false
	}
	if !existing.DeletionTimestamp.IsZero() || !a.belongsToClaim(existing) {
		return nil, false
	}
	return existing, true
}

// listPoolAddresses returns all IPAddress objects that reference this pool. The
// list is served by the cached client, which resolves the poolRef field index.
// Cache staleness can only cause extra create conflicts (resolved by the API
// server), never two holders of one address.
func (a *claimAddressAllocator) listPoolAddresses(ctx context.Context) ([]ipamv1.IPAddress, error) {
	return poolutil.ListAddressesInUse(ctx, a.client, a.pool.GetNamespace(), a.strategy.poolRef())
}

// convergeClaimAddresses reduces a set of address objects that all reference
// this claim to a single object. When more than one exists (only possible if
// two replicas allocated concurrently from different snapshots), the
// deterministic winner is kept and the others are released.
func (a *claimAddressAllocator) convergeClaimAddresses(ctx context.Context, addresses []ipamv1.IPAddress) (*ipamv1.IPAddress, error) {
	if len(addresses) == 0 {
		return nil, errors.New("no address to converge")
	}
	sortAddresses(addresses)
	winner := addresses[0]
	for i := 1; i < len(addresses); i++ {
		duplicate := addresses[i]
		if err := a.release(ctx, &duplicate); err != nil {
			return nil, fmt.Errorf("failed to release duplicate address %s/%s: %w", duplicate.Namespace, duplicate.Name, err)
		}
	}
	return &winner, nil
}

// verifyExclusiveOwnership checks whether another claim's IPAddress object holds
// the same address as ours. This is only reachable for cluster-scoped pools,
// where create uniqueness (per namespace) does not serialize claims in
// different namespaces.
//
// The comparison is deterministic and total - earlier creation wins, ties are
// broken by namespace/name - so all replicas agree on which object loses. The
// losing object is always released, whoever observes the duplicate first: this
// must not be deferred to "the other claim's allocator", because that allocator
// may already have finished before this object was created. Because exactly one
// object can be the winner, deleting losers is race-free and cannot cascade.
//
// Returns true if ours lost and was released; the caller then tries a different
// address. Returns false once no losing duplicate remains, with ours kept.
func (a *claimAddressAllocator) verifyExclusiveOwnership(ctx context.Context, ours *ipamv1.IPAddress) (bool, error) {
	all, err := a.listPoolAddresses(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to list addresses: %w", err)
	}
	for i := range all {
		other := &all[i]
		if other.Spec.Address != ours.Spec.Address ||
			(other.Namespace == ours.Namespace && other.Name == ours.Name) ||
			!other.DeletionTimestamp.IsZero() ||
			a.belongsToClaim(other) {
			continue
		}
		if addressObjectWins(other, ours) {
			// We lose: release our own object and report the loss.
			if err := a.release(ctx, ours); err != nil {
				return false, fmt.Errorf("failed to release duplicate address %s/%s: %w", ours.Namespace, ours.Name, err)
			}
			return true, nil
		}
		// We win: release the losing duplicate. Release is idempotent, so a
		// concurrent replica that also releases it is harmless.
		if err := a.release(ctx, other); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("failed to release duplicate address %s/%s: %w", other.Namespace, other.Name, err)
		}
	}
	return false, nil
}

// addressObjectWins reports whether a should survive over b when both hold the
// same address: the earlier creation wins, ties are broken by namespace/name.
func addressObjectWins(a, b *ipamv1.IPAddress) bool {
	return compareCreation(a, b) < 0
}

// compareCreation orders two objects by creation time and then by
// namespace/name. Two details matter for convergence:
//
//   - Only the (API-server assigned) wall clock is used. metav1.Time.Compare
//     can take the in-process monotonic clock reading into account, which
//     differs between an object this process just created and a copy read back
//     from storage; replicas must reach the identical order.
//   - Creation timestamps are compared at one-second granularity, matching how
//     metav1.Time is serialized (RFC 3339, no fractional seconds). Otherwise an
//     in-memory object created in this process (nanoseconds) would sort before
//     the same object read back from storage (truncated to seconds), and two
//     replicas would pick different duplicate winners.
func compareCreation(a, b *ipamv1.IPAddress) int {
	at := a.CreationTimestamp.Time.UTC().Truncate(time.Second)
	bt := b.CreationTimestamp.Time.UTC().Truncate(time.Second)
	if !at.Equal(bt) {
		if at.Before(bt) {
			return -1
		}
		return 1
	}
	return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
}

// newAddressObject builds the complete IPAddress object for a candidate.
// Everything is set before the object is created: if the claim is deleted or
// the controller crashes right after the create, the object still carries its
// ownership trace (claimRef in the spec, owner references to claim and pool)
// and the protection finalizer, so it can be identified and released later.
func (a *claimAddressAllocator) newAddressObject(candidate allocationCandidate) (*ipamv1.IPAddress, error) {
	address := ipamutil.NewIPAddress(a.claim, a.pool)
	address.Name = poolutil.AddressToObjectName(candidate.address)
	address.Spec = a.strategy.specFor(candidate)
	address.Spec.ClaimRef = ipamv1.IPAddressClaimReference{Name: a.claim.Name}

	if err := ipamutil.EnsureIPAddressMetadata(a.scheme, &address, a.claim, a.pool); err != nil {
		return nil, fmt.Errorf("failed to ensure metadata on new address: %w", err)
	}
	return &address, nil
}

// ensureMetadata makes sure an existing address carries the claim and pool
// owner references, the cluster label and the ProtectAddress finalizer,
// patching the object if anything is missing.
func (a *claimAddressAllocator) ensureMetadata(ctx context.Context, address *ipamv1.IPAddress) error {
	base := address.DeepCopy()
	if err := ipamutil.EnsureIPAddressMetadata(a.scheme, address, a.claim, a.pool); err != nil {
		return err
	}
	if equality.Semantic.DeepEqual(base, address) {
		return nil
	}
	return a.client.Patch(ctx, address, client.MergeFrom(base))
}

// maxConvergencePasses bounds the retries performed while converging duplicate
// or superseded address objects for one claim.
const maxConvergencePasses = 5

// release removes the protection finalizer and deletes the address object.
func (a *claimAddressAllocator) release(ctx context.Context, address *ipamv1.IPAddress) error {
	base := address.DeepCopy()
	if controllerutil.RemoveFinalizer(address, ipamutil.ProtectAddressFinalizer) {
		if err := a.client.Patch(ctx, address, client.MergeFrom(base)); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
	}
	if err := a.client.Delete(ctx, address); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// sortAddresses orders address objects deterministically: earlier creation
// first, ties broken by namespace/name.
func sortAddresses(addresses []ipamv1.IPAddress) {
	slices.SortFunc(addresses, func(a, b ipamv1.IPAddress) int { return compareCreation(&a, &b) })
}
