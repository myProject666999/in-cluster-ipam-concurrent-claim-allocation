# Concurrent claim allocation

This note documents how the in-cluster IPAM provider keeps address allocation
correct when many claims are reconciled at the same time — across goroutines,
across controller replicas, and across the overlap window of a leader-election
handover. It records the approach that was chosen, the retry bound, the candidate
selection strategy, and every observable behavior change.

## The race

Two allocators that read the same "addresses in use" snapshot can both pick the
same free address. Nothing about the controller loop prevents this:

- `MaxConcurrentReconciles` only bounds one process; it says nothing about a
  second replica.
- Leader election does **not** remove the overlap. During a handover the old
  leader may still be finishing queued reconciles while the new leader has
  started. The old and new processes can hold the same snapshot.
- Even within a single replica, a cached informer can serve a snapshot that is
  older than a just-completed create.

Correctness therefore cannot rest on single-threaded or single-replica
execution, or on an in-process mutex. It has to rest on the atomicity of the
storage layer.

## Approaches considered

1. **A pool-level process lock.** Simple, but it only protects one process. It
   does nothing for a second replica or the handover overlap, so it cannot be the
   basis of the correctness argument. Rejected.
2. **Address-derived object names + create uniqueness.** Name each `IPAddress`
   after the address it holds. The API server guarantees that a name is unique
   within a namespace, so `Create` of a second object for the same address fails
   with `AlreadyExists`. The loser simply tries the next candidate. This is the
   chosen approach: it uses the storage layer's atomicity directly, works across
   any number of processes, and needs no locking.
3. **Optimistic concurrency on a shared counter/object (resourceVersion).**
   Workable, but it serializes everything through one contended object and needs
   an explicit conflict loop on every write. The per-address create conflict of
   approach 2 is a strictly narrower contention point, so it is preferred.

The rest of this document describes approach 2.

## Why the name mapping must be canonical

Approach 2 is only sound if "same address ⇒ same object name". If two spellings
of one address produced two names, both creates would succeed and the address
would be held twice, bypassing the protection entirely. IPv6 makes this acute:
`2001:db8::1`, `2001:0db8:0:0:0:0:0:1`, `2001:DB8::1` and `2001:db8::1%eth0`
all denote the same address.

`poolutil.AddressToObjectName` normalizes before forming the name:

- IPv4-mapped IPv6 (`::ffff:10.0.0.1`) is unmapped to plain IPv4, so an address
  cannot be claimed once as IPv4 and once as IPv4-mapped IPv6.
- Any zone (`%eth0`) is stripped.
- IPv4 uses dotted-quad (`10.0.0.1`).
- IPv6 uses **all eight hextets, lowercase, no leading zeros, joined by `-`**
  (`2001-db8-0-0-0-0-0-1`) rather than the compressed `::` form. The compressed
  form can begin or end with `-` (`::1` → `--1`), which is not a valid RFC 1123
  label; the expanded form always starts and ends with a hex digit.

The result is always a valid RFC 1123 subdomain of at most 39 characters. The
mapping is injective (different addresses never share a name) and reversible
(`ObjectNameToAddress`), and IPv4 names (dots, no hyphens) never collide with
IPv6 names (hyphens, no dots). Object names created by older provider versions —
which were named after the claim — do not parse as addresses; `ObjectNameToAddress`
rejects them, and the allocator treats a status reference to such a name as "no
preferred address".

## Allocation algorithm

For one claim (`internal/controllers/ipaddressclaim_allocator.go`):

1. **Reuse first.** List the `IPAddress` objects that belong to this claim and
   pool (matched by namespace, `spec.claimRef`, `spec.poolRef`). This is read
   through the API reader (uncached), so a stale informer cache cannot make the
   allocator believe the claim has no address and create a second one. If one or
   more exist, converge them (below) and return the winner. This makes the whole
   path idempotent: re-reconciling a claim ten times returns the same address.
2. **Otherwise allocate.** List the pool's in-use addresses, compute the free
   set, pick a candidate, and `Create` its object.
   - On `AlreadyExists`: if the existing object belongs to *this* claim, a
     concurrent replica created it — adopt it and converge. Otherwise another
     claim holds the address: add it to the local "conflicted" set and try the
     next candidate.
   - After a successful create, for **cluster-scoped pools**, verify exclusive
     ownership (see "Global pools" below) and, for any pool, converge any
     duplicate the claim may now have.
3. **Exhaustion vs contention.**
   - If the free set is empty, refresh the snapshot once and retry; only if it is
     *still* empty is the pool reported exhausted. A `Result{RequeueAfter}` is
     returned so the claim retries later without busy-waiting. Exhaustion and
     contention are never conflated.
   - If every candidate loses its create race within the retry bound, a
     retriable `ErrAllocationContention` is returned and the workqueue retries
     with backoff.

### Retry bound

`poolutil.MaxAllocationAttempts` (10) bounds the create attempts in one
allocation round, so a hot pool cannot make one reconcile hammer the API server.
The in-use snapshot is re-listed every `allocationRelistInterval` (3) attempts;
between refreshes, addresses lost to conflicts are tracked locally, so the bound
costs at most a handful of extra reads. Hitting the bound yields
`ErrAllocationContention` (retry the reconcile later), never a false "exhausted".

### Requeueing exhausted claims

A claim that finds the pool exhausted is requeued after
`poolExhaustedRequeueAfter` (30s). That is only a backstop: the claim controller
also watches pools and enqueues all claims of a pool that transitioned from 0
free to >0 free (`poolNoLongerEmpty` / `prefixPoolNoLongerEmpty`). So a claim
that was exhausted succeeds on the next round once capacity is released, without
polling.

## Candidate selection

`poolutil.CandidateAddress` and `poolutil.FindFreePrefixAt` implement the
strategy, balancing collision avoidance with the operators' expectation that
allocation is predictable and compact:

- **Attempt 0 is always the lowest free address** — exactly what the previous
  sequential implementation chose. In the uncontended case nothing changes:
  addresses are handed out in order and a freed address is the next one reused,
  so the pool stays contiguous under churn instead of fragmenting toward the
  top of the range.
- **Later attempts scatter.** Once the first choice loses a create race, the
  next attempts start at an offset derived from a hash of the claim key
  (namespace/name) and the attempt number. Concurrent claims spread across the
  free space instead of all retrying the same lowest address.
- **Deterministic, hence convergent.** The only inputs are the free set, the
  claim key and the attempt number, so two replicas reading the same snapshot
  compute the same candidate sequence. That is what lets a claim that two
  replicas both processed end up on one address (see "Convergence").
- **Preferred reuse.** If the claim status already names an address (from a
  previous allocation whose object was lost), attempt 0 tries that address
  first, keeping allocations stable across object recreation.

Does scattering fragment the pool? No more than the sequential case in steady
state: attempt 0 is still the lowest free address, so only claims that actually
collide under contention probe elsewhere. Churn-driven reuse is covered by
`TestFreedAddressesAreReused` and `TestHighChurnReusesAddresses`.

Prefix pools use the same idea with `FindFreePrefixAt(index)`: attempt 0 is the
historical first-fit prefix, later attempts probe at a claim-derived index into
the deterministic left-to-right list of allocatable prefixes.

## Global (cluster-scoped) pools

`IPAddress` is namespaced, so create uniqueness serializes allocators only
*within* a namespace. Two claims in different namespaces, both referencing one
`GlobalInClusterIPPool`, can each create an object holding the same address.
After a successful create against a cluster-scoped pool, the allocator lists the
pool's addresses and resolves any duplicate deterministically: the earlier
creation wins, ties are broken by namespace/name. The loser is released by
whichever replica notices it first (releases are idempotent), and the losing
claim retries with a different candidate. Because exactly one object can be the
winner, deleting losers cannot cascade.

Ordering compares creation timestamps at **one-second granularity**. `metav1.Time`
serializes as RFC 3339 without fractional seconds, so an object this process just
created (nanoseconds) and the same object read back (seconds) would otherwise
sort differently. Comparing whole seconds — and only the wall clock, never the
monotonic reading — makes every replica agree on the winner.

## Convergence

Two replicas can transiently create two objects for the *same* claim (one starts
from a snapshot without the other's object, or both lose different races and move
on independently). Whenever an allocator has more than one object for the claim,
it keeps the deterministic winner and releases the rest
(`convergeAndReturn`). The convergence loop re-lists until a pass finds the
winner still present and no duplicate remains; it remembers the objects it
released so a replica that picked a concurrently-deleted object as winner
re-evaluates instead of returning a dead object. If, exceptionally, every object
the claim had is released by racing replicas before the loop can settle, the
allocator returns a retriable `ErrAllocationContention` rather than recursing, so
the retry depth stays bounded.

## Ownership trace and crash recovery

Every `IPAddress` is created with its full ownership trace already set: the
`ClaimRef` in the spec, owner references to the claim and the pool, the cluster
label, and the `ProtectAddress` finalizer. This is what makes allocation
recoverable:

- **Status write failed after the address was created.** The next reconcile scans
  the claim's namespace, finds the address by its `ClaimRef`/owner references,
  reuses it, and rewrites the status. (`TestStatusWriteFailureRecovery`.)
- **Claim deleted mid-allocation.** The address still references the claim, so
  the delete path (`findAddressForClaim`, which also falls back to the
  `ClaimRef` scan) finds and releases it instead of leaking an unreachable
  address. Reclaiming addresses whose claim disappeared without a finalizer pass
  is out of scope, but the trace that makes such reclamation possible is
  present. (`TestClaimDeletedDuringAllocation`.)

## What did not change

- The controller still runs with `MaxConcurrentReconciles: 1` by default. That is
  now a throughput/thrash knob, not a correctness requirement: the correctness
  argument rests on the storage layer, so raising it is safe.
- `poolutil.FindFreeAddress`, `FindFreePrefix` and the counting functions keep
  their behavior; the new `FindFreePrefixAt` is layered on the same primitives.

## Observable behavior changes

1. **`IPAddress` object names are the allocated address** (canonicalized as
   described above), not the claim name. Anything that looked an address up by
   the claim's name must follow `claim.status.addressRef.name` instead. The
   claim's status, and the controller's own delete path, do exactly that.
2. A claim whose pool overlaps another pool's addresses can no longer hold an
   address the other pool already handed out: the shared object name makes the
   collision a create conflict. Overlapping pools were always a
   misconfiguration; it now surfaces as one claim waiting rather than two claims
   sharing an address.
3. Duplicate `IPAddress` objects for one claim (only reachable under concurrent
   replicas) are converged to one, chosen deterministically.
