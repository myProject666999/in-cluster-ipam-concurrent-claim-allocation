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

	"go4.org/netipx"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/api/v1alpha2"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/index"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/poolutil"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/pkg/ipamutil"
	pooltypes "sigs.k8s.io/cluster-api-ipam-provider-in-cluster/pkg/types"
)

// PrefixIPAddressClaimHandler reconciles claims against prefix pool resources.
type PrefixIPAddressClaimHandler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader performs uncached reads of the claim's own IPAddress objects.
	// It is optional; when nil the (cached) Client is used.
	APIReader client.Reader
	claim     *ipamv1.IPAddressClaim
	pool      pooltypes.GenericInClusterPrefixPool
}

var _ ipamutil.ClaimHandler = &PrefixIPAddressClaimHandler{}

func (i *InClusterProviderAdapter) prefixPoolToIPClaims(kind string) func(context.Context, client.Object) []reconcile.Request {
	return func(ctx context.Context, a client.Object) []reconcile.Request {
		pool, ok := a.(pooltypes.GenericInClusterPrefixPool)
		if !ok {
			return nil
		}
		requests := []reconcile.Request{}
		claims := &ipamv1.IPAddressClaimList{}
		err := i.Client.List(ctx, claims,
			client.MatchingFields{
				index.IPAddressClaimPoolRefCombinedField: index.IPPoolRefValue(ipamv1.IPPoolReference{
					Name:     pool.GetName(),
					Kind:     kind,
					APIGroup: v1alpha2.GroupVersion.Group,
				}),
			},
			client.InNamespace(pool.GetNamespace()),
		)
		if err != nil {
			return requests
		}
		for _, claim := range claims.Items {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      claim.Name,
					Namespace: claim.Namespace,
				},
			})
		}
		return requests
	}
}

// FetchPool fetches the (Global)InClusterPrefixPool.
func (h *PrefixIPAddressClaimHandler) FetchPool(ctx context.Context) (client.Object, *ctrl.Result, error) {
	switch h.claim.Spec.PoolRef.Kind {
	case inClusterPrefixPoolKind:
		pool := &v1alpha2.InClusterPrefixPool{}
		if err := h.Get(ctx, types.NamespacedName{Namespace: h.claim.Namespace, Name: h.claim.Spec.PoolRef.Name}, pool); err != nil {
			return nil, nil, fmt.Errorf("failed to fetch pool: %w", err)
		}
		h.pool = pool
	case globalInClusterPrefixPoolKind:
		pool := &v1alpha2.GlobalInClusterPrefixPool{}
		if err := h.Get(ctx, types.NamespacedName{Name: h.claim.Spec.PoolRef.Name}, pool); err != nil {
			return nil, nil, fmt.Errorf("failed to fetch pool: %w", err)
		}
		h.pool = pool
	}

	if h.pool == nil {
		err := errors.New("pool not found")
		ctrl.LoggerFrom(ctx).Error(err, "the referenced pool could not be found")
		return nil, nil, nil
	}

	return h.pool, nil, nil
}

// EnsureAddress ensures that exactly one IPAddress with a valid prefix
// allocation exists for the claim and returns it. See claimAddressAllocator for
// the concurrency model: the object is named after the allocated prefix's
// first address, so create uniqueness serializes competing allocators.
func (h *PrefixIPAddressClaimHandler) EnsureAddress(ctx context.Context) (*ipamv1.IPAddress, *ctrl.Result, error) {
	strategy := &prefixPoolStrategy{
		claimKey:  fmt.Sprintf("%s/%s", h.claim.Namespace, h.claim.Name),
		reference: h.claim.Spec.PoolRef,
		config:    poolutil.NewPrefixPoolConfig(h.pool.PoolSpec()),
		gateway:   h.pool.PoolSpec().Gateway,
	}

	allocator := &claimAddressAllocator{
		client:   h.Client,
		reader:   h.APIReader,
		scheme:   h.Scheme,
		claim:    h.claim,
		pool:     h.pool,
		strategy: strategy,
	}
	return allocator.ensure(ctx)
}

// ReleaseAddress releases the allocated prefix.
func (h *PrefixIPAddressClaimHandler) ReleaseAddress(_ context.Context) (*ctrl.Result, error) {
	// We don't need to do anything here, since the prefix is released when the IPAddress is deleted.
	return nil, nil
}

// prefixPoolStrategy allocates sub-prefixes from an
// (Global)InClusterPrefixPool.
type prefixPoolStrategy struct {
	claimKey  string
	reference ipamv1.IPPoolReference
	config    *poolutil.PrefixPoolConfig
	gateway   string
}

var _ allocationStrategy = &prefixPoolStrategy{}

func (s *prefixPoolStrategy) poolRef() ipamv1.IPPoolReference { return s.reference }

func (s *prefixPoolStrategy) preferredCandidate(name string) *allocationCandidate {
	addr, err := poolutil.ObjectNameToAddress(name)
	if err != nil || !poolutil.PrefixIsAllocatable(netip.PrefixFrom(addr, s.config.AllocationPrefixLength), s.config) {
		return nil
	}
	return &allocationCandidate{address: addr, prefixLength: int32(s.config.AllocationPrefixLength)} //nolint:gosec // validated to [1,128]
}

func (s *prefixPoolStrategy) chooseCandidate(inUse []ipamv1.IPAddress, conflicted []netip.Addr, attempt int, preferred *allocationCandidate) (allocationCandidate, error) {
	inUseIPSet, err := poolutil.AddressesToAllocationPrefixSet(inUse)
	if err != nil {
		return allocationCandidate{}, fmt.Errorf("failed to convert IPAddressList to allocation prefix set: %w", err)
	}
	if len(conflicted) > 0 {
		builder := &netipx.IPSetBuilder{}
		builder.AddSet(inUseIPSet)
		for _, addr := range conflicted {
			builder.AddPrefix(netip.PrefixFrom(addr, s.config.AllocationPrefixLength))
		}
		if inUseIPSet, err = builder.IPSet(); err != nil {
			return allocationCandidate{}, fmt.Errorf("failed to update allocation prefix set: %w", err)
		}
	}

	// Attempt 0 keeps the historical first-fit choice (the lowest free prefix),
	// so uncontended allocation order and address reuse are unchanged. Later
	// attempts probe at a claim-derived index and then advance by one per
	// attempt, so competing claims spread over the free space instead of all
	// retrying the same first free prefix. The order is deterministic for a
	// given snapshot, so two replicas of the same claim still converge.
	index := 0
	if attempt > 0 {
		index = attempt
		if window, err := poolutil.PrefixCandidateCount(s.config); err == nil && window > 0 {
			index = (poolutil.ScatterIndex(s.claimKey, attempt, window) + attempt) % window
		}
	}

	if preferred != nil && attempt == 0 {
		// Re-acquire the prefix recorded in the claim status if it is still
		// free, keeping allocations stable when the IPAddress object was lost.
		candidate := netip.PrefixFrom(preferred.address, s.config.AllocationPrefixLength)
		if !inUseIPSet.OverlapsPrefix(candidate) {
			return allocationCandidate{address: candidate.Addr(), prefixLength: int32(candidate.Bits())}, nil //nolint:gosec // Bits() is [0,128]
		}
	}

	freePrefix, err := poolutil.FindFreePrefixAt(s.config, inUseIPSet, index)
	if err != nil {
		if errors.Is(err, poolutil.ErrNoPrefixAvailable) {
			return allocationCandidate{}, poolutil.ErrNoFreeAddress
		}
		return allocationCandidate{}, err
	}
	return allocationCandidate{address: freePrefix.Addr(), prefixLength: int32(freePrefix.Bits())}, nil //nolint:gosec // Bits() is [0,128]
}

func (s *prefixPoolStrategy) specFor(c allocationCandidate) ipamv1.IPAddressSpec {
	return ipamv1.IPAddressSpec{
		PoolRef: s.reference,
		Address: c.address.String(),
		Gateway: s.gateway,
		Prefix:  ptr.To(c.prefixLength),
	}
}

func prefixPoolStatus(o client.Object) *v1alpha2.InClusterPrefixPoolStatusAddresses {
	switch ipPool := o.(type) {
	case *v1alpha2.InClusterPrefixPool:
		return ipPool.Status.Addresses
	case *v1alpha2.GlobalInClusterPrefixPool:
		return ipPool.Status.Addresses
	default:
		return nil
	}
}

// prefixPoolNoLongerEmpty only returns true if the pool status previously had
// 0 free prefixes and now has free prefixes.
func prefixPoolNoLongerEmpty() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldStatus := prefixPoolStatus(e.ObjectOld)
			newStatus := prefixPoolStatus(e.ObjectNew)
			if oldStatus != nil && newStatus != nil {
				if oldStatus.Free == 0 && newStatus.Free > 0 {
					return true
				}
			}
			return false
		},
	}
}
