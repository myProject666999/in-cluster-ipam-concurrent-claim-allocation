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
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/api/v1alpha2"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/index"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/poolutil"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/internal/testutil/fakeclient"
	"sigs.k8s.io/cluster-api-ipam-provider-in-cluster/pkg/ipamutil"
)

const testNamespace = "test-ns"

// newConcurrencyScheme returns a scheme containing the API types the in-cluster
// provider works with.
func newConcurrencyScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha2.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := ipamv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := clusterv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// newConcurrencyClient returns a fake client with the poolRef index registered,
// mirroring the controller's cache setup.
func newConcurrencyClient(t *testing.T, scheme *runtime.Scheme) *fakeclient.Client {
	t.Helper()
	return fakeclient.New(scheme).
		WithIndex(&ipamv1.IPAddress{}, index.IPAddressPoolRefCombinedField, index.IPAddressByCombinedPoolRef)
}

func newNamespacedPool(name string, addresses ...string) *v1alpha2.InClusterIPPool {
	return &v1alpha2.InClusterIPPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:       v1alpha2.InClusterIPPoolSpec{Addresses: addresses, Prefix: 24},
	}
}

func newGlobalPool(name string, addresses ...string) *v1alpha2.GlobalInClusterIPPool {
	return &v1alpha2.GlobalInClusterIPPool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha2.InClusterIPPoolSpec{Addresses: addresses, Prefix: 24},
	}
}

func newConcurrencyClaim(name, namespace, poolKind, poolName string) *ipamv1.IPAddressClaim {
	return &ipamv1.IPAddressClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: ipamv1.IPAddressClaimSpec{
			PoolRef: ipamv1.IPPoolReference{
				APIGroup: v1alpha2.GroupVersion.Group,
				Kind:     poolKind,
				Name:     poolName,
			},
		},
	}
}

// ensureAddressRound runs one allocation round for the claim, retrying a
// bounded number of times on allocation contention exactly as the reconciling
// workqueue would (with backoff). It returns the allocated address, or a
// non-nil Result when the pool is exhausted.
func ensureAddressRound(ctx context.Context, c client.Client, scheme *runtime.Scheme, claim *ipamv1.IPAddressClaim) (*ipamv1.IPAddress, *ctrl.Result, error) {
	for range 100 {
		h := &IPAddressClaimHandler{Client: c, Scheme: scheme, claim: claim}
		if _, _, err := h.FetchPool(ctx); err != nil {
			return nil, nil, err
		}
		address, res, err := h.EnsureAddress(ctx)
		if errors.Is(err, poolutil.ErrAllocationContention) {
			continue
		}
		return address, res, err
	}
	return nil, nil, fmt.Errorf("claim %s/%s kept losing create conflicts", claim.Namespace, claim.Name)
}

// runClaimsConcurrently allocates for all claims from numWorkers goroutines and
// returns per-claim results.
func runClaimsConcurrently(ctx context.Context, c client.Client, scheme *runtime.Scheme, claims []*ipamv1.IPAddressClaim, numWorkers int) (map[string]*ipamv1.IPAddress, map[string]*ctrl.Result, error) {
	work := make(chan *ipamv1.IPAddressClaim)
	var wg sync.WaitGroup

	var mu sync.Mutex
	allocated := map[string]*ipamv1.IPAddress{}
	exhausted := map[string]*ctrl.Result{}
	var firstErr atomic.Value

	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for claim := range work {
				address, res, err := ensureAddressRound(ctx, c, scheme, claim)
				if err != nil {
					firstErr.Store(fmt.Errorf("claim %s/%s: %w", claim.Namespace, claim.Name, err))
					return
				}
				mu.Lock()
				if address != nil {
					allocated[claim.Namespace+"/"+claim.Name] = address
				} else {
					exhausted[claim.Namespace+"/"+claim.Name] = res
				}
				mu.Unlock()
			}
		}()
	}
	for _, claim := range claims {
		work <- claim
	}
	close(work)
	wg.Wait()

	if err := firstErr.Load(); err != nil {
		return nil, nil, err.(error)
	}
	return allocated, exhausted, nil
}

func makeConcurrencyClaims(namespace, poolKind, poolName string, n int) []*ipamv1.IPAddressClaim {
	claims := make([]*ipamv1.IPAddressClaim, 0, n)
	for i := range n {
		claims = append(claims, newConcurrencyClaim(fmt.Sprintf("claim-%d", i), namespace, poolKind, poolName))
	}
	return claims
}

func rangeAddresses(prefix string, n int) []string {
	addrs := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		addrs = append(addrs, fmt.Sprintf("%s.%d", prefix, i))
	}
	return addrs
}

func storedAddresses(c *fakeclient.Client) []client.Object {
	return c.Objects(&ipamv1.IPAddress{})
}

// TestConcurrentAllocationAllDistinct: 200 claims allocate from a 200-address
// pool via 100 goroutines. Every claim must get an address and all 200
// addresses must be distinct.
func TestConcurrentAllocationAllDistinct(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", 200)...)); err != nil {
		t.Fatal(err)
	}
	claims := makeConcurrencyClaims(testNamespace, inClusterIPPoolKind, "pool", 200)

	// Inject a scheduling point between each allocator's read of the in-use
	// snapshot and its create, maximizing the chance of interleavings.
	c.PreCreate = func(client.Object) { goruntime.Gosched() }

	allocated, exhausted, err := runClaimsConcurrently(ctx, c, scheme, claims, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(exhausted) != 0 {
		t.Fatalf("expected no exhausted claims, got %d", len(exhausted))
	}
	if len(allocated) != 200 {
		t.Fatalf("expected 200 allocated claims, got %d", len(allocated))
	}

	seen := map[string]string{}
	for key, address := range allocated {
		if prev, ok := seen[address.Spec.Address]; ok {
			t.Fatalf("address %s allocated to both %s and %s", address.Spec.Address, prev, key)
		}
		seen[address.Spec.Address] = key
	}
	if objs := storedAddresses(c); len(objs) != 200 {
		t.Fatalf("expected 200 IPAddress objects, got %d", len(objs))
	}
}

// TestConcurrentAllocationExhaustion: 200 claims allocate from a 100-address
// pool via 100 goroutines. Exactly 100 claims must succeed and exactly 100 must
// receive a definitive exhausted result (not a contention error).
func TestConcurrentAllocationExhaustion(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", 100)...)); err != nil {
		t.Fatal(err)
	}
	claims := makeConcurrencyClaims(testNamespace, inClusterIPPoolKind, "pool", 200)

	c.PreCreate = func(client.Object) { goruntime.Gosched() }

	allocated, exhausted, err := runClaimsConcurrently(ctx, c, scheme, claims, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(allocated) != 100 {
		t.Fatalf("expected 100 allocated claims, got %d", len(allocated))
	}
	if len(exhausted) != 100 {
		t.Fatalf("expected 100 exhausted claims, got %d", len(exhausted))
	}
	for key, res := range exhausted {
		if res == nil || res.RequeueAfter <= 0 {
			t.Fatalf("claim %s: exhausted result must requeue with a delay, got %+v", key, res)
		}
	}

	seen := map[string]bool{}
	for _, address := range allocated {
		if seen[address.Spec.Address] {
			t.Fatalf("address %s allocated twice", address.Spec.Address)
		}
		seen[address.Spec.Address] = true
	}
}

// TestSingleFreeAddressHundredClaims: with a single free address and 100
// competing claims, exactly one wins, the other 99 get a definitive exhausted
// result, and once capacity is freed an exhausted claim succeeds on a later
// round. This is the "one free address, 100 claims" scenario.
func TestSingleFreeAddressHundredClaims(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", "10.0.0.1")); err != nil {
		t.Fatal(err)
	}
	claims := makeConcurrencyClaims(testNamespace, inClusterIPPoolKind, "pool", 100)

	c.PreCreate = func(client.Object) { goruntime.Gosched() }

	allocated, exhausted, err := runClaimsConcurrently(ctx, c, scheme, claims, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(allocated) != 1 {
		t.Fatalf("expected exactly 1 allocated claim, got %d", len(allocated))
	}
	if len(exhausted) != 99 {
		t.Fatalf("expected 99 exhausted claims, got %d", len(exhausted))
	}

	// Free the address again; an exhausted claim must now succeed.
	for _, address := range allocated {
		if err := c.Delete(ctx, address); err != nil {
			t.Fatal(err)
		}
	}
	var retryClaim *ipamv1.IPAddressClaim
	for key := range exhausted {
		ns, name := splitClaimKey(key)
		retryClaim = newConcurrencyClaim(name, ns, inClusterIPPoolKind, "pool")
		break
	}
	address, res, err := ensureAddressRound(ctx, c, scheme, retryClaim)
	if err != nil {
		t.Fatal(err)
	}
	if address == nil || res != nil {
		t.Fatalf("expected exhausted claim to succeed after capacity was freed, got address=%v res=%+v", address, res)
	}
	if address.Spec.Address != "10.0.0.1" {
		t.Fatalf("expected the freed address to be reused, got %s", address.Spec.Address)
	}
}

// TestSameClaimProcessedRepeatedly: processing the same claim ten times must
// always return the same address and never allocate a second one - both when the
// claim status was persisted and when it was lost.
func TestSameClaimProcessedRepeatedly(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", 10)...)); err != nil {
		t.Fatal(err)
	}
	claim := newConcurrencyClaim("steady", testNamespace, inClusterIPPoolKind, "pool")
	if err := c.Create(ctx, claim); err != nil {
		t.Fatal(err)
	}

	first, res, err := ensureAddressRound(ctx, c, scheme, claim)
	if err != nil || first == nil {
		t.Fatalf("first allocation failed: address=%v res=%+v err=%v", first, res, err)
	}
	claim.Status.AddressRef = ipamv1.IPAddressReference{Name: first.Name}

	for i := range 9 {
		again, res, err := ensureAddressRound(ctx, c, scheme, claim)
		if err != nil || again == nil {
			t.Fatalf("round %d failed: address=%v res=%+v err=%v", i, again, res, err)
		}
		if again.Name != first.Name || again.Spec.Address != first.Spec.Address {
			t.Fatalf("round %d returned %s (%s), want %s (%s)", i, again.Name, again.Spec.Address, first.Name, first.Spec.Address)
		}
	}

	// Simulate a lost status update: the claim has no status, but the address
	// object exists. The next round must recover the same address.
	lostStatus := newConcurrencyClaim("steady", testNamespace, inClusterIPPoolKind, "pool")
	recovered, res, err := ensureAddressRound(ctx, c, scheme, lostStatus)
	if err != nil || recovered == nil {
		t.Fatalf("recovery failed: address=%v res=%+v err=%v", recovered, res, err)
	}
	if recovered.Name != first.Name {
		t.Fatalf("recovery returned %s, want %s", recovered.Name, first.Name)
	}

	if objs := storedAddresses(c); len(objs) != 1 {
		t.Fatalf("expected exactly 1 IPAddress object, got %d", len(objs))
	}
}

// TestStatusWriteFailureRecovery drives the full reconciler: the address is
// created but writing the claim status fails; the next reconcile must recover
// the address from the object itself and complete the status.
func TestStatusWriteFailureRecovery(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", 10)...)); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, newConcurrencyClaim("unlucky", testNamespace, inClusterIPPoolKind, "pool")); err != nil {
		t.Fatal(err)
	}

	reconciler := &ipamutil.ClaimReconciler{
		Client:  c,
		Scheme:  scheme,
		Adapter: &InClusterProviderAdapter{Client: c, Scheme: scheme},
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: "unlucky"}}

	// First reconcile adds the finalizer.
	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}

	// Fail the next claim patch (the status write) once.
	var failOnce atomic.Bool
	failOnce.Store(true)
	c.PatchError = func(obj client.Object) error {
		if _, ok := obj.(*ipamv1.IPAddressClaim); ok && failOnce.CompareAndSwap(true, false) {
			return errors.New("injected status write failure")
		}
		return nil
	}

	if _, err := reconciler.Reconcile(ctx, req); err == nil {
		t.Fatal("expected the reconcile to fail due to the injected status write failure")
	}
	c.PatchError = nil

	// The address exists, but the stored claim has no status.
	stored := &ipamv1.IPAddressClaim{}
	if err := c.Get(ctx, req.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.AddressRef.Name != "" {
		t.Fatalf("expected empty status after failed write, got %q", stored.Status.AddressRef.Name)
	}
	objs := storedAddresses(c)
	if len(objs) != 1 {
		t.Fatalf("expected 1 IPAddress object, got %d", len(objs))
	}
	addressName := objs[0].GetName()

	// The next reconcile recovers the existing address and completes the status.
	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, req.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.AddressRef.Name != addressName {
		t.Fatalf("expected status to reference %q, got %q", addressName, stored.Status.AddressRef.Name)
	}
	if objs := storedAddresses(c); len(objs) != 1 {
		t.Fatalf("expected still exactly 1 IPAddress object, got %d", len(objs))
	}
}

// TestClaimDeletedDuringAllocation verifies that an address created while its
// claim is being deleted carries enough ownership information to be
// identified, and that the delete path finds and removes it.
func TestClaimDeletedDuringAllocation(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", 10)...)); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, newConcurrencyClaim("doomed", testNamespace, inClusterIPPoolKind, "pool")); err != nil {
		t.Fatal(err)
	}

	// Allocate, then the claim is deleted before its status was written. The
	// handler must see the persisted claim so owner references carry its UID.
	storedClaim := &ipamv1.IPAddressClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "doomed"}, storedClaim); err != nil {
		t.Fatal(err)
	}
	address, res, err := ensureAddressRound(ctx, c, scheme, storedClaim)
	if err != nil || address == nil {
		t.Fatalf("allocation failed: address=%v res=%+v err=%v", address, res, err)
	}

	// The address must carry its ownership trace: claimRef in the spec and an
	// owner reference to the claim.
	if address.Spec.ClaimRef.Name != "doomed" {
		t.Fatalf("address does not reference its claim: %+v", address.Spec.ClaimRef)
	}
	foundOwner := false
	for _, ref := range address.OwnerReferences {
		if ref.Kind == "IPAddressClaim" && ref.Name == "doomed" && ref.UID == storedClaim.UID {
			foundOwner = true
		}
	}
	if !foundOwner {
		t.Fatalf("address has no owner reference to its claim: %+v", address.OwnerReferences)
	}

	// Now the claim is marked for deletion (its ReleaseAddress finalizer keeps it
	// around) and the reconciler runs the delete path.
	storedClaim.Finalizers = []string{ipamutil.ReleaseAddressFinalizer}
	now := metav1.Now()
	storedClaim.DeletionTimestamp = &now
	c.AddObject(storedClaim)

	reconciler := &ipamutil.ClaimReconciler{
		Client:  c,
		Scheme:  scheme,
		Adapter: &InClusterProviderAdapter{Client: c, Scheme: scheme},
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: "doomed"}}
	if _, err := reconciler.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}

	if objs := storedAddresses(c); len(objs) != 0 {
		t.Fatalf("expected the orphaned address to be deleted, got %d objects", len(objs))
	}
	after := &ipamv1.IPAddressClaim{}
	if err := c.Get(ctx, req.NamespacedName, after); err != nil {
		t.Fatal(err)
	}
	if len(after.Finalizers) != 0 {
		t.Fatalf("expected claim finalizers to be removed, got %v", after.Finalizers)
	}
}

// TestSynchronizedSnapshotRace forces two claims to allocate from exactly the
// same snapshot by gating their creates on a barrier: both observe the same free
// address, one create wins, the loser must move on to the next candidate.
func TestSynchronizedSnapshotRace(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", "10.0.0.1", "10.0.0.2")); err != nil {
		t.Fatal(err)
	}
	claims := makeConcurrencyClaims(testNamespace, inClusterIPPoolKind, "pool", 2)

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var createCount atomic.Int32
	c.PreCreate = func(client.Object) {
		if createCount.Add(1) <= 2 {
			entered <- struct{}{}
			<-release
		}
	}

	var wg sync.WaitGroup
	results := make([]*ipamv1.IPAddress, 2)
	errs := make([]error, 2)
	for i, claim := range claims {
		wg.Add(1)
		go func() {
			defer wg.Done()
			address, _, err := ensureAddressRound(ctx, c, scheme, claim)
			results[i], errs[i] = address, err
		}()
	}

	// Both goroutines have read their snapshot and are about to create.
	<-entered
	<-entered
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("claim %d failed: %v", i, err)
		}
	}
	if results[0] == nil || results[1] == nil {
		t.Fatalf("both claims must succeed, got %v and %v", results[0], results[1])
	}
	if results[0].Spec.Address == results[1].Spec.Address {
		t.Fatalf("both claims hold %s", results[0].Spec.Address)
	}
}

// TestGlobalPoolCrossNamespaceRace: IPAddress objects are namespaced, so for
// global pools create uniqueness alone cannot prevent two claims in different
// namespaces from winning the same address. The post-create verification must
// resolve the duplicate deterministically.
func TestGlobalPoolCrossNamespaceRace(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newGlobalPool("global-pool", "10.0.0.1", "10.0.0.2")); err != nil {
		t.Fatal(err)
	}
	claims := []*ipamv1.IPAddressClaim{
		newConcurrencyClaim("claim-a", "ns-one", globalInClusterIPPoolKind, "global-pool"),
		newConcurrencyClaim("claim-b", "ns-two", globalInClusterIPPoolKind, "global-pool"),
	}

	// Both claims create from the same empty snapshot; both creates succeed
	// (different namespaces), so the duplicate must be detected afterwards.
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var createCount atomic.Int32
	c.PreCreate = func(client.Object) {
		if createCount.Add(1) <= 2 {
			entered <- struct{}{}
			<-release
		}
	}

	var wg sync.WaitGroup
	for _, claim := range claims {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = ensureAddressRound(ctx, c, scheme, claim)
		}()
	}
	<-entered
	<-entered
	close(release)
	wg.Wait()

	// Whatever the interleaving, at most one object may hold each address.
	countByAddress := map[string]int{}
	for _, obj := range storedAddresses(c) {
		countByAddress[obj.(*ipamv1.IPAddress).Spec.Address]++
	}
	for addr, n := range countByAddress {
		if n > 1 {
			t.Fatalf("address %s is held by %d objects", addr, n)
		}
	}

	// Reconciling both claims again (e.g. after the loser's status pointed at a
	// since-deleted object) leaves both claims with distinct addresses.
	first, _, err := ensureAddressRound(ctx, c, scheme, claims[0])
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := ensureAddressRound(ctx, c, scheme, claims[1])
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || second == nil || first.Spec.Address == second.Spec.Address {
		t.Fatalf("claims must hold distinct addresses, got %v and %v", first, second)
	}
}

// TestAllocationContentionIsNotExhaustion: when every create conflicts but the
// pool has free addresses, the outcome must be a retriable contention error
// after a bounded number of attempts - never a false "pool exhausted".
func TestAllocationContentionIsNotExhaustion(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", 10)...)); err != nil {
		t.Fatal(err)
	}
	claim := newConcurrencyClaim("blocked", testNamespace, inClusterIPPoolKind, "pool")

	var creates atomic.Int32
	c.CreateError = func(obj client.Object) error {
		if _, ok := obj.(*ipamv1.IPAddress); ok {
			creates.Add(1)
			return apierrors.NewAlreadyExists(schema.GroupResource{Group: ipamv1.GroupVersion.Group, Resource: "ipaddresses"}, obj.GetName())
		}
		return nil
	}

	h := &IPAddressClaimHandler{Client: c, Scheme: scheme, claim: claim}
	if _, _, err := h.FetchPool(ctx); err != nil {
		t.Fatal(err)
	}
	address, res, err := h.EnsureAddress(ctx)
	if address != nil || res != nil {
		t.Fatalf("expected no address and no result, got %v and %+v", address, res)
	}
	if !errors.Is(err, poolutil.ErrAllocationContention) {
		t.Fatalf("expected ErrAllocationContention, got %v", err)
	}
	if got := creates.Load(); got != poolutil.MaxAllocationAttempts {
		t.Fatalf("expected exactly %d create attempts, got %d", poolutil.MaxAllocationAttempts, got)
	}
}

// TestFreedAddressesAreReused: under churn (claims created and deleted), the
// lowest freed address is picked first, so the pool does not fragment.
func TestFreedAddressesAreReused(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", 10)...)); err != nil {
		t.Fatal(err)
	}

	kept := make([]*ipamv1.IPAddress, 0, 5)
	for i := range 5 {
		claim := newConcurrencyClaim(fmt.Sprintf("claim-%d", i), testNamespace, inClusterIPPoolKind, "pool")
		address, _, err := ensureAddressRound(ctx, c, scheme, claim)
		if err != nil || address == nil {
			t.Fatalf("allocation %d failed: %v", i, err)
		}
		kept = append(kept, address)
	}
	if err := c.Delete(ctx, kept[1]); err != nil {
		t.Fatal(err)
	}

	claim := newConcurrencyClaim("newcomer", testNamespace, inClusterIPPoolKind, "pool")
	address, _, err := ensureAddressRound(ctx, c, scheme, claim)
	if err != nil || address == nil {
		t.Fatalf("allocation failed: %v", err)
	}
	if want := kept[1].Spec.Address; address.Spec.Address != want {
		t.Fatalf("expected freed address %s to be reused, got %s", want, address.Spec.Address)
	}
}

// TestPreferredAddressReacquired: if the claim status records an address but the
// IPAddress object was lost out-of-band, the next allocation re-acquires the same
// address instead of handing out a new one.
func TestPreferredAddressReacquired(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", 10)...)); err != nil {
		t.Fatal(err)
	}
	first, _, err := ensureAddressRound(ctx, c, scheme, newConcurrencyClaim("holder", testNamespace, inClusterIPPoolKind, "pool"))
	if err != nil || first == nil {
		t.Fatalf("allocation failed: %v", err)
	}

	// Simulate the object being lost while the status still references it.
	if err := c.Delete(ctx, first); err != nil {
		t.Fatal(err)
	}

	retry := newConcurrencyClaim("holder", testNamespace, inClusterIPPoolKind, "pool")
	retry.Status.AddressRef = ipamv1.IPAddressReference{Name: first.Name}
	again, _, err := ensureAddressRound(ctx, c, scheme, retry)
	if err != nil || again == nil {
		t.Fatalf("re-allocation failed: %v", err)
	}
	if again.Name != first.Name || again.Spec.Address != first.Spec.Address {
		t.Fatalf("expected the recorded address %s to be re-acquired, got %s", first.Spec.Address, again.Spec.Address)
	}
}

// TestHighChurnReusesAddresses: under rapid claim creation and deletion the
// pool must keep handing out a compact set of addresses instead of probing ever
// further forward and fragmenting to exhaustion prematurely.
func TestHighChurnReusesAddresses(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	const poolSize = 20
	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", poolSize)...)); err != nil {
		t.Fatal(err)
	}

	// Repeatedly allocate a batch of claims, then delete them. A fresh batch of
	// the same size must always be satisfiable: without reuse the pool would
	// exhaust after the first batch.
	for round := range 5 {
		batch := make([]*ipamv1.IPAddress, 0, poolSize)
		for i := range poolSize {
			claim := newConcurrencyClaim(fmt.Sprintf("churn-%d-%d", round, i), testNamespace, inClusterIPPoolKind, "pool")
			address, res, err := ensureAddressRound(ctx, c, scheme, claim)
			if err != nil {
				t.Fatalf("round %d allocation %d failed: %v", round, i, err)
			}
			if address == nil {
				t.Fatalf("round %d allocation %d: pool exhausted unexpectedly (res=%+v)", round, i, res)
			}
			batch = append(batch, address)
		}
		for _, address := range batch {
			if err := c.Delete(ctx, address); err != nil {
				t.Fatal(err)
			}
		}
	}

	if objs := storedAddresses(c); len(objs) != 0 {
		t.Fatalf("expected all addresses released, got %d", len(objs))
	}
}

// TestConcurrentSameClaimConverges: many goroutines process the same claim
// concurrently (as replicas would). They must all return the same address and
// leave exactly one IPAddress object.
func TestConcurrentSameClaimConverges(t *testing.T) {
	scheme := newConcurrencyScheme(t)
	c := newConcurrencyClient(t, scheme)
	ctx := context.Background()

	if err := c.Create(ctx, newNamespacedPool("pool", rangeAddresses("10.0.0", 50)...)); err != nil {
		t.Fatal(err)
	}
	claim := newConcurrencyClaim("contended", testNamespace, inClusterIPPoolKind, "pool")

	c.PreCreate = func(client.Object) { goruntime.Gosched() }

	const workers = 25
	var wg sync.WaitGroup
	names := make([]string, workers)
	errs := make([]error, workers)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each worker reconciles its own copy of the claim, as separate
			// cache reads would produce.
			local := claim.DeepCopy()
			address, _, err := ensureAddressRound(ctx, c, scheme, local)
			if err != nil {
				errs[i] = err
				return
			}
			if address != nil {
				names[i] = address.Name
			}
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d failed: %v", i, err)
		}
	}
	// Simultaneous replicas may transiently observe different addresses (one
	// creates an address and another replica, converging on a different winner,
	// releases it). The invariant that must hold is eventual convergence: after
	// the dust settles exactly one object remains, and every subsequent
	// reconcile of the claim returns that same object.
	if objs := storedAddresses(c); len(objs) != 1 {
		names := make([]string, 0, len(objs))
		for _, obj := range objs {
			names = append(names, obj.GetName())
		}
		t.Fatalf("expected exactly 1 IPAddress object after convergence, got %d: %v", len(objs), names)
	}
	settled := storedAddresses(c)[0].GetName()

	distinct := map[string]bool{}
	for range workers {
		local := claim.DeepCopy()
		address, _, err := ensureAddressRound(ctx, c, scheme, local)
		if err != nil {
			t.Fatalf("post-convergence reconcile failed: %v", err)
		}
		if address == nil {
			t.Fatal("post-convergence reconcile returned no address")
		}
		distinct[address.Name] = true
	}
	if len(distinct) != 1 || !distinct[settled] {
		t.Fatalf("expected every reconcile to return the settled address %s, got %v", settled, distinct)
	}
}

func splitClaimKey(key string) (string, string) {
	ns, name, _ := strings.Cut(key, "/")
	return ns, name
}
