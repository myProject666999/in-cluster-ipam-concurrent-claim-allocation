package ipamutil

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	clusterutil "sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/predicates"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// ReleaseAddressFinalizer is used to release an IP address before cleaning up the claim.
	ReleaseAddressFinalizer = "ipam.cluster.x-k8s.io/ReleaseAddress"

	// ProtectAddressFinalizer is used to prevent deletion of an IPAddress object while its claim is not deleted.
	ProtectAddressFinalizer = "ipam.cluster.x-k8s.io/ProtectAddress"
)

// ClaimReconciler reconciles a IPAddressClaim object using a ProviderAdapter.
// It can be used to implement custom IPAM providers without worrying about the basic lifecycle, pausing and owner
// references, which should be the same or very similar for any provider.
// The custom implementation for a specific provider is provided by implementing the ProviderAdapter interface. The
// ClaimReconciler can then be used as follows, with controllers.InClusterProviderAdapter serving
// as the provider implementation.
//
//	(&ipamutil.ClaimReconciler{
//		Client:           mgr.GetClient(),
//		Scheme:           mgr.GetScheme(),
//		WatchFilterValue: watchFilter,
//		Adapter: &controllers.InClusterProviderAdapter{},
//	}).SetupWithManager(ctx, mgr)
type ClaimReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	WatchFilterValue string

	Adapter ProviderAdapter
}

// ProviderAdapter is an interface that must be implemented by the IPAM provider.
type ProviderAdapter interface {
	// SetupWithManager will be called during the setup of the controller for the ClaimReconciler to allow the provider
	// implementation to extend the controller configuration.
	SetupWithManager(context.Context, *ctrl.Builder) error
	// ClaimHandlerFor is called during reconciliation to get a ClaimHandler for the reconciled [ipamv1.IPAddressClaim].
	ClaimHandlerFor(client.Client, *ipamv1.IPAddressClaim) ClaimHandler
}

// ClaimHandler knows how to allocate and release IP addresses for a specific provider.
type ClaimHandler interface {
	// FetchPool is called to fetch the pool referenced by the claim. The pool needs to be stored by the handler.
	// This method is called before EnsureAddress and ReleaseAddress.
	// Note that the ClaimReconciler will call the ReleaseAddress method regardless whether fetching the pool was
	// successful or not.
	FetchPool(ctx context.Context) (client.Object, *ctrl.Result, error)
	// EnsureAddress makes sure that exactly one IPAddress exists for the claim and returns it.
	//
	// Implementations must be idempotent (reconciling the same claim again must return the same
	// address) and safe for concurrent use by any number of goroutines and controller replicas.
	// Providers must rely on the atomicity of the storage layer - e.g. unique object names or
	// resource-version preconditions - and must not assume single-threaded or single-replica
	// execution: during a leader-election handover an old and a new replica can process the same
	// claim at the same time.
	//
	// A non-nil result with a nil error means no address could be allocated yet (e.g. the pool is
	// exhausted) and the claim should be reconciled again later. It will only be called when
	// FetchPool was successful.
	EnsureAddress(ctx context.Context) (*ipamv1.IPAddress, *ctrl.Result, error)
	// ReleaseAddress is called to release the ip address that was allocated for the claim.
	// It will be called even if FetchPool was not successful when reconciling a deleted claim, so there is no guarantee
	// that the claim is available in the handler.
	ReleaseAddress(ctx context.Context) (*ctrl.Result, error)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClaimReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if r.Adapter == nil {
		return fmt.Errorf("error setting the manager: Adapter is nil")
	}

	if err := mgr.GetFieldIndexer().IndexField(ctx, &ipamv1.IPAddressClaim{}, "clusterName", indexClusterName); err != nil {
		return fmt.Errorf("failed to register indexer for IPAddressClaim: %w", err)
	}

	b := ctrl.NewControllerManagedBy(mgr).
		WithEventFilter(predicates.ResourceNotPausedAndHasFilterLabel(mgr.GetScheme(), ctrl.LoggerFrom(ctx), r.WatchFilterValue)).
		// A Watch is added for the Cluster in the case that the Cluster is
		// unpaused so that a request can be queued to re-reconcile the
		// IPAddressClaim.
		Watches(
			&clusterv1.Cluster{},
			ctrlhandler.EnqueueRequestsFromMapFunc(r.clusterToIPClaims),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldCluster := e.ObjectOld.(*clusterv1.Cluster)
					newCluster := e.ObjectNew.(*clusterv1.Cluster)
					return annotations.IsPaused(oldCluster, oldCluster) && !annotations.IsPaused(newCluster, newCluster)
				},
				CreateFunc: func(e event.CreateEvent) bool {
					cluster := e.Object.(*clusterv1.Cluster)
					return !annotations.IsPaused(cluster, cluster)
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					cluster := e.Object.(*clusterv1.Cluster)
					return !annotations.IsPaused(cluster, cluster)
				},
			}),
		)

	if err := r.Adapter.SetupWithManager(ctx, b); err != nil {
		return err
	}
	return b.Complete(r)
}

// Reconcile is called by the controller to reconcile a claim.
func (r *ClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, reterr error) {
	log := ctrl.LoggerFrom(ctx)

	// Fetch the IPAddressClaim
	claim := &ipamv1.IPAddressClaim{}
	if err := r.Get(ctx, req.NamespacedName, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if the owning cluster is paused
	var cluster *clusterv1.Cluster
	var err error
	if claim.Spec.ClusterName != "" {
		cluster, err = clusterutil.GetClusterByName(ctx, r.Client, claim.Namespace, claim.Spec.ClusterName)
	} else if _, ok := claim.GetLabels()[clusterv1.ClusterNameLabel]; ok {
		cluster, err = clusterutil.GetClusterFromMetadata(ctx, r.Client, claim.ObjectMeta)
	}
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "error fetching cluster linked to IPAddressClaim")
			return ctrl.Result{}, err
		}
		if !claim.ObjectMeta.DeletionTimestamp.IsZero() {
			// In case the claim is deleted, we'll process it even when we can't find the cluster. During cluster deletion it
			// can happen that the Cluster object is released before all claims are cleaned up, which would block the claims indefinitely.
			log.Info("IPAddressClaim linked to a cluster that is not found, unable to determine cluster's paused state, proceeding with deletion")
		} else {
			log.Info("IPAddressClaim linked to a cluster that is not found, unable to determine cluster's paused state, skipping reconciliation")
			return ctrl.Result{}, nil
		}
	}
	if cluster != nil {
		if annotations.IsPaused(cluster, cluster) {
			log.Info("IPAddressClaim linked to a cluster that is paused, skipping reconciliation")
			return ctrl.Result{}, nil
		}
	}

	// Create a patch helper for the claim.
	patchHelper, err := patch.NewHelper(claim, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}

	defer func() {
		if err := patchHelper.Patch(ctx, claim); err != nil {
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}
	}()

	if controllerutil.AddFinalizer(claim, ReleaseAddressFinalizer) {
		return ctrl.Result{}, nil
	}

	var res *reconcile.Result
	var pool client.Object

	// Create the provider handler and fetch the pool.
	handler := r.Adapter.ClaimHandlerFor(r.Client, claim)
	if pool, res, err = handler.FetchPool(ctx); err != nil || res != nil {
		if apierrors.IsNotFound(err) {
			err := fmt.Errorf("pool not found: %w", err)
			log.Error(err, "the referenced pool could not be found")
			if !claim.ObjectMeta.DeletionTimestamp.IsZero() {
				return r.reconcileDelete(ctx, claim, handler)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrap(err, "failed to fetch pool")
	}

	if pool == nil {
		err = fmt.Errorf("pool is nil")
		log.Error(err, "pool error")
		return ctrl.Result{}, errors.Wrap(err, "reconciliation failed")
	}

	if annotations.HasPaused(pool) {
		log.Info("IPAddressClaim references Pool which is paused, skipping reconciliation.", "IPAddressClaim", claim.GetName(), "Pool", pool.GetName())
		return ctrl.Result{}, nil
	}

	if !claim.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, claim, handler)
	}

	// The handler ensures an IPAddress exists for the claim. For the in-cluster provider this
	// relies on the atomicity of the storage layer (create conflicts on address-derived object
	// names), so it is safe no matter how many reconciler goroutines or controller replicas run
	// concurrently - including an old and a new leader overlapping during a handover.
	address, res, err := handler.EnsureAddress(ctx)
	if res != nil || err != nil {
		if err != nil {
			err = errors.Wrap(err, "failed to ensure address")
		}
		return unwrapResult(res), err
	}

	// We need to ensure the address is properly watched by controller-runtime client (as it is cached) before moving
	// to the next reconciliation request.
	// This is required, otherwise if the next "EnsureAddress" is called too fast, or the controller-runtime cached client informer
	// is too slow, we may end up not seeing that the IP Address is being used and can provide duplicated IPs.
	err = wait.PollUntilContextTimeout(ctx, 5*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		key := client.ObjectKeyFromObject(address)
		if err := r.Client.Get(ctx, key, &ipamv1.IPAddress{}); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return true, nil
	})

	if err != nil {
		log.Error(err, "failed waiting for IPAddress to be visible in the cache after create", "namespace", address.GetNamespace(), "name", address.GetName())
		return ctrl.Result{}, errors.Wrapf(err, "failed waiting for IPAddress %s/%s to be visible in the cache after create", address.GetNamespace(), address.GetName())
	}

	log.Info(fmt.Sprintf("IPAddress %s/%s (%s) has been ensured", address.Namespace, address.Name, address.Spec.Address),
		"IPAddressClaim", fmt.Sprintf("%s/%s", claim.Namespace, claim.Name))

	if !address.DeletionTimestamp.IsZero() {
		// We prevent deleting IPAddresses while their corresponding IPClaim still exists since we cannot guarantee that the IP
		// wil remain the same when we recreate it.
		log.Info("Address is marked for deletion, but deletion is prevented until the claim is deleted as well", "address", address.Name)
	}

	claim.Status.AddressRef = ipamv1.IPAddressReference{Name: address.Name}

	return ctrl.Result{}, nil
}

func (r *ClaimReconciler) reconcileDelete(ctx context.Context, claim *ipamv1.IPAddressClaim, handler ClaimHandler) (ctrl.Result, error) {
	res, err := handler.ReleaseAddress(ctx)
	if err != nil {
		return unwrapResult(res), fmt.Errorf("release address: %w", err)
	}
	if res != nil && (res.Requeue || res.RequeueAfter > 0) {
		return *res, nil
	}

	address, err := r.findAddressForClaim(ctx, claim)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to fetch address")
	}

	if address != nil {
		var err error
		p := client.MergeFrom(address.DeepCopy())
		if controllerutil.RemoveFinalizer(address, ProtectAddressFinalizer) {
			if err = r.Client.Patch(ctx, address, p); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, errors.Wrap(err, "failed to remove address finalizer")
			}
		}

		if err == nil {
			if err := r.Client.Delete(ctx, address); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
	}

	controllerutil.RemoveFinalizer(claim, ReleaseAddressFinalizer)
	return ctrl.Result{}, nil
}

// findAddressForClaim locates the IPAddress that belongs to the given claim.
//
// It first tries the name recorded in the claim status. If that is unset or
// stale (for example because the status update failed after the address was
// created), it falls back to scanning the claim's namespace for an IPAddress
// whose spec references both this claim and this pool. That scan is what makes
// an in-flight allocation recoverable: the address object itself carries enough
// ownership information (claimRef, poolRef, owner references) to find it again,
// so a claim deleted mid-allocation does not leak an unreachable address.
func (r *ClaimReconciler) findAddressForClaim(ctx context.Context, claim *ipamv1.IPAddressClaim) (*ipamv1.IPAddress, error) {
	if name := claim.Status.AddressRef.Name; name != "" {
		address := &ipamv1.IPAddress{}
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: name}, address)
		if err == nil {
			return address, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
	}

	addresses := &ipamv1.IPAddressList{}
	if err := r.Client.List(ctx, addresses, client.InNamespace(claim.Namespace)); err != nil {
		return nil, err
	}
	var matches []ipamv1.IPAddress
	for _, a := range addresses.Items {
		if a.Spec.ClaimRef.Name == claim.Name &&
			a.Spec.PoolRef.Name == claim.Spec.PoolRef.Name &&
			a.Spec.PoolRef.Kind == claim.Spec.PoolRef.Kind {
			matches = append(matches, a)
		}
	}
	if len(matches) == 0 {
		return nil, nil
	}
	// Deterministic choice if several objects reference the claim; the
	// allocation path converges them to this same winner.
	slices.SortFunc(matches, func(a, b ipamv1.IPAddress) int { return strings.Compare(a.Name, b.Name) })
	return &matches[0], nil
}

func (r *ClaimReconciler) clusterToIPClaims(_ context.Context, a client.Object) []reconcile.Request {
	requests := []reconcile.Request{}
	claims := &ipamv1.IPAddressClaimList{}
	if err := r.List(context.Background(), claims, client.MatchingFields{"clusterName": a.GetName()}); err != nil {
		return requests
	}
	for _, c := range claims.Items {
		r := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      c.Name,
				Namespace: c.Namespace,
			},
		}
		requests = append(requests, r)
	}
	return requests
}

func indexClusterName(object client.Object) []string {
	claim, ok := object.(*ipamv1.IPAddressClaim)
	if !ok {
		return nil
	}
	if claim.Spec.ClusterName != "" {
		return []string{claim.Spec.ClusterName}
	}
	if claim.Labels[clusterv1.ClusterNameLabel] != "" {
		return []string{claim.Labels[clusterv1.ClusterNameLabel]}
	}
	return nil
}

func unwrapResult(res *ctrl.Result) ctrl.Result {
	if res == nil {
		return ctrl.Result{}
	}
	return *res
}
