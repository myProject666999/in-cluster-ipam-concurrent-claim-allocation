/*
Copyright 2023 The Kubernetes Authors.

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

// Package ipamutil implements various utility functions to assist with CAPI IPAM implementation.
package ipamutil

import (
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ipamv1 "sigs.k8s.io/cluster-api/api/ipam/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// NewIPAddress creates a new ipamv1.IPAddress with references to a pool and claim.
//
// The object is named after the claim. Callers that derive the object name from
// the allocated address (see poolutil.AddressToObjectName) override the name
// before persisting the object; the name is what makes the API server's create
// uniqueness serialize concurrent allocators.
func NewIPAddress(claim *ipamv1.IPAddressClaim, pool client.Object) ipamv1.IPAddress {
	poolGVK := pool.GetObjectKind().GroupVersionKind()

	return ipamv1.IPAddress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claim.Name,
			Namespace: claim.Namespace,
		},
		Spec: ipamv1.IPAddressSpec{
			ClaimRef: ipamv1.IPAddressClaimReference{
				Name: claim.Name,
			},
			PoolRef: ipamv1.IPPoolReference{
				APIGroup: poolGVK.Group,
				Kind:     poolGVK.Kind,
				Name:     pool.GetName(),
			},
		},
	}
}

// EnsureIPAddressMetadata ensures that an IPAddress carries everything needed
// to identify and protect it: the claim and pool owner references, the cluster
// name label of the claim (if any) and the ProtectAddress finalizer.
//
// The claim reference in the spec plus these owner references are the
// "ownership trace" that lets the controller recover an allocation if the
// claim's status update failed, and that lets garbage collection and the delete
// path find the address if the claim is deleted while an allocation is in
// flight.
func EnsureIPAddressMetadata(scheme *runtime.Scheme, address *ipamv1.IPAddress, claim *ipamv1.IPAddressClaim, pool client.Object) error {
	if err := EnsureIPAddressOwnerReferences(scheme, address, claim, pool); err != nil {
		return err
	}

	if val, ok := claim.Labels[clusterv1.ClusterNameLabel]; ok {
		if address.Labels == nil {
			address.Labels = make(map[string]string)
		}
		address.Labels[clusterv1.ClusterNameLabel] = val
	}

	_ = controllerutil.AddFinalizer(address, ProtectAddressFinalizer)

	return nil
}

// EnsureIPAddressOwnerReferences ensures that an IPAddress has the
// IPAddressClaim and IPPool as an OwnerReference.
func EnsureIPAddressOwnerReferences(scheme *runtime.Scheme, address *ipamv1.IPAddress, claim *ipamv1.IPAddressClaim, pool client.Object) error {
	if err := controllerutil.SetControllerReference(claim, address, scheme); err != nil {
		if _, ok := err.(*controllerutil.AlreadyOwnedError); !ok {
			return errors.Wrap(err, "Failed to update address's claim owner reference")
		}
	}

	if err := controllerutil.SetOwnerReference(pool, address, scheme); err != nil {
		return errors.Wrap(err, "Failed to update address's pool owner reference")
	}

	var poolRefIdx int
	poolGVK := pool.GetObjectKind().GroupVersionKind()
	for i, ownerRef := range address.GetOwnerReferences() {
		if ownerRef.APIVersion == poolGVK.GroupVersion().String() &&
			ownerRef.Kind == poolGVK.Kind &&
			ownerRef.Name == pool.GetName() {
			poolRefIdx = i
		}
	}

	address.OwnerReferences[poolRefIdx].Controller = ptr.To(false)
	address.OwnerReferences[poolRefIdx].BlockOwnerDeletion = ptr.To(true)

	return nil
}
