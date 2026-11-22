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

// Package fakeclient implements an in-memory Kubernetes client for tests.
//
// Unlike a purely map-based stub, it strictly implements the two storage
// guarantees the IPAM allocation logic relies on:
//
//   - Create fails with AlreadyExists if an object with the same key exists.
//   - Update and Patch fail with Conflict when the object's resourceVersion does
//     not match the stored one (optimistic concurrency).
//
// It also allows tests to inject scheduling points between operations (for
// example between reading the in-use snapshot and creating an object) via the
// Pre* hooks. The hooks run while the store lock is NOT held, so concurrent
// goroutines interleave exactly the way real API server round trips would.
//
// Deliberate simplifications: Delete removes objects immediately (finalizers are
// not honored), there is no watch support, and status writes update the whole
// object.
package fakeclient

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	jsonpatch "github.com/evanphx/json-patch/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Client is an in-memory implementation of client.Client. It is safe for
// concurrent use.
type Client struct {
	mu        sync.Mutex
	scheme    *runtime.Scheme
	objects   map[schema.GroupVersionKind]map[types.NamespacedName]client.Object
	rvCounter int64
	indexes   map[schema.GroupVersionKind]map[string]client.IndexerFunc

	// Pre* hooks are invoked before the corresponding operation touches the
	// store, without the store lock held. Tests use them to inject scheduling
	// points (runtime.Gosched, sleeps, barriers) into the read-write window of
	// the code under test.
	PreGet    func(obj client.Object)
	PreList   func(list client.ObjectList)
	PreCreate func(obj client.Object)
	PreUpdate func(obj client.Object)
	PrePatch  func(obj client.Object)
	PreDelete func(obj client.Object)

	// *Error hooks, when set, are invoked before the corresponding operation
	// and their result is returned as the operation's error. Tests use them to
	// inject API failures such as a failed status update.
	GetError    func(key types.NamespacedName) error
	CreateError func(obj client.Object) error
	UpdateError func(obj client.Object) error
	PatchError  func(obj client.Object) error
	DeleteError func(obj client.Object) error
}

// New returns an empty Client using the given scheme.
func New(scheme *runtime.Scheme) *Client {
	return &Client{
		scheme:  scheme,
		objects: map[schema.GroupVersionKind]map[types.NamespacedName]client.Object{},
		indexes: map[schema.GroupVersionKind]map[string]client.IndexerFunc{},
	}
}

var _ client.Client = &Client{}

// WithIndex registers a field index for the given object type, mirroring the
// behavior of the controller-runtime cache: List calls with a matching
// MatchingFields selector are resolved through the index function.
func (c *Client) WithIndex(obj client.Object, field string, fn client.IndexerFunc) *Client {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		panic(err)
	}
	if c.indexes[gvk] == nil {
		c.indexes[gvk] = map[string]client.IndexerFunc{}
	}
	c.indexes[gvk][field] = fn
	return c
}

// AddObject inserts an object directly into the store, bypassing hooks and
// create semantics. It allows tests to set up state that Create could not
// produce, e.g. objects with a deletion timestamp.
func (c *Client) AddObject(obj client.Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storeLocked(obj)
}

// Objects returns deep copies of all stored objects of the given type.
func (c *Client) Objects(obj client.Object) []client.Object {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		panic(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]client.Object, 0, len(c.objects[gvk]))
	for _, o := range c.objects[gvk] {
		out = append(out, o.DeepCopyObject().(client.Object))
	}
	return out
}

func (c *Client) gvkFor(obj runtime.Object) schema.GroupVersionKind {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		panic(err)
	}
	return gvk
}

// groupResource guesses the GroupResource for error reporting.
func groupResource(gvk schema.GroupVersionKind) schema.GroupResource {
	gvr, _ := apimeta.UnsafeGuessKindToResource(gvk)
	return gvr.GroupResource()
}

// storeLocked inserts or replaces obj in the store, assigning a fresh
// resourceVersion. The caller's object is updated with the stored metadata.
func (c *Client) storeLocked(obj client.Object) {
	gvk := c.gvkFor(obj)
	c.rvCounter++
	obj.SetResourceVersion(strconv.FormatInt(c.rvCounter, 10))
	if obj.GetUID() == "" {
		obj.SetUID(uuid.NewUUID())
	}
	if obj.GetCreationTimestamp().Time.IsZero() {
		obj.SetCreationTimestamp(metav1.Now())
	}
	// Like the API server, reads back include apiVersion and kind.
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	if c.objects[gvk] == nil {
		c.objects[gvk] = map[types.NamespacedName]client.Object{}
	}
	c.objects[gvk][client.ObjectKeyFromObject(obj)] = obj.DeepCopyObject().(client.Object)
}

func (c *Client) lookupLocked(gvk schema.GroupVersionKind, key types.NamespacedName) (client.Object, bool) {
	o, ok := c.objects[gvk][key]
	return o, ok
}

// Get implements client.Reader.
func (c *Client) Get(_ context.Context, key types.NamespacedName, obj client.Object, _ ...client.GetOption) error {
	gvk := c.gvkFor(obj)
	if c.GetError != nil {
		if err := c.GetError(key); err != nil {
			return err
		}
	}
	if c.PreGet != nil {
		c.PreGet(obj)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stored, ok := c.lookupLocked(gvk, key)
	if !ok {
		return apierrors.NewNotFound(groupResource(gvk), key.Name)
	}
	return copyInto(stored, obj)
}

// List implements client.Reader. It supports namespace, label and (indexed)
// field selectors.
func (c *Client) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.PreList != nil {
		c.PreList(list)
	}
	options := &client.ListOptions{}
	options.ApplyOptions(opts)

	gvk := c.gvkFor(list)
	elemGVK := schema.GroupVersionKind{Group: gvk.Group, Version: gvk.Version, Kind: strings.TrimSuffix(gvk.Kind, "List")}

	c.mu.Lock()
	defer c.mu.Unlock()

	var items []runtime.Object
	for _, stored := range c.objects[elemGVK] {
		if options.Namespace != "" && stored.GetNamespace() != options.Namespace {
			continue
		}
		if options.LabelSelector != nil && !options.LabelSelector.Matches(labels.Set(stored.GetLabels())) {
			continue
		}
		if options.FieldSelector != nil {
			matches, err := c.matchesFieldsLocked(elemGVK, stored, options.FieldSelector)
			if err != nil {
				return err
			}
			if !matches {
				continue
			}
		}
		items = append(items, stored.DeepCopyObject())
	}
	return apimeta.SetList(list, items)
}

func (c *Client) matchesFieldsLocked(gvk schema.GroupVersionKind, obj client.Object, selector fields.Selector) (bool, error) {
	for _, req := range selector.Requirements() {
		indexFn, ok := c.indexes[gvk][req.Field]
		if !ok {
			return false, fmt.Errorf("no index with name %s has been registered for %v", req.Field, gvk)
		}
		if req.Operator != selection.Equals && req.Operator != selection.DoubleEquals {
			return false, fmt.Errorf("unsupported field selector operator %q", req.Operator)
		}
		if !slices.Contains(indexFn(obj), req.Value) {
			return false, nil
		}
	}
	return true, nil
}

// Create implements client.Writer. It fails with AlreadyExists if an object
// with the same key already exists - the guarantee the IPAM allocation relies on.
func (c *Client) Create(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
	gvk := c.gvkFor(obj)
	if c.CreateError != nil {
		if err := c.CreateError(obj); err != nil {
			return err
		}
	}
	if c.PreCreate != nil {
		c.PreCreate(obj)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := client.ObjectKeyFromObject(obj)
	if _, ok := c.lookupLocked(gvk, key); ok {
		return apierrors.NewAlreadyExists(groupResource(gvk), key.Name)
	}
	c.storeLocked(obj)
	return nil
}

// Update implements client.Writer. It fails with Conflict if the object's
// resourceVersion does not match the stored one.
func (c *Client) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	gvk := c.gvkFor(obj)
	if c.UpdateError != nil {
		if err := c.UpdateError(obj); err != nil {
			return err
		}
	}
	if c.PreUpdate != nil {
		c.PreUpdate(obj)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := client.ObjectKeyFromObject(obj)
	stored, ok := c.lookupLocked(gvk, key)
	if !ok {
		return apierrors.NewNotFound(groupResource(gvk), key.Name)
	}
	if rv := obj.GetResourceVersion(); rv != "" && rv != stored.GetResourceVersion() {
		return apierrors.NewConflict(groupResource(gvk), key.Name, fmt.Errorf("resourceVersion %s does not match stored %s", rv, stored.GetResourceVersion()))
	}
	c.storeLocked(obj)
	return nil
}

// Patch implements client.Writer. Merge patches (including MergeFrom and
// RawPatch) are applied against the stored object. If the patch contains a
// metadata.resourceVersion precondition it is checked against the stored object
// and a Conflict is returned on mismatch.
func (c *Client) Patch(_ context.Context, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
	gvk := c.gvkFor(obj)
	if c.PatchError != nil {
		if err := c.PatchError(obj); err != nil {
			return err
		}
	}
	if c.PrePatch != nil {
		c.PrePatch(obj)
	}
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := client.ObjectKeyFromObject(obj)
	stored, ok := c.lookupLocked(gvk, key)
	if !ok {
		return apierrors.NewNotFound(groupResource(gvk), key.Name)
	}
	switch patch.Type() {
	case types.MergePatchType, types.StrategicMergePatchType:
	default:
		return fmt.Errorf("patch type %s is not supported by the fake client", patch.Type())
	}

	// Enforce resourceVersion preconditions (optimistic concurrency).
	var patchMeta struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(data, &patchMeta); err == nil && patchMeta.Metadata.ResourceVersion != "" {
		if patchMeta.Metadata.ResourceVersion != stored.GetResourceVersion() {
			return apierrors.NewConflict(groupResource(gvk), key.Name, fmt.Errorf("the object has been modified; expected resourceVersion %s", patchMeta.Metadata.ResourceVersion))
		}
	}

	storedJSON, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	merged, err := jsonpatch.MergePatch(storedJSON, data)
	if err != nil {
		return err
	}
	fresh, err := c.scheme.New(gvk)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(merged, fresh); err != nil {
		return err
	}
	freshObj := fresh.(client.Object)
	// A merge patch may carry a null deletionTimestamp; the API server would
	// reject it, but the fake simply ignores a cleared timestamp.
	c.storeLocked(freshObj)
	return copyInto(freshObj, obj)
}

// Delete implements client.Writer. Objects are removed immediately; finalizers
// are not honored.
func (c *Client) Delete(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
	gvk := c.gvkFor(obj)
	if c.DeleteError != nil {
		if err := c.DeleteError(obj); err != nil {
			return err
		}
	}
	if c.PreDelete != nil {
		c.PreDelete(obj)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := client.ObjectKeyFromObject(obj)
	if _, ok := c.lookupLocked(gvk, key); !ok {
		return apierrors.NewNotFound(groupResource(gvk), key.Name)
	}
	delete(c.objects[gvk], key)
	return nil
}

// DeleteAllOf implements client.Writer.
func (c *Client) DeleteAllOf(_ context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	gvk := c.gvkFor(obj)
	options := &client.DeleteAllOfOptions{}
	options.ApplyOptions(opts)
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, stored := range c.objects[gvk] {
		if options.Namespace != "" && stored.GetNamespace() != options.Namespace {
			continue
		}
		delete(c.objects[gvk], key)
	}
	return nil
}

// Apply implements client.Writer. It is not supported by the fake client.
func (c *Client) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
	return fmt.Errorf("apply is not supported by the fake client")
}

// Status returns a SubResourceWriter that operates on the whole object; the
// fake does not track subresources separately.
func (c *Client) Status() client.SubResourceWriter {
	return &subResourceClient{client: c}
}

// SubResource returns a SubResourceClient that operates on the whole object.
func (c *Client) SubResource(_ string) client.SubResourceClient {
	return &subResourceClient{client: c}
}

// Scheme implements client.Client.
func (c *Client) Scheme() *runtime.Scheme {
	return c.scheme
}

// RESTMapper implements client.Client.
func (c *Client) RESTMapper() apimeta.RESTMapper {
	mapper := apimeta.NewDefaultRESTMapper(nil)
	for gvk := range c.scheme.AllKnownTypes() {
		if strings.HasSuffix(gvk.Kind, "List") {
			continue
		}
		mapper.Add(gvk, apimeta.RESTScopeNamespace)
	}
	return mapper
}

// GroupVersionKindFor implements client.Client.
func (c *Client) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return apiutil.GVKForObject(obj, c.scheme)
}

// IsObjectNamespaced implements client.Client.
func (c *Client) IsObjectNamespaced(_ runtime.Object) (bool, error) {
	return true, nil
}

type subResourceClient struct {
	client *Client
}

var _ client.SubResourceClient = &subResourceClient{}

func (s *subResourceClient) Get(ctx context.Context, obj client.Object, subResource client.Object, _ ...client.SubResourceGetOption) error {
	return s.client.Get(ctx, client.ObjectKeyFromObject(obj), subResource)
}

func (s *subResourceClient) Create(ctx context.Context, obj client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
	return s.client.Create(ctx, obj)
}

func (s *subResourceClient) Update(ctx context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	return s.client.Update(ctx, obj)
}

func (s *subResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, _ ...client.SubResourcePatchOption) error {
	return s.client.Patch(ctx, obj, patch)
}

func (s *subResourceClient) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
	return fmt.Errorf("apply is not supported by the fake client")
}

// copyInto deep-copies src into dst via JSON round-trip, preserving the
// concrete type of dst.
func copyInto(src, dst client.Object) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dst)
}
