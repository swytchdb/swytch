package controller

import (
	"context"
	"testing"

	api "github.com/swytchdb/swytch/operator/api/v1alpha1"
	"github.com/swytchdb/swytch/operator/internal/workload"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcile(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = api.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	s := &api.Swytch{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "apps", UID: "uid"}, Spec: api.SwytchSpec{Deployment: "server", Memory: resource.MustParse("512Mi"), Discovery: api.Discovery{DNS: &api.DiscoveryConfig{SecretKeyRef: api.SecretKeyRef{Name: "secret", Key: "key"}}}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(s, &appsv1.Deployment{}).WithObjects(s).Build()
	r := &Reconciler{Client: c, Scheme: scheme, DefaultImage: "swytch:test"}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: s.Name, Namespace: s.Namespace}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	d := &appsv1.Deployment{}
	key := types.NamespacedName{Name: workload.Name(s), Namespace: s.Namespace}
	if err := c.Get(ctx, key, d); err != nil {
		t.Fatal(err)
	}
	if !metav1.IsControlledBy(d, s) || *d.Spec.Replicas != 1 || d.Spec.Template.Spec.Containers[0].Resources.Requests != nil {
		t.Fatal("incorrect server deployment")
	}
	peer := &corev1.Service{}
	if err := c.Get(ctx, types.NamespacedName{Name: workload.PeerName(s), Namespace: s.Namespace}, peer); err != nil {
		t.Fatal(err)
	}
	if peer.Spec.ClusterIP != "None" || !peer.Spec.PublishNotReadyAddresses || peer.Spec.Ports[0].Protocol != corev1.ProtocolUDP {
		t.Fatal("incorrect DNS service")
	}
	// Simulate API defaulting requests from limits; reconciliation must not loop.
	d.Spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: s.Spec.Memory}
	if err := c.Update(ctx, d); err != nil {
		t.Fatal(err)
	}
	version := d.ResourceVersion
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, key, d); err != nil {
		t.Fatal(err)
	}
	if d.ResourceVersion != version {
		t.Fatal("reconciliation overwrote API defaults")
	}
	if err := c.Get(ctx, req.NamespacedName, s); err != nil {
		t.Fatal(err)
	}
	zero := int32(0)
	s.Spec.Server = &api.Server{Replicas: &zero}
	s.Spec.Discovery.Cloud = s.Spec.Discovery.DNS
	s.Spec.Discovery.DNS = nil
	if err := c.Update(ctx, s); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, key, d); err != nil {
		t.Fatal(err)
	}
	if *d.Spec.Replicas != 0 {
		t.Fatal("scale to zero ignored")
	}
	if err := c.Get(ctx, types.NamespacedName{Name: workload.PeerName(s), Namespace: s.Namespace}, peer); err == nil {
		t.Fatal("Cloud left DNS service behind")
	}
}

func TestDoesNotAdoptUnrelatedService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = api.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	s := &api.Swytch{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "apps", UID: "uid"}}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: workload.PeerName(s), Namespace: s.Namespace}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc).Build()
	r := &Reconciler{Client: c, Scheme: scheme}
	if err := r.service(context.Background(), s, true); err == nil {
		t.Fatal("adopted unrelated service")
	}
}
