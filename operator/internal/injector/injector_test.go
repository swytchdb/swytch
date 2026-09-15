package injector

import (
	"context"
	"encoding/json"
	"testing"

	api "github.com/swytchdb/swytch/operator/api/v1alpha1"
	"github.com/swytchdb/swytch/operator/internal/workload"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestAdmission(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = api.AddToScheme(scheme)
	s := &api.Swytch{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "apps", UID: "uid"}, Spec: api.SwytchSpec{Deployment: "sidecar", Memory: resource.MustParse("512Mi"), Discovery: api.Discovery{Cloud: &api.DiscoveryConfig{SecretKeyRef: api.SecretKeyRef{Name: "secret", Key: "key"}}}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s).Build()
	h := Handler{Reader: c, DefaultImage: "swytch:test"}
	for _, tc := range []struct {
		name, namespace, profile string
		annotated, allowed       bool
	}{
		{"unannotated", "apps", "", false, true},
		{"valid", "apps", "cache", true, true},
		{"missing", "apps", "missing", true, false},
		{"namespace-isolation", "other", "cache", true, false},
		{"empty", "apps", "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app"}}}}
			if tc.annotated {
				p.Annotations = map[string]string{workload.Annotation: tc.profile}
			}
			raw, _ := json.Marshal(p)
			resp := h.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{Operation: admissionv1.Create, Namespace: tc.namespace, Resource: metav1.GroupVersionResource{Version: "v1", Resource: "pods"}, Object: runtime.RawExtension{Raw: raw}}})
			if resp.Allowed != tc.allowed {
				t.Fatalf("unexpected response: %+v", resp)
			}
			if tc.name == "valid" && len(resp.Patches) == 0 {
				t.Fatal("no injection patch")
			}
			if !tc.annotated && len(resp.Patches) != 0 {
				t.Fatal("patched unannotated Pod")
			}
		})
	}
}
