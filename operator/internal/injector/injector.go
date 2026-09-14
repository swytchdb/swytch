package injector

import (
	"context"
	"encoding/json"
	"net/http"

	api "github.com/swytchdb/swytch/operator/api/v1alpha1"
	"github.com/swytchdb/swytch/operator/internal/workload"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type Handler struct {
	Reader           client.Reader
	DefaultImage     string
	ImagePullSecrets []corev1.LocalObjectReference
}

func (h *Handler) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Create || req.Resource.Group != "" || req.Resource.Resource != "pods" || req.SubResource != "" {
		return admission.Allowed("not a Pod creation")
	}
	p := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, p); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	name, annotated := p.Annotations[workload.Annotation]
	if !annotated {
		return admission.Allowed("no Swytch annotation")
	}
	if name == "" {
		return admission.Denied("Swytch profile annotation must not be empty")
	}
	s := &api.Swytch{}
	if err := h.Reader.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: name}, s); err != nil {
		return admission.Denied("Swytch profile is unavailable in this namespace: " + err.Error())
	}
	if !s.DeletionTimestamp.IsZero() {
		return admission.Denied("Swytch profile is being deleted")
	}
	if err := workload.Inject(p, s, h.DefaultImage); err != nil {
		return admission.Denied(err.Error())
	}
	for _, secret := range h.ImagePullSecrets {
		found := false
		for _, existing := range p.Spec.ImagePullSecrets {
			if existing.Name == secret.Name {
				found = true
				break
			}
		}
		if !found {
			p.Spec.ImagePullSecrets = append(p.Spec.ImagePullSecrets, secret)
		}
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}
