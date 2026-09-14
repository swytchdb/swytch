// Package v1alpha1 defines the namespaced Swytch deployment profile.
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "swytch.getswytch.com", Version: "v1alpha1"}

func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &Swytch{}, &SwytchList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}

type SecretKeyRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type DiscoveryConfig struct {
	SecretKeyRef SecretKeyRef `json:"secretKeyRef"`
}

type Discovery struct {
	DNS   *DiscoveryConfig `json:"dns,omitempty"`
	Cloud *DiscoveryConfig `json:"cloud,omitempty"`
}

type Sidecar struct {
	Transport string `json:"transport,omitempty"`
}

type Server struct {
	Replicas *int32 `json:"replicas,omitempty"`
}

type Limits struct {
	CPU *resource.Quantity `json:"cpu,omitempty"`
}

type SwytchSpec struct {
	Deployment string              `json:"deployment"`
	Memory     resource.Quantity   `json:"memory"`
	Requests   corev1.ResourceList `json:"requests,omitempty"`
	Limits     Limits              `json:"limits,omitempty"`
	Image      string              `json:"image,omitempty"`
	Sidecar    *Sidecar            `json:"sidecar,omitempty"`
	Server     *Server             `json:"server,omitempty"`
	Discovery  Discovery           `json:"discovery"`
}

type SwytchStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

type Swytch struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SwytchSpec   `json:"spec"`
	Status            SwytchStatus `json:"status,omitempty"`
}

type SwytchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Swytch `json:"items"`
}

func (s *Swytch) DeepCopy() *Swytch {
	if s == nil {
		return nil
	}
	out := *s
	out.ObjectMeta = *s.ObjectMeta.DeepCopy()
	out.Spec.Memory = s.Spec.Memory.DeepCopy()
	if s.Spec.Requests != nil {
		out.Spec.Requests = make(corev1.ResourceList, len(s.Spec.Requests))
		for k, v := range s.Spec.Requests {
			out.Spec.Requests[k] = v.DeepCopy()
		}
	}
	if s.Spec.Limits.CPU != nil {
		q := s.Spec.Limits.CPU.DeepCopy()
		out.Spec.Limits.CPU = &q
	}
	if s.Spec.Sidecar != nil {
		v := *s.Spec.Sidecar
		out.Spec.Sidecar = &v
	}
	if s.Spec.Server != nil {
		v := *s.Spec.Server
		out.Spec.Server = &v
		if v.Replicas != nil {
			n := *v.Replicas
			out.Spec.Server.Replicas = &n
		}
	}
	if s.Spec.Discovery.DNS != nil {
		v := *s.Spec.Discovery.DNS
		out.Spec.Discovery.DNS = &v
	}
	if s.Spec.Discovery.Cloud != nil {
		v := *s.Spec.Discovery.Cloud
		out.Spec.Discovery.Cloud = &v
	}
	out.Status.Conditions = append([]metav1.Condition(nil), s.Status.Conditions...)
	return &out
}

func (s *Swytch) DeepCopyObject() runtime.Object { return s.DeepCopy() }

func (s *SwytchList) DeepCopyObject() runtime.Object {
	if s == nil {
		return nil
	}
	out := *s
	out.ListMeta = *s.ListMeta.DeepCopy()
	out.Items = make([]Swytch, len(s.Items))
	for i := range s.Items {
		out.Items[i] = *s.Items[i].DeepCopy()
	}
	return &out
}
