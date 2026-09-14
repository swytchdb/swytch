package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	api "github.com/swytchdb/swytch/operator/api/v1alpha1"
	"github.com/swytchdb/swytch/operator/internal/workload"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type Reconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	DefaultImage     string
	ImagePullSecrets []corev1.LocalObjectReference
}

func (r *Reconciler) SetupWithManager(m ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(m).For(&api.Swytch{}).Owns(&appsv1.Deployment{}).Owns(&corev1.Service{}).Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	s := &api.Swytch{}
	if err := r.Get(ctx, req.NamespacedName, s); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !s.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if err := workload.Validate(s); err != nil {
		return ctrl.Result{}, r.status(ctx, s, false, "InvalidConfiguration", err.Error())
	}
	if err := r.resources(ctx, s); err != nil {
		if statusErr := r.status(ctx, s, false, "ReconcileFailed", err.Error()); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{}, err
	}
	if s.Spec.Deployment == "server" {
		d := &appsv1.Deployment{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: workload.Name(s)}, d); err != nil {
			return ctrl.Result{}, err
		}
		if d.Status.ObservedGeneration < d.Generation || d.Status.UpdatedReplicas != workload.Replicas(s) || d.Status.AvailableReplicas != workload.Replicas(s) {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, r.status(ctx, s, false, "Progressing", "Waiting for server Deployment availability")
		}
	}
	return ctrl.Result{}, r.status(ctx, s, true, "Reconciled", "Deployment configuration reconciled; this does not indicate data availability")
}

func (r *Reconciler) status(ctx context.Context, s *api.Swytch, ready bool, reason, message string) error {
	before := s.DeepCopy()
	s.Status.ObservedGeneration = s.Generation
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: "Ready", Status: status, Reason: reason, Message: message, ObservedGeneration: s.Generation})
	if reflect.DeepEqual(before.Status, s.Status) {
		return nil
	}
	return r.Status().Patch(ctx, s, client.MergeFrom(before))
}

// Adopt only objects already controlled by this profile. A name collision must
// never overwrite an unrelated user workload or Service.
func owned(obj client.Object, s *api.Swytch) error {
	if obj.GetResourceVersion() != "" && !metav1.IsControlledBy(obj, s) {
		return fmt.Errorf("%s is not owned by this Swytch profile", obj.GetName())
	}
	return nil
}

func (r *Reconciler) resources(ctx context.Context, s *api.Swytch) error {
	if s.Spec.Discovery.DNS != nil {
		if err := r.service(ctx, s, true); err != nil {
			return err
		}
	} else {
		peer := &corev1.Service{}
		err := r.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: workload.PeerName(s)}, peer)
		if err == nil && metav1.IsControlledBy(peer, s) {
			if err := r.Delete(ctx, peer); err != nil {
				return client.IgnoreNotFound(err)
			}
		} else if err != nil && client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	if s.Spec.Deployment != "server" {
		return nil
	}
	if err := r.service(ctx, s, false); err != nil {
		return err
	}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: workload.Name(s), Namespace: s.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, d, func() error {
		if err := owned(d, s); err != nil {
			return err
		}
		if err := controllerutil.SetControllerReference(s, d, r.Scheme); err != nil {
			return err
		}
		n := workload.Replicas(s)
		d.Spec.Replicas = &n
		d.Spec.Selector = &metav1.LabelSelector{MatchLabels: workload.Labels(s)}
		d.Spec.Template.Labels = workload.Labels(s)
		desired := corev1.PodSpec{Containers: []corev1.Container{workload.Container(s, r.DefaultImage)}, ImagePullSecrets: r.ImagePullSecrets}
		raw, err := json.Marshal(desired)
		if err != nil {
			return err
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(raw))
		// Admission defaults omitted requests from limits. Do not continuously
		// overwrite those defaults, but still apply explicit profile changes.
		if d.Spec.Template.Annotations["swytch.getswytch.com/config"] != hash || !equality.Semantic.DeepDerivative(desired, d.Spec.Template.Spec) {
			d.Spec.Template.Spec = desired
			if d.Spec.Template.Annotations == nil {
				d.Spec.Template.Annotations = map[string]string{}
			}
			d.Spec.Template.Annotations["swytch.getswytch.com/config"] = hash
		}
		return nil
	})
	return err
}

func (r *Reconciler) service(ctx context.Context, s *api.Swytch, peer bool) error {
	name := workload.Name(s)
	if peer {
		name = workload.PeerName(s)
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := owned(svc, s); err != nil {
			return err
		}
		if err := controllerutil.SetControllerReference(s, svc, r.Scheme); err != nil {
			return err
		}
		svc.Spec.Selector = workload.Labels(s)
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		if peer {
			svc.Spec.ClusterIP = corev1.ClusterIPNone
			svc.Spec.PublishNotReadyAddresses = true
			svc.Spec.Ports = []corev1.ServicePort{{Name: "peer", Port: 7379, TargetPort: intstr.FromInt32(7379), Protocol: corev1.ProtocolUDP}}
		} else {
			svc.Spec.Ports = []corev1.ServicePort{{Name: "redis", Port: 6379, TargetPort: intstr.FromInt32(6379), Protocol: corev1.ProtocolTCP}}
		}
		return nil
	})
	return err
}
