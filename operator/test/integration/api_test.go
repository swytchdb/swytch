package integration

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	api "github.com/swytchdb/swytch/operator/api/v1alpha1"
	"github.com/swytchdb/swytch/operator/internal/controller"
	"github.com/swytchdb/swytch/operator/internal/injector"
	"github.com/swytchdb/swytch/operator/internal/workload"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestKubernetesAPI(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run disposable API integration tests")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := "/mutate-pods"
	none := admissionv1.SideEffectClassNone
	fail := admissionv1.Fail
	hooks := &admissionv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "swytch-test"}, Webhooks: []admissionv1.MutatingWebhook{{Name: "pods.swytch.getswytch.com", AdmissionReviewVersions: []string{"v1"}, SideEffects: &none, FailurePolicy: &fail, ClientConfig: admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Name: "unused", Namespace: "default", Path: &path}}, MatchConditions: []admissionv1.MatchCondition{{Name: "annotated", Expression: "has(object.metadata.annotations) && 'swytch.getswytch.com/profile' in object.metadata.annotations"}}, Rules: []admissionv1.RuleWithOperations{{Operations: []admissionv1.OperationType{admissionv1.Create}, Rule: admissionv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}}}}}}}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../charts/swytch-operator/crds"}, ErrorIfCRDPathMissing: true, WebhookInstallOptions: envtest.WebhookInstallOptions{MutatingWebhooks: []*admissionv1.MutatingWebhookConfiguration{hooks}}}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = api.AddToScheme(scheme)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	w := env.WebhookInstallOptions
	server := webhook.NewServer(webhook.Options{Host: w.LocalServingHost, Port: w.LocalServingPort, CertDir: w.LocalServingCertDir})
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, WebhookServer: server, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		t.Fatal(err)
	}
	r := &controller.Reconciler{Client: mgr.GetClient(), Scheme: scheme, DefaultImage: "swytch:test"}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	mgr.GetWebhookServer().Register(path, &admission.Webhook{Handler: &injector.Handler{Reader: mgr.GetAPIReader(), DefaultImage: "swytch:test"}})
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	wait(t, func() bool { return server.StartedChecker()(nil) == nil })
	for _, name := range []string{"apps", "other"} {
		if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	newProfile := func(name, mode string) *api.Swytch {
		return &api.Swytch{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "apps"}, Spec: api.SwytchSpec{Deployment: mode, Memory: resource.MustParse("512Mi"), Discovery: api.Discovery{DNS: &api.DiscoveryConfig{SecretKeyRef: api.SecretKeyRef{Name: "secret", Key: "key"}}}}}
	}
	t.Run("CRD-validation", func(t *testing.T) {
		for name, modify := range map[string]func(*api.Swytch){
			"both":    func(s *api.Swytch) { s.Spec.Discovery.Cloud = s.Spec.Discovery.DNS },
			"neither": func(s *api.Swytch) { s.Spec.Discovery.DNS = nil },
			"request": func(s *api.Swytch) {
				s.Spec.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
			},
			"zero":     func(s *api.Swytch) { s.Spec.Memory = resource.MustParse("0") },
			"mismatch": func(s *api.Swytch) { s.Spec.Server = &api.Server{} },
		} {
			s := newProfile(name, "sidecar")
			modify(s)
			if err := c.Create(ctx, s); err == nil {
				t.Fatalf("invalid %s profile accepted", name)
			}
		}
		s := newProfile("immutable", "sidecar")
		if err := c.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
		s.Spec.Deployment = "server"
		if err := c.Update(ctx, s); err == nil {
			t.Fatal("deployment mode changed")
		}
	})
	t.Run("admission", func(t *testing.T) {
		s := newProfile("sidecar", "sidecar")
		s.Spec.Sidecar = &api.Sidecar{Transport: "unix"}
		if err := c.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
		for _, ns := range []string{"apps", "other"} {
			p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: ns, Annotations: map[string]string{workload.Annotation: s.Name}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app:test"}}}}
			err := c.Create(ctx, p)
			if ns == "other" {
				if err == nil {
					t.Fatal("cross-namespace profile accepted")
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Spec.InitContainers) != 1 || len(p.Spec.Volumes) != 1 || p.Spec.InitContainers[0].RestartPolicy == nil {
				t.Fatal("sidecar missing")
			}
		}
	})
	t.Run("reconcile-defaults", func(t *testing.T) {
		s := newProfile("server", "server")
		if err := c.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
		d := &appsv1.Deployment{}
		key := types.NamespacedName{Name: workload.Name(s), Namespace: s.Namespace}
		wait(t, func() bool { return c.Get(ctx, key, d) == nil })
		version := d.ResourceVersion
		// Explicitly reconcile through the real API to catch defaulting loops.
		direct := &controller.Reconciler{Client: c, Scheme: scheme, DefaultImage: "swytch:test"}
		if _, err := direct.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: s.Name, Namespace: s.Namespace}}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, key, d); err != nil {
			t.Fatal(err)
		}
		if version != d.ResourceVersion {
			t.Fatal("defaulted Deployment updated unnecessarily")
		}
	})
	t.Run("helm-certificates", func(t *testing.T) {
		if _, err := exec.LookPath("helm"); err != nil {
			t.Skip("helm unavailable")
		}
		// Remove the test webhook before installing the chart's webhook.
		if err := c.Delete(ctx, hooks); err != nil {
			t.Fatal(err)
		}
		kubeconfig := filepath.Join(t.TempDir(), "config")
		kc := clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"test": {Server: cfg.Host, CertificateAuthorityData: cfg.CAData}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"test": {ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData}}, Contexts: map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}}, CurrentContext: "test"}
		if err := clientcmd.WriteToFile(kc, kubeconfig); err != nil {
			t.Fatal(err)
		}
		upgrade := func(revision string) {
			t.Helper()
			cmd := exec.Command("helm", "upgrade", "--install", "test", "../../charts/swytch-operator", "--namespace", "operator-system", "--create-namespace", "--kubeconfig", kubeconfig, "--set-string", "webhook.certificateRevision="+revision)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("helm: %v\n%s", err, out)
			}
		}
		get := func() *corev1.Secret {
			t.Helper()
			s := &corev1.Secret{}
			if err := c.Get(ctx, types.NamespacedName{Name: "test-swytch-operator-tls", Namespace: "operator-system"}, s); err != nil {
				t.Fatal(err)
			}
			return s
		}
		upgrade("1")
		first := get()
		upgrade("1")
		second := get()
		if string(first.Data["tls.crt"]) != string(second.Data["tls.crt"]) {
			t.Fatal("upgrade replaced certificate")
		}
		upgrade("2")
		third := get()
		if string(second.Data["tls.crt"]) == string(third.Data["tls.crt"]) {
			t.Fatal("rotation did not replace certificate")
		}
		if string(first.Data["ca.crt"]) != string(third.Data["ca.crt"]) {
			t.Fatal("rotation changed CA")
		}
		block, _ := pem.Decode(third.Data["tls.crt"])
		if block == nil {
			t.Fatal("invalid certificate")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(first.Data["ca.crt"])
		if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: "test-swytch-operator.operator-system.svc"}); err != nil {
			t.Fatal(err)
		}
	})
}

func wait(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}
