package main

import (
	"flag"
	"os"
	"strings"

	api "github.com/swytchdb/swytch/operator/api/v1alpha1"
	"github.com/swytchdb/swytch/operator/internal/controller"
	"github.com/swytchdb/swytch/operator/internal/injector"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func main() {
	image := flag.String("swytch-image", "ghcr.io/swytchdb/swytch:1.4.1", "Default Swytch workload image")
	pullSecrets := flag.String("swytch-image-pull-secrets", "", "Comma-separated Secret names in workload namespaces")
	certDir := flag.String("cert-dir", "/certs", "Webhook certificate directory")
	leader := flag.Bool("leader-elect", true, "Enable leader election")
	options := zap.Options{}
	options.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options)))
	if err := run(*image, *pullSecrets, *certDir, *leader); err != nil {
		ctrl.Log.Error(err, "operator stopped")
		os.Exit(1)
	}
}

func run(image, secrets, certDir string, leader bool) error {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := api.AddToScheme(scheme); err != nil {
		return err
	}
	server := webhook.NewServer(webhook.Options{Port: 9443, CertDir: certDir})
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme, WebhookServer: server, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: ":8081", LeaderElection: leader, LeaderElectionID: "swytch-operator.swytch.getswytch.com"})
	if err != nil {
		return err
	}
	var refs []corev1.LocalObjectReference
	for _, name := range strings.Split(secrets, ",") {
		if name = strings.TrimSpace(name); name != "" {
			refs = append(refs, corev1.LocalObjectReference{Name: name})
		}
	}
	r := &controller.Reconciler{Client: mgr.GetClient(), Scheme: scheme, DefaultImage: image, ImagePullSecrets: refs}
	if err := r.SetupWithManager(mgr); err != nil {
		return err
	}
	mgr.GetWebhookServer().Register("/mutate-pods", &admission.Webhook{Handler: &injector.Handler{Reader: mgr.GetAPIReader(), DefaultImage: image, ImagePullSecrets: refs}})
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("webhook", server.StartedChecker()); err != nil {
		return err
	}
	return mgr.Start(ctrl.SetupSignalHandler())
}
