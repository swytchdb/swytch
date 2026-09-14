package workload

import (
	"reflect"
	"strings"
	"testing"

	api "github.com/swytchdb/swytch/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func profile() *api.Swytch {
	return &api.Swytch{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "apps", UID: "profile-uid"}, Spec: api.SwytchSpec{Deployment: "sidecar", Memory: resource.MustParse("512Mi"), Discovery: api.Discovery{DNS: &api.DiscoveryConfig{SecretKeyRef: api.SecretKeyRef{Name: "credentials", Key: "passphrase"}}}}}
}

func pod() *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app:v1"}}}}
}

func TestResourceBudget(t *testing.T) {
	s := profile()
	c := Container(s, "swytch:test")
	if c.Resources.Requests != nil {
		t.Fatal("requests must be omitted by default")
	}
	if c.Resources.Limits.Memory().Cmp(s.Spec.Memory) != 0 {
		t.Fatal("wrong memory limit")
	}
	s.Spec.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi"), corev1.ResourceCPU: resource.MustParse("100m")}
	q := resource.MustParse("500m")
	s.Spec.Limits.CPU = &q
	c = Container(s, "swytch:test")
	if !reflect.DeepEqual(c.Resources.Requests, s.Spec.Requests) {
		t.Fatal("explicit requests lost")
	}
	if c.Resources.Limits.Cpu().Cmp(q) != 0 {
		t.Fatal("CPU limit lost")
	}
}

func TestDiscoveryAndTransport(t *testing.T) {
	for _, mode := range []string{"dns", "cloud"} {
		for _, transport := range []string{"tcp", "unix"} {
			t.Run(mode+"/"+transport, func(t *testing.T) {
				s := profile()
				s.Spec.Sidecar = &api.Sidecar{Transport: transport}
				if mode == "cloud" {
					s.Spec.Discovery.Cloud = s.Spec.Discovery.DNS
					s.Spec.Discovery.DNS = nil
				}
				p := pod()
				if err := Inject(p, s, "swytch:test"); err != nil {
					t.Fatal(err)
				}
				c := p.Spec.InitContainers[0]
				args := strings.Join(c.Args, " ")
				if c.RestartPolicy == nil || *c.RestartPolicy != corev1.ContainerRestartPolicyAlways {
					t.Fatal("not a native sidecar")
				}
				if mode == "dns" && (!strings.Contains(args, "--join=swytch-cache-peers.apps.svc") || strings.Contains(args, "--cloud")) {
					t.Fatal(args)
				}
				if mode == "cloud" && (!strings.Contains(args, "--cloud=$(SWYTCH_SECRET)") || strings.Contains(args, "--join") || strings.Contains(args, "--cluster-passphrase")) {
					t.Fatal(args)
				}
				if c.Env[1].ValueFrom.SecretKeyRef.Name != "credentials" || c.Env[1].Value != "" {
					t.Fatal("not a secret reference")
				}
				if transport == "unix" {
					if len(p.Spec.Volumes) != 1 || len(p.Spec.Containers[0].VolumeMounts) != 1 || !strings.Contains(args, "--unixsocket=") {
						t.Fatal("socket not shared")
					}
					for _, a := range c.Args {
						if strings.HasPrefix(a, "--bind=") && a != "--bind=" {
							t.Fatal("TCP enabled for Unix transport")
						}
					}
				} else if len(p.Spec.Volumes) != 0 || !strings.Contains(args, "--bind=127.0.0.1") {
					t.Fatal("incorrect TCP transport")
				}
				before := p.DeepCopy()
				if err := Inject(p, s, "swytch:test"); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, p) {
					t.Fatal("reinjection changed Pod")
				}
			})
		}
	}
}

func TestRejectConflicts(t *testing.T) {
	for name, mutate := range map[string]func(*corev1.Pod){
		"container": func(p *corev1.Pod) { p.Spec.Containers[0].Name = ContainerName },
		"port":      func(p *corev1.Pod) { p.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 6379}} },
		"port-name": func(p *corev1.Pod) {
			p.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "swytch-peer", ContainerPort: 9999}}
		},
		"host-network": func(p *corev1.Pod) { p.Spec.HostNetwork = true },
		"marker":       func(p *corev1.Pod) { p.Annotations = map[string]string{Injected: "profile-uid"} },
		"label":        func(p *corev1.Pod) { p.Labels = map[string]string{Label: "other"} },
	} {
		t.Run(name, func(t *testing.T) {
			p := pod()
			mutate(p)
			if Inject(p, profile(), "image") == nil {
				t.Fatal("conflict accepted")
			}
		})
	}
	s := profile()
	s.Spec.Sidecar = &api.Sidecar{Transport: "unix"}
	p := pod()
	p.Spec.Volumes = []corev1.Volume{{Name: SocketVolume}}
	if Inject(p, s, "image") == nil {
		t.Fatal("volume collision accepted")
	}
	p = pod()
	p.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "other", MountPath: SocketDir}}
	if Inject(p, s, "image") == nil {
		t.Fatal("mount collision accepted")
	}
}

func TestValidate(t *testing.T) {
	for name, mutate := range map[string]func(*api.Swytch){
		"both-discovery": func(s *api.Swytch) { s.Spec.Discovery.Cloud = s.Spec.Discovery.DNS },
		"no-discovery":   func(s *api.Swytch) { s.Spec.Discovery.DNS = nil },
		"memory":         func(s *api.Swytch) { s.Spec.Memory = resource.MustParse("0") },
		"request": func(s *api.Swytch) {
			s.Spec.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
		},
		"cpu": func(s *api.Swytch) {
			q := resource.MustParse("1")
			s.Spec.Limits.CPU = &q
			s.Spec.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
		},
		"server-settings": func(s *api.Swytch) { s.Spec.Server = &api.Server{} },
		"transport":       func(s *api.Swytch) { s.Spec.Sidecar = &api.Sidecar{Transport: "udp"} },
		"secret":          func(s *api.Swytch) { s.Spec.Discovery.DNS.SecretKeyRef.Name = "" },
	} {
		t.Run(name, func(t *testing.T) {
			s := profile()
			mutate(s)
			if Validate(s) == nil {
				t.Fatal("invalid profile accepted")
			}
		})
	}
}
