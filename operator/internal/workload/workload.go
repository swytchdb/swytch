package workload

import (
	"fmt"
	"strings"

	api "github.com/swytchdb/swytch/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	Annotation    = "swytch.getswytch.com/profile"
	Label         = "swytch.getswytch.com/profile"
	Injected      = "swytch.getswytch.com/injected"
	ContainerName = "swytch"
	SocketVolume  = "swytch-socket"
	SocketDir     = "/var/run/swytch"
)

func Validate(s *api.Swytch) error {
	if len(s.Name) > 48 || len(validation.IsDNS1123Label(s.Name)) != 0 {
		return fmt.Errorf("profile name must be a DNS label of at most 48 characters")
	}
	if s.Spec.Deployment != "sidecar" && s.Spec.Deployment != "server" {
		return fmt.Errorf("deployment must be sidecar or server")
	}
	if s.Spec.Memory.Sign() <= 0 {
		return fmt.Errorf("memory must be positive")
	}
	for k, q := range s.Spec.Requests {
		if k != corev1.ResourceMemory && k != corev1.ResourceCPU {
			return fmt.Errorf("only memory and cpu requests are supported")
		}
		if q.Sign() < 0 {
			return fmt.Errorf("requests must be nonnegative")
		}
		if k == corev1.ResourceMemory && q.Cmp(s.Spec.Memory) > 0 {
			return fmt.Errorf("memory request exceeds limit")
		}
		if k == corev1.ResourceCPU && s.Spec.Limits.CPU != nil && q.Cmp(*s.Spec.Limits.CPU) > 0 {
			return fmt.Errorf("cpu request exceeds limit")
		}
	}
	if s.Spec.Limits.CPU != nil && s.Spec.Limits.CPU.Sign() <= 0 {
		return fmt.Errorf("cpu limit must be positive")
	}
	if s.Spec.Deployment == "sidecar" && s.Spec.Server != nil {
		return fmt.Errorf("server settings require server deployment")
	}
	if s.Spec.Deployment == "server" && s.Spec.Sidecar != nil {
		return fmt.Errorf("sidecar settings require sidecar deployment")
	}
	if s.Spec.Server != nil && s.Spec.Server.Replicas != nil && *s.Spec.Server.Replicas < 0 {
		return fmt.Errorf("replicas must be nonnegative")
	}
	if s.Spec.Sidecar != nil && s.Spec.Sidecar.Transport != "" && s.Spec.Sidecar.Transport != "tcp" && s.Spec.Sidecar.Transport != "unix" {
		return fmt.Errorf("transport must be tcp or unix")
	}
	if (s.Spec.Discovery.DNS == nil) == (s.Spec.Discovery.Cloud == nil) {
		return fmt.Errorf("exactly one of discovery.dns or discovery.cloud is required")
	}
	ref := SecretRef(s)
	if len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || ref.Name == "" || len(validation.IsConfigMapKey(ref.Key)) != 0 || ref.Key == "" {
		return fmt.Errorf("a valid secret name and key are required")
	}
	return nil
}

func SecretRef(s *api.Swytch) api.SecretKeyRef {
	if s.Spec.Discovery.DNS != nil {
		return s.Spec.Discovery.DNS.SecretKeyRef
	}
	if s.Spec.Discovery.Cloud != nil {
		return s.Spec.Discovery.Cloud.SecretKeyRef
	}
	return api.SecretKeyRef{}
}

func Unix(s *api.Swytch) bool {
	return s.Spec.Deployment == "sidecar" && s.Spec.Sidecar != nil && s.Spec.Sidecar.Transport == "unix"
}
func Name(s *api.Swytch) string              { return "swytch-" + s.Name }
func PeerName(s *api.Swytch) string          { return Name(s) + "-peers" }
func Labels(s *api.Swytch) map[string]string { return map[string]string{Label: s.Name} }
func Replicas(s *api.Swytch) int32 {
	if s.Spec.Server != nil && s.Spec.Server.Replicas != nil {
		return *s.Spec.Server.Replicas
	}
	return 1
}

func Container(s *api.Swytch, defaultImage string) corev1.Container {
	image := s.Spec.Image
	if image == "" {
		image = defaultImage
	}
	bind := "127.0.0.1"
	if s.Spec.Deployment == "server" {
		bind = "0.0.0.0"
	}
	args := []string{"redis", "--bind=" + bind, "--port=6379", "--maxmemory=80%", "--cluster-port=7379", "--cluster-advertise=[$(POD_IP)]:7379", "--metrics-port=9090"}
	if Unix(s) {
		args[1] = "--bind="
		args = append(args, "--unixsocket="+SocketDir+"/swytch.sock", "--unixsocketperm=438") // 0666, parsed as a decimal int by the CLI.
	}
	ref := SecretRef(s)
	env := []corev1.EnvVar{
		{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
		{Name: "SWYTCH_SECRET", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name}, Key: ref.Key}}},
	}
	if s.Spec.Discovery.DNS != nil {
		args = append(args, "--join="+PeerName(s)+"."+s.Namespace+".svc", "--cluster-passphrase=$(SWYTCH_SECRET)")
	} else {
		args = append(args, "--cloud=$(SWYTCH_SECRET)")
	}
	ports := []corev1.ContainerPort{{Name: "swytch-peer", ContainerPort: 7379, Protocol: corev1.ProtocolUDP}, {Name: "swytch-metrics", ContainerPort: 9090}}
	if !Unix(s) {
		ports = append(ports, corev1.ContainerPort{Name: "swytch-redis", ContainerPort: 6379})
	}
	resources := corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: s.Spec.Memory.DeepCopy()}}
	if len(s.Spec.Requests) > 0 {
		resources.Requests = corev1.ResourceList{}
		for k, q := range s.Spec.Requests {
			resources.Requests[k] = q.DeepCopy()
		}
	}
	if s.Spec.Limits.CPU != nil {
		resources.Limits[corev1.ResourceCPU] = s.Spec.Limits.CPU.DeepCopy()
	}
	probe := func() *corev1.Probe {
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromInt32(9090)}}, PeriodSeconds: 5, TimeoutSeconds: 2, FailureThreshold: 3}
	}
	c := corev1.Container{Name: ContainerName, Image: image, ImagePullPolicy: corev1.PullIfNotPresent, Args: args, Env: env, Ports: ports, Resources: resources, ReadinessProbe: probe(), LivenessProbe: probe(), StartupProbe: probe()}
	c.StartupProbe.FailureThreshold = 60
	uid, gid := int64(65532), int64(65532)
	nonRoot, noEscalation, readOnly := true, false, true
	c.SecurityContext = &corev1.SecurityContext{
		RunAsUser: &uid, RunAsGroup: &gid, RunAsNonRoot: &nonRoot,
		AllowPrivilegeEscalation: &noEscalation, ReadOnlyRootFilesystem: &readOnly,
		Capabilities:   &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	if Unix(s) {
		c.VolumeMounts = []corev1.VolumeMount{{Name: SocketVolume, MountPath: SocketDir}}
	}
	return c
}

// Inject only mutates the supplied Pod. It does not own or restart the application.
func Inject(p *corev1.Pod, s *api.Swytch, image string) error {
	if err := Validate(s); err != nil {
		return err
	}
	if s.Spec.Deployment != "sidecar" {
		return fmt.Errorf("annotation must reference a sidecar profile")
	}
	if p.Spec.HostNetwork {
		return fmt.Errorf("Swytch sidecars do not support hostNetwork")
	}
	if p.Annotations[Injected] != "" {
		if p.Annotations[Injected] != string(s.UID) {
			return fmt.Errorf("conflicting injection marker")
		}
		for _, c := range p.Spec.InitContainers {
			if c.Name == ContainerName && c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways && p.Labels[Label] == s.Name {
				return nil
			}
		}
		return fmt.Errorf("injection marker exists without a native Swytch sidecar")
	}
	if v, ok := p.Labels[Label]; ok && v != s.Name {
		return fmt.Errorf("conflicting Swytch profile label")
	}
	c := Container(s, image)
	for _, existing := range append(append([]corev1.Container{}, p.Spec.Containers...), p.Spec.InitContainers...) {
		if existing.Name == ContainerName {
			return fmt.Errorf("container name %q is reserved", ContainerName)
		}
		for _, port := range existing.Ports {
			for _, reserved := range c.Ports {
				protocol := port.Protocol
				if protocol == "" {
					protocol = corev1.ProtocolTCP
				}
				rp := reserved.Protocol
				if rp == "" {
					rp = corev1.ProtocolTCP
				}
				if port.Name == reserved.Name || (port.ContainerPort == reserved.ContainerPort && protocol == rp) {
					return fmt.Errorf("container %s has a conflicting Swytch port", existing.Name)
				}
			}
		}
		if Unix(s) {
			for _, m := range existing.VolumeMounts {
				if m.Name == SocketVolume || m.MountPath == SocketDir || strings.HasPrefix(m.MountPath, SocketDir+"/") {
					return fmt.Errorf("container %s has a conflicting socket mount", existing.Name)
				}
			}
		}
	}
	if Unix(s) {
		for _, v := range p.Spec.Volumes {
			if v.Name == SocketVolume {
				return fmt.Errorf("volume %s is reserved", SocketVolume)
			}
		}
		p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: SocketVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		for i := range p.Spec.Containers {
			p.Spec.Containers[i].VolumeMounts = append(p.Spec.Containers[i].VolumeMounts, corev1.VolumeMount{Name: SocketVolume, MountPath: SocketDir})
		}
	}
	restart := corev1.ContainerRestartPolicyAlways
	c.RestartPolicy = &restart
	p.Spec.InitContainers = append([]corev1.Container{c}, p.Spec.InitContainers...)
	if p.Labels == nil {
		p.Labels = map[string]string{}
	}
	p.Labels[Label] = s.Name
	if p.Annotations == nil {
		p.Annotations = map[string]string{}
	}
	p.Annotations[Injected] = string(s.UID)
	return nil
}
