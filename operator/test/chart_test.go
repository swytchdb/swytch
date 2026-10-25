package test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestHelmRender(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm unavailable")
	}

	chart := os.Getenv("SWYTCH_HELM_CHART")
	if chart == "" {
		t.Fatal("set SWYTCH_HELM_CHART to the absolute chart path in a swytch-helm checkout")
	}

	cmd := exec.Command("helm", "template", "test", chart, "--namespace", "operators", "--kube-version", "1.33.0", "--include-crds", "--set", "swytch.imagePullSecrets[0]=registry", "--set", "nodeSelector.pool=infra")

	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}

	kinds := map[string]int{}

	for doc := range strings.SplitSeq(string(out), "\n---") {
		var obj map[string]any
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatal(err)
		}

		kind, _ := obj["kind"].(string)

		kinds[kind]++
		if kind == "Deployment" {
			spec := obj["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)

			container := spec["containers"].([]any)[0].(map[string]any)
			if _, ok := container["resources"].(map[string]any)["requests"]; ok {
				t.Fatal("operator chart defaults requests")
			}
		}
	}

	for _, kind := range []string{"CustomResourceDefinition", "Deployment", "Service", "Secret", "MutatingWebhookConfiguration", "ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "ServiceAccount"} {
		if kinds[kind] != 1 {
			t.Fatalf("expected one %s, got %d", kind, kinds[kind])
		}
	}

	if !strings.Contains(string(out), "--swytch-image-pull-secrets=registry") {
		t.Fatal("workload image pull secret missing")
	}
}
