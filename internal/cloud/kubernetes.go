package cloud

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"github.com/mintyly/gke-drain/internal/drain"
)

func (c *Client) nodes(ctx context.Context) ([]drain.Node, error) {
	var nodes []drain.Node
	page := ""
	seen := map[string]bool{}
	for {
		values := url.Values{"labelSelector": {"cloud.google.com/gke-nodepool=" + c.resource.Pool}, "limit": {"500"}}
		if page != "" {
			values.Set("continue", page)
		}
		var result struct {
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
			Items []nodeJSON `json:"items"`
		}
		if err := c.kube(ctx, http.MethodGet, "/api/v1/nodes?"+values.Encode(), "", nil, &result); err != nil {
			return nil, err
		}
		for _, raw := range result.Items {
			if raw.Metadata.Labels["cloud.google.com/gke-nodepool"] != c.resource.Pool {
				return nil, errors.New("Kubernetes returned a node outside the selected pool")
			}
			nodes = append(nodes, toNode(raw))
		}
		if result.Metadata.Continue == "" {
			break
		}
		if seen[result.Metadata.Continue] {
			return nil, errors.New("Kubernetes nodes repeated a pagination token")
		}
		seen[result.Metadata.Continue] = true
		page = result.Metadata.Continue
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return nodes, nil
}

func toNode(raw nodeJSON) drain.Node {
	n := drain.Node{Name: raw.Metadata.Name, UID: raw.Metadata.UID, ResourceVersion: raw.Metadata.ResourceVersion,
		Zone: raw.Metadata.Labels["topology.kubernetes.io/zone"], ProviderID: raw.Spec.ProviderID,
		MachineType: raw.Metadata.Labels["node.kubernetes.io/instance-type"], CreatedAt: raw.Metadata.CreationTimestamp,
		Unschedulable: raw.Spec.Unschedulable, Owner: raw.Metadata.Annotations[drain.OwnershipAnnotation]}
	for _, condition := range raw.Status.Conditions {
		if condition.Type == "Ready" {
			n.Ready = condition.Status == "True"
		}
	}
	if raw.Metadata.DeletionTimestamp != nil {
		n.Ready = false
	}
	return n
}

func (c *Client) PodsOnNode(ctx context.Context, node string) ([]drain.Pod, error) {
	if !validObjectName(node) {
		return nil, errors.New("invalid Kubernetes node name")
	}
	var pods []drain.Pod
	page := ""
	seen := map[string]bool{}
	for {
		values := url.Values{"fieldSelector": {"spec.nodeName=" + node}, "limit": {"500"}}
		if page != "" {
			values.Set("continue", page)
		}
		var result struct {
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
			Items []podJSON `json:"items"`
		}
		if err := c.kube(ctx, http.MethodGet, "/api/v1/pods?"+values.Encode(), "", nil, &result); err != nil {
			return nil, err
		}
		for _, raw := range result.Items {
			if raw.Spec.NodeName != node {
				return nil, errors.New("Kubernetes returned a pod outside the selected node")
			}
			pods = append(pods, toPod(raw))
		}
		if result.Metadata.Continue == "" {
			break
		}
		if seen[result.Metadata.Continue] {
			return nil, errors.New("Kubernetes pods repeated a pagination token")
		}
		seen[result.Metadata.Continue] = true
		page = result.Metadata.Continue
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Key() < pods[j].Key() })
	return pods, nil
}

func toPod(raw podJSON) drain.Pod {
	p := drain.Pod{Name: raw.Metadata.Name, Namespace: raw.Metadata.Namespace, UID: raw.Metadata.UID,
		Node: raw.Spec.NodeName, Completed: raw.Status.Phase == "Succeeded" || raw.Status.Phase == "Failed",
		Terminating: raw.Metadata.DeletionTimestamp != nil}
	_, p.Mirror = raw.Metadata.Annotations["kubernetes.io/config.mirror"]
	for _, owner := range raw.Metadata.OwnerReferences {
		if owner.Controller {
			p.OwnerKind, p.OwnerName, p.OwnerUID = owner.Kind, owner.Name, owner.UID
			break
		}
	}
	for _, volume := range raw.Spec.Volumes {
		p.EmptyDir = p.EmptyDir || volume.EmptyDir != nil
		p.HostPath = p.HostPath || volume.HostPath != nil
		p.PVC = p.PVC || volume.PersistentVolumeClaim != nil || volume.Ephemeral != nil
	}
	return p
}

// Kubernetes object names are used as URL segments; allow DNS-subdomain names
// but never slash, percent escapes, dot segments, or query delimiters.
func validObjectName(name string) bool {
	if name == "" || len(name) > 253 || name == "." || name == ".." {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}

type patchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

func (c *Client) getNode(ctx context.Context, name string) (nodeJSON, error) {
	var node nodeJSON
	if !validObjectName(name) {
		return node, errors.New("invalid Kubernetes node name")
	}
	err := c.kube(ctx, http.MethodGet, "/api/v1/nodes/"+name, "", nil, &node)
	if err == nil && node.Metadata.Name != name {
		return node, errors.New("Kubernetes returned a different node")
	}
	return node, err
}

func (c *Client) Cordon(ctx context.Context, node drain.Node, runID string) error {
	if runID == "" {
		return errors.New("cordon requires a nonempty run ID")
	}
	current, err := c.getNode(ctx, node.Name)
	if err != nil {
		return err
	}
	if current.Metadata.UID == "" || current.Metadata.UID != node.UID || current.Metadata.ResourceVersion == "" || current.Spec.ProviderID != node.ProviderID || current.Metadata.Labels["topology.kubernetes.io/zone"] != node.Zone {
		return fmt.Errorf("node %s identity changed since planning; refusing stale cordon", node.Name)
	}
	if !toNode(current).Ready {
		return fmt.Errorf("node %s is not Ready or is being deleted", node.Name)
	}
	if current.Metadata.Labels["cloud.google.com/gke-nodepool"] != c.resource.Pool {
		return errors.New("cordon node is no longer in the selected pool")
	}
	if current.Spec.Unschedulable || current.Metadata.Annotations[drain.OwnershipAnnotation] != "" {
		return fmt.Errorf("node %s is already cordoned or owned by another operation", node.Name)
	}
	annotations := current.Metadata.Annotations
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[drain.OwnershipAnnotation] = runID
	patch := []patchOperation{
		{Op: "test", Path: "/metadata/uid", Value: current.Metadata.UID},
		{Op: "test", Path: "/metadata/resourceVersion", Value: current.Metadata.ResourceVersion},
		{Op: "add", Path: "/spec/unschedulable", Value: true},
		{Op: "add", Path: "/metadata/annotations", Value: annotations},
	}
	return c.kube(ctx, http.MethodPatch, "/api/v1/nodes/"+node.Name, "application/json-patch+json", patch, nil)
}

func workloadResource(kind string) (string, bool) {
	switch kind {
	case "ReplicaSet":
		return "/apis/apps/v1/", true
	case "StatefulSet":
		return "/apis/apps/v1/", true
	case "ReplicationController":
		return "/api/v1/", true
	}
	return "", false
}

func workloadKey(w drain.Workload) string { return w.Namespace + "/" + w.Kind + "/" + w.Name }

func (c *Client) Workloads(ctx context.Context, refs []drain.Workload) ([]drain.Workload, error) {
	result := make([]drain.Workload, 0, len(refs))
	for _, ref := range refs {
		base, ok := workloadResource(ref.Kind)
		if !ok {
			return nil, fmt.Errorf("unsupported workload kind %q", ref.Kind)
		}
		if !validObjectName(ref.Namespace) || !validObjectName(ref.Name) || ref.UID == "" {
			return nil, errors.New("workload reference requires namespace, name, and UID")
		}
		resource := map[string]string{"ReplicaSet": "replicasets", "StatefulSet": "statefulsets", "ReplicationController": "replicationcontrollers"}[ref.Kind]
		var raw workloadJSON
		if err := c.kube(ctx, http.MethodGet, base+"namespaces/"+ref.Namespace+"/"+resource+"/"+ref.Name, "", nil, &raw); err != nil {
			return nil, err
		}
		if raw.Metadata.UID != ref.UID || raw.Metadata.Name != ref.Name || raw.Metadata.Namespace != ref.Namespace {
			return nil, fmt.Errorf("workload %s identity changed", workloadKey(ref))
		}
		if raw.Metadata.DeletionTimestamp != nil {
			return nil, fmt.Errorf("workload %s is being deleted", workloadKey(ref))
		}
		desired := 1
		if raw.Spec.Replicas != nil {
			desired = *raw.Spec.Replicas
		}
		result = append(result, drain.Workload{Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name, UID: raw.Metadata.UID,
			Desired: desired, Ready: raw.Status.ReadyReplicas, Generation: raw.Metadata.Generation, ObservedGeneration: raw.Status.ObservedGeneration})
	}
	return result, nil
}
