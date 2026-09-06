package cloud

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/mintyly/gke-drain/internal/drain"
)

func (c *Client) Snapshot(ctx context.Context) (drain.Snapshot, error) {
	s := drain.Snapshot{Resource: c.resource}
	var cluster clusterJSON
	if err := c.cloud(ctx, http.MethodGet, c.gkeBase+"/"+c.resource.ClusterPath(), nil, &cluster, true); err != nil {
		return s, err
	}
	if cluster.ID == "" || cluster.ID != c.clusterID {
		return s, errors.New("selected cluster identity changed; stop and create a new client/plan")
	}
	if cluster.Endpoint != c.endpoint || cluster.MasterAuth.ClusterCACertificate != c.ca {
		return s, errors.New("selected cluster endpoint or CA changed; stop and reconnect before planning")
	}
	var pool poolJSON
	if err := c.cloud(ctx, http.MethodGet, c.gkeBase+"/"+c.resource.PoolPath(), nil, &pool, true); err != nil {
		return s, err
	}
	if pool.Name != c.resource.Pool {
		return s, errors.New("GKE returned a different node pool")
	}
	s.ClusterID, s.ClusterStatus, s.PoolStatus = cluster.ID, cluster.Status, pool.Status
	s.Autopilot = cluster.Autopilot.Enabled || pool.AutopilotConfig.Enabled
	s.Autoscaling = pool.Autoscaling.Enabled || pool.Autoscaling.Autoprovisioned
	if s.Autopilot {
		return s, errors.New("Autopilot clusters and Autopilot-managed node pools are not supported")
	}
	s.Locations = pool.Locations
	if len(s.Locations) == 0 {
		s.Locations = cluster.Locations
	}
	busy, err := c.activeOperations(ctx)
	if err != nil {
		return s, err
	}
	s.Busy = busy
	for _, raw := range pool.InstanceGroupURLs {
		group, err := c.group(ctx, raw)
		if err != nil {
			return s, err
		}
		s.Groups = append(s.Groups, group)
	}
	s.Nodes, err = c.nodes(ctx)
	if err != nil {
		return s, err
	}
	// Read pods only for pool nodes. This avoids requiring unrelated namespaces'
	// workload-controller read permissions while still finding every local pod.
	for _, node := range s.Nodes {
		pods, err := c.PodsOnNode(ctx, node.Name)
		if err != nil {
			return s, err
		}
		s.Pods = append(s.Pods, pods...)
	}
	refs := map[string]drain.Workload{}
	for _, pod := range s.Pods {
		if pod.Ignored() {
			continue
		}
		if _, ok := workloadResource(pod.OwnerKind); !ok {
			continue
		}
		key := pod.Namespace + "/" + pod.OwnerKind + "/" + pod.OwnerName
		ref := drain.Workload{Namespace: pod.Namespace, Kind: pod.OwnerKind, Name: pod.OwnerName, UID: pod.OwnerUID}
		if prior, ok := refs[key]; ok && prior.UID != ref.UID {
			return s, fmt.Errorf("pods reference inconsistent controller UIDs for %s", key)
		}
		refs[key] = ref
	}
	ordered := make([]drain.Workload, 0, len(refs))
	for _, ref := range refs {
		ordered = append(ordered, ref)
	}
	sort.Slice(ordered, func(i, j int) bool { return workloadKey(ordered[i]) < workloadKey(ordered[j]) })
	s.Workloads, err = c.Workloads(ctx, ordered)
	return s, err
}

func (c *Client) activeOperations(ctx context.Context) (bool, error) {
	page := ""
	seen := map[string]bool{}
	for {
		values := url.Values{}
		if page != "" {
			values.Set("pageToken", page)
		}
		var result struct {
			Operations    []operationJSON `json:"operations"`
			NextPageToken string          `json:"nextPageToken"`
			MissingZones  []string        `json:"missingZones"`
		}
		path := c.gkeBase + "/projects/" + c.resource.Project + "/locations/" + c.resource.Location + "/operations"
		if encoded := values.Encode(); encoded != "" {
			path += "?" + encoded
		}
		if err := c.cloud(ctx, http.MethodGet, path, nil, &result, true); err != nil {
			return false, err
		}
		if len(result.MissingZones) > 0 {
			return false, errors.New("GKE operation listing is incomplete; cannot establish a stable cluster")
		}
		for _, op := range result.Operations {
			if op.Status == "DONE" {
				continue
			}
			// A missing/unknown target cannot establish that the operation is safe
			// to ignore; conservative blocking is preferable to racing an upgrade.
			if op.TargetLink == "" {
				return true, nil
			}
			project, location, cluster, _, err := parseGKETarget(op.TargetLink)
			if err != nil || project != c.resource.Project || location != c.resource.Location {
				return true, nil
			}
			if cluster == c.resource.Cluster {
				return true, nil
			}
		}
		if result.NextPageToken == "" {
			return false, nil
		}
		if seen[result.NextPageToken] {
			return false, errors.New("GKE operation listing repeated a pagination token")
		}
		seen[result.NextPageToken] = true
		page = result.NextPageToken
	}
}

func (c *Client) group(ctx context.Context, raw string) (drain.Group, error) {
	path, zone, _, err := computePath(raw, c.resource.Project, "instanceGroupManagers")
	if err != nil {
		return drain.Group{}, err
	}
	g := drain.Group{URL: raw, Zone: zone}
	var info struct {
		TargetSize int `json:"targetSize"`
		Status     struct {
			IsStable bool `json:"isStable"`
		} `json:"status"`
	}
	if err := c.cloud(ctx, http.MethodGet, c.computeBase+path, nil, &info, true); err != nil {
		return g, err
	}
	g.TargetSize, g.Stable = info.TargetSize, info.Status.IsStable
	page := ""
	seen := map[string]bool{}
	for {
		values := url.Values{"maxResults": {"500"}}
		if page != "" {
			values.Set("pageToken", page)
		}
		var result struct {
			ManagedInstances []struct {
				Instance      string `json:"instance"`
				CurrentAction string `json:"currentAction"`
			} `json:"managedInstances"`
			NextPageToken string `json:"nextPageToken"`
		}
		// listManagedInstances is a read-only Compute POST, not a mutation.
		if err := c.cloud(ctx, http.MethodPost, c.computeBase+path+"/listManagedInstances?"+values.Encode(), struct{}{}, &result, true); err != nil {
			return g, err
		}
		for _, instance := range result.ManagedInstances {
			_, izone, name, err := computePath(instance.Instance, c.resource.Project, "instances")
			if err != nil {
				return g, err
			}
			if izone != zone {
				return g, errors.New("MIG returned an instance in a different zone")
			}
			if instance.CurrentAction != "NONE" {
				g.Stable = false
			}
			g.Instances = append(g.Instances, "gce://"+c.resource.Project+"/"+zone+"/"+name)
		}
		if result.NextPageToken == "" {
			break
		}
		if seen[result.NextPageToken] {
			return g, errors.New("MIG listing repeated a pagination token")
		}
		seen[result.NextPageToken] = true
		page = result.NextPageToken
	}
	sort.Strings(g.Instances)
	return g, nil
}

func (c *Client) Resize(ctx context.Context, count int) (string, error) {
	if count < 0 {
		return "", errors.New("node count cannot be negative")
	}
	var op operationJSON
	err := c.cloud(ctx, http.MethodPost, c.gkeBase+"/"+c.resource.PoolPath()+":setSize", struct {
		NodeCount int `json:"nodeCount"`
	}{count}, &op, false)
	if err != nil {
		return "", fmt.Errorf("GKE resize submission failed; outcome may be unknown, inspect GKE operations before retrying: %w", err)
	}
	if err := c.checkOperationScope(op); err != nil {
		return "", fmt.Errorf("GKE resize response could not be verified; inspect operations before retrying: %w", err)
	}
	name, err := c.operationName(op.Name)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (c *Client) operationName(name string) (string, error) {
	prefix := "projects/" + c.resource.Project + "/locations/" + c.resource.Location + "/operations/"
	if strings.HasPrefix(name, prefix) {
		name = strings.TrimPrefix(name, prefix)
	}
	if !identifier.MatchString(name) {
		return "", errors.New("GKE returned an invalid or differently scoped operation name")
	}
	return prefix + name, nil
}

func (c *Client) checkOperationScope(op operationJSON) error {
	if _, err := c.operationName(op.Name); err != nil {
		return err
	}
	if op.Location != "" && op.Location != c.resource.Location {
		return errors.New("GKE operation location differs from the selected cluster")
	}
	if op.Zone != "" && op.Zone != c.resource.Location {
		return errors.New("GKE operation zone differs from the selected cluster")
	}
	if op.TargetLink != "" {
		project, location, cluster, pool, err := parseGKETarget(op.TargetLink)
		if err != nil || project != c.resource.Project || location != c.resource.Location || cluster != c.resource.Cluster || (pool != "" && pool != c.resource.Pool) {
			return errors.New("GKE operation target differs from the selected node pool")
		}
	}
	return nil
}

// GKE can return legacy /zones/ links even for calls through /locations/. A
// cluster-level operation target also belongs to this request's scope.
func parseGKETarget(raw string) (project, location, cluster, pool string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host != "container.googleapis.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
		return "", "", "", "", errors.New("unexpected GKE operation target URL")
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if (len(parts) != 7 && len(parts) != 9) || (parts[0] != "v1" && parts[0] != "v1beta1") || parts[1] != "projects" || (parts[3] != "locations" && parts[3] != "zones") || parts[5] != "clusters" || !identifier.MatchString(parts[2]) || !identifier.MatchString(parts[4]) || !identifier.MatchString(parts[6]) {
		return "", "", "", "", errors.New("unrecognized GKE operation target path")
	}
	if len(parts) == 9 {
		if parts[7] != "nodePools" || !identifier.MatchString(parts[8]) {
			return "", "", "", "", errors.New("unrecognized GKE operation target pool")
		}
		pool = parts[8]
	}
	return parts[2], parts[4], parts[6], pool, nil
}
