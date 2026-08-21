package cloud

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/mintyly/gke-drain/internal/drain"
)

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
