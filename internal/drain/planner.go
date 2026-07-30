package drain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const planVersion = "v1"

var resourceSegment = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// ValidateOptions checks values before either discovery or mutation is attempted.
func ValidateOptions(o Options) error {
	if o.TargetPerZone < 1 {
		return fmt.Errorf("target-per-zone must be at least 1; deleting a node pool is outside this tool's scope")
	}
	if o.BatchPerZone < 1 {
		return fmt.Errorf("batch-per-zone must be at least 1")
	}
	if o.DrainTimeout <= 0 || o.OperationTimeout <= 0 || o.PollInterval <= 0 {
		return fmt.Errorf("drain timeout, operation timeout, and poll interval must all be positive")
	}
	return nil
}

func validateResource(r Resource) error {
	for _, part := range []struct{ label, value string }{
		{"project", r.Project}, {"location", r.Location}, {"cluster", r.Cluster}, {"node pool", r.Pool},
	} {
		if !resourceSegment.MatchString(part.value) || part.value == "." || part.value == ".." {
			return fmt.Errorf("invalid %s resource identifier %q", part.label, part.value)
		}
	}
	return nil
}

type topology struct {
	zones   []string
	current int
	nodes   map[string]Node
}

// validateTopology joins Kubernetes nodes to Compute's read-only managed-instance
// inventory. A node label alone is not sufficient evidence of pool membership.
func validateTopology(s Snapshot) (topology, error) {
	t := topology{nodes: make(map[string]Node)}
	if err := validateResource(s.Resource); err != nil {
		return t, err
	}
	if s.ClusterID == "" {
		return t, fmt.Errorf("cluster identity is missing")
	}
	if s.Autopilot {
		return t, fmt.Errorf("only GKE Standard node pools are supported; this cluster uses Autopilot")
	}
	if s.ClusterStatus != "RUNNING" || s.PoolStatus != "RUNNING" || s.Busy {
		return t, fmt.Errorf("cluster and node pool must be RUNNING with no active operation")
	}
	if s.Autoscaling {
		return t, fmt.Errorf("node-pool autoscaling is enabled; disable it explicitly before planning a manual reduction")
	}
	if len(s.Locations) == 0 || len(s.Groups) != len(s.Locations) {
		return t, fmt.Errorf("expected exactly one managed instance group for every node-pool zone")
	}
	zoneSet := make(map[string]bool)
	for _, zone := range s.Locations {
		if !resourceSegment.MatchString(zone) || zoneSet[zone] {
			return t, fmt.Errorf("invalid or duplicate node-pool zone %q", zone)
		}
		zoneSet[zone] = true
		t.zones = append(t.zones, zone)
	}
	sort.Strings(t.zones)
	groups := make(map[string]Group)
	groupURLs := make(map[string]bool)
	members := make(map[string]string)
	for _, g := range s.Groups {
		if !zoneSet[g.Zone] || g.URL == "" || groupURLs[g.URL] {
			return t, fmt.Errorf("managed instance group has an unexpected zone, empty URL, or duplicate identity")
		}
		if _, exists := groups[g.Zone]; exists {
			return t, fmt.Errorf("more than one managed instance group found in zone %s", g.Zone)
		}
		if !g.Stable || g.TargetSize < 1 || len(g.Instances) != g.TargetSize {
			return t, fmt.Errorf("managed instance group in %s is unstable or its observed membership differs from its positive target", g.Zone)
		}
		if t.current == 0 {
			t.current = g.TargetSize
		} else if t.current != g.TargetSize {
			return t, fmt.Errorf("node-pool zones have unequal sizes; normalize them before using a per-zone resize")
		}
		for _, providerID := range g.Instances {
			if !validProviderID(providerID, s.Resource.Project, g.Zone) {
				return t, fmt.Errorf("managed instance %q does not match project %s and zone %s", providerID, s.Resource.Project, g.Zone)
			}
			if _, exists := members[providerID]; exists {
				return t, fmt.Errorf("duplicate managed instance %s", providerID)
			}
			members[providerID] = g.Zone
		}
		groups[g.Zone] = g
		groupURLs[g.URL] = true
	}
	if len(s.Nodes) != len(members) {
		return t, fmt.Errorf("Kubernetes node count %d does not match managed instance membership %d", len(s.Nodes), len(members))
	}
	seenIDs := make(map[string]bool)
	seenUIDs := make(map[string]bool)
	for _, n := range s.Nodes {
		if n.Name == "" || n.UID == "" || n.ResourceVersion == "" || t.nodes[n.Name].Name != "" || seenUIDs[n.UID] || seenIDs[n.ProviderID] {
			return t, fmt.Errorf("node has missing or duplicate name, UID, resource version, or provider ID: %q", n.Name)
		}
		if zone, exists := members[n.ProviderID]; !exists || zone != n.Zone {
			return t, fmt.Errorf("node %s is not an exact member of its pool's managed instance group", n.Name)
		}
		if !n.Ready || n.Unschedulable || n.Owner != "" {
			return t, fmt.Errorf("node %s must be Ready, schedulable, and free of another drain ownership annotation", n.Name)
		}
		t.nodes[n.Name] = n
		seenUIDs[n.UID], seenIDs[n.ProviderID] = true, true
	}
	return t, nil
}

func validProviderID(id, project, zone string) bool {
	if !strings.HasPrefix(id, "gce://") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(id, "gce://"), "/")
	return len(parts) == 3 && parts[0] == project && parts[1] == zone && resourceSegment.MatchString(parts[2])
}

func validatePod(p Pod, opts Options) error {
	if p.Ignored() {
		return nil
	}
	if p.Namespace == "" || p.Name == "" || p.UID == "" {
		return fmt.Errorf("pod on node %s has incomplete identity", p.Node)
	}
	switch p.OwnerKind {
	case "ReplicaSet", "ReplicationController", "StatefulSet":
	default:
		return fmt.Errorf("pod %s has unsupported or missing controller kind %q", p.Key(), p.OwnerKind)
	}
	if p.OwnerUID == "" || p.OwnerName == "" {
		return fmt.Errorf("pod %s has incomplete controller identity", p.Key())
	}
	if p.HostPath {
		return fmt.Errorf("pod %s uses hostPath; this tool cannot safely move node-local data", p.Key())
	}
	if p.EmptyDir && !opts.AllowEmptyDir {
		return fmt.Errorf("pod %s uses emptyDir; explicit --allow-emptydir acknowledgement is required", p.Key())
	}
	if p.PVC && !opts.AllowPVC {
		return fmt.Errorf("pod %s uses a persistent volume claim; explicit allow-persistent-volumes acknowledgement is required", p.Key())
	}
	return nil
}

func workloadIdentity(w Workload) string { return w.Namespace + "/" + w.Kind + "/" + w.Name }

func requiredWorkloads(s Snapshot, opts Options, nodes map[string]Node) ([]Workload, map[string]int, error) {
	byUID := make(map[string]Workload)
	for _, w := range s.Workloads {
		if w.UID == "" {
			return nil, nil, fmt.Errorf("workload %s has no UID", workloadIdentity(w))
		}
		if _, exists := byUID[w.UID]; exists {
			return nil, nil, fmt.Errorf("duplicate workload UID %s", w.UID)
		}
		byUID[w.UID] = w
	}
	required := make(map[string]Workload)
	counts := make(map[string]int)
	seenPods, seenUIDs := make(map[string]bool), make(map[string]bool)
	for _, p := range s.Pods {
		if _, inPool := nodes[p.Node]; !inPool || p.Ignored() {
			continue
		}
		if err := validatePod(p, opts); err != nil {
			return nil, nil, err
		}
		if p.Terminating {
			return nil, nil, fmt.Errorf("pod %s is already terminating; wait for workload churn to settle before planning", p.Key())
		}
		if seenPods[p.Key()] || seenUIDs[p.UID] {
			return nil, nil, fmt.Errorf("duplicate pod identity %s", p.Key())
		}
		seenPods[p.Key()], seenUIDs[p.UID] = true, true
		w, exists := byUID[p.OwnerUID]
		if !exists || w.Namespace != p.Namespace || w.Kind != p.OwnerKind || w.Name != p.OwnerName {
			return nil, nil, fmt.Errorf("pod %s has no matching controller with UID %s", p.Key(), p.OwnerUID)
		}
		if w.Desired < 0 || w.Ready < w.Desired || w.ObservedGeneration < w.Generation {
			return nil, nil, fmt.Errorf("workload %s is not healthy at its observed generation", workloadIdentity(w))
		}
		required[w.UID] = w
		counts[p.Node]++
	}
	refs := make([]Workload, 0, len(required))
	for _, w := range required {
		refs = append(refs, w)
	}
	sort.Slice(refs, func(i, j int) bool { return workloadIdentity(refs[i]) < workloadIdentity(refs[j]) })
	return refs, counts, nil
}

// BuildPlan is pure: it validates a snapshot and returns preferred drain
// candidates. GKE still chooses the actual nodes removed by setSize.
func BuildPlan(s Snapshot, opts Options) (Plan, error) {
	if err := ValidateOptions(opts); err != nil {
		return Plan{}, err
	}
	t, err := validateTopology(s)
	if err != nil {
		return Plan{}, err
	}
	if opts.TargetPerZone > t.current {
		return Plan{}, fmt.Errorf("target-per-zone %d exceeds current size %d; this tool only reduces node pools", opts.TargetPerZone, t.current)
	}
	refs, counts, err := requiredWorkloads(s, opts, t.nodes)
	if err != nil {
		return Plan{}, err
	}
	p := Plan{
		SchemaVersion: planVersion, Resource: s.Resource, ClusterID: s.ClusterID,
		CurrentPerZone: t.current, TargetPerZone: opts.TargetPerZone,
		Zones: t.zones, TotalBefore: t.current * len(t.zones), TotalAfter: opts.TargetPerZone * len(t.zones),
		Batches: []Batch{}, Warnings: []string{
			"GKE setSize chooses which nodes to remove; preferred drain candidates are not a victim-selection guarantee.",
			"A blocked Kubernetes eviction prevents resize submission. Once submitted, GKE can honor PodDisruptionBudgets and graceful termination for only up to one hour before completing the resize.",
			"Batch size and target size are per zone: each resize applies across every node-pool zone.",
		},
	}
	byZone := make(map[string][]Node)
	for _, n := range s.Nodes {
		byZone[n.Zone] = append(byZone[n.Zone], n)
	}
	for _, zone := range t.zones {
		sort.Slice(byZone[zone], func(i, j int) bool {
			a, b := byZone[zone][i], byZone[zone][j]
			ap, bp := opts.PreferMachineType != "" && a.MachineType == opts.PreferMachineType, opts.PreferMachineType != "" && b.MachineType == opts.PreferMachineType
			if ap != bp {
				return ap
			}
			if counts[a.Name] != counts[b.Name] {
				return counts[a.Name] < counts[b.Name]
			}
			if !a.CreatedAt.Equal(b.CreatedAt) {
				return a.CreatedAt.Before(b.CreatedAt)
			}
			return a.Name < b.Name
		})
	}
	for before, offset := t.current, 0; before > opts.TargetPerZone; {
		count := opts.BatchPerZone
		if remaining := before - opts.TargetPerZone; count > remaining {
			count = remaining
		}
		batch := Batch{Number: len(p.Batches) + 1, BeforePerZone: before, AfterPerZone: before - count, Candidates: []Node{}}
		for _, zone := range t.zones {
			batch.Candidates = append(batch.Candidates, byZone[zone][offset:offset+count]...)
		}
		p.Batches = append(p.Batches, batch)
		before, offset = before-count, offset+count
	}
	p.Fingerprint, err = fingerprint(s, opts, refs, t.nodes)
	return p, err
}

func fingerprint(s Snapshot, opts Options, refs []Workload, nodes map[string]Node) (string, error) {
	// Resource versions and readiness counts change during harmless observation.
	// Identities, ownership, desired state, and plan-affecting options do not.
	stable := s
	stable.Locations = append([]string(nil), s.Locations...)
	sort.Strings(stable.Locations)
	stable.Groups = append([]Group(nil), s.Groups...)
	for i := range stable.Groups {
		stable.Groups[i].Instances = append([]string(nil), stable.Groups[i].Instances...)
		sort.Strings(stable.Groups[i].Instances)
	}
	sort.Slice(stable.Groups, func(i, j int) bool { return stable.Groups[i].Zone < stable.Groups[j].Zone })
	stable.Nodes = append([]Node(nil), s.Nodes...)
	for i := range stable.Nodes {
		stable.Nodes[i].ResourceVersion = ""
		stable.Nodes[i].Ready = false
	}
	sort.Slice(stable.Nodes, func(i, j int) bool { return stable.Nodes[i].Name < stable.Nodes[j].Name })
	stable.Pods = []Pod{}
	for _, p := range s.Pods {
		if _, exists := nodes[p.Node]; exists && !p.Ignored() {
			p.Terminating = false
			stable.Pods = append(stable.Pods, p)
		}
	}
	sort.Slice(stable.Pods, func(i, j int) bool { return stable.Pods[i].Key() < stable.Pods[j].Key() })
	stable.Workloads = append([]Workload(nil), refs...)
	for i := range stable.Workloads {
		stable.Workloads[i].Ready, stable.Workloads[i].ObservedGeneration = 0, 0
	}
	data, err := json.Marshal(struct {
		Version  string   `json:"version"`
		Snapshot Snapshot `json:"snapshot"`
		Options  Options  `json:"options"`
	}{planVersion, stable, opts})
	if err != nil {
		return "", fmt.Errorf("encode plan fingerprint: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
