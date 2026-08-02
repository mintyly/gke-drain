package drain

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func testOptions(target, batch int) Options {
	o := DefaultOptions()
	o.TargetPerZone, o.BatchPerZone = target, batch
	o.DrainTimeout, o.OperationTimeout, o.PollInterval = 100*time.Millisecond, 100*time.Millisecond, time.Millisecond
	return o
}

func testSnapshot(zoneCount, size int, pods bool) Snapshot {
	s := Snapshot{
		Resource:  Resource{Project: "sample-project", Location: "asia-southeast1", Cluster: "demo", Pool: "workers"},
		ClusterID: "cluster-uid", ClusterStatus: "RUNNING", PoolStatus: "RUNNING",
	}
	for z := 0; z < zoneCount; z++ {
		zone := fmt.Sprintf("asia-southeast1-%c", 'a'+z)
		s.Locations = append(s.Locations, zone)
		g := Group{URL: "https://www.googleapis.com/compute/v1/projects/sample-project/zones/" + zone + "/instanceGroupManagers/workers", Zone: zone, TargetSize: size, Stable: true}
		for i := 0; i < size; i++ {
			name := fmt.Sprintf("worker-%c-%02d", 'a'+z, i)
			n := Node{Name: name, UID: "uid-" + name, ResourceVersion: "1", Zone: zone, ProviderID: "gce://sample-project/" + zone + "/" + name,
				MachineType: "e2-standard-4", CreatedAt: time.Date(2026, time.January, 1, i, 0, 0, 0, time.UTC), Ready: true}
			s.Nodes = append(s.Nodes, n)
			g.Instances = append(g.Instances, n.ProviderID)
			if pods {
				s.Pods = append(s.Pods, Pod{Namespace: "default", Name: "app-" + name, UID: "pod-" + name, Node: name,
					OwnerKind: "ReplicaSet", OwnerName: "app", OwnerUID: "controller-uid"})
			}
		}
		s.Groups = append(s.Groups, g)
	}
	if pods {
		s.Workloads = []Workload{{Namespace: "default", Name: "app", UID: "controller-uid", Kind: "ReplicaSet", Desired: zoneCount * size, Ready: zoneCount * size, Generation: 1, ObservedGeneration: 1}}
	}
	return s
}

func cloneSnapshot(s Snapshot) Snapshot {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	var copied Snapshot
	if err := json.Unmarshal(data, &copied); err != nil {
		panic(err)
	}
	return copied
}

func TestRegionalBatchesHavePerZoneBoundsAndShortFinalBatch(t *testing.T) {
	s := testSnapshot(3, 8, true)
	p, err := BuildPlan(s, testOptions(3, 2))
	if err != nil {
		t.Fatal(err)
	}
	if p.TotalBefore != 24 || p.TotalAfter != 9 || len(p.Batches) != 3 {
		t.Fatalf("unexpected plan: %#v", p)
	}
	seen := make(map[string]bool)
	for i, want := range []struct{ before, after, candidates int }{{8, 6, 6}, {6, 4, 6}, {4, 3, 3}} {
		b := p.Batches[i]
		if b.BeforePerZone != want.before || b.AfterPerZone != want.after || len(b.Candidates) != want.candidates {
			t.Fatalf("batch %d has wrong bounds: %#v", i, b)
		}
		zoneCounts := make(map[string]int)
		for _, n := range b.Candidates {
			if seen[n.Name] {
				t.Fatalf("candidate %s selected twice", n.Name)
			}
			seen[n.Name] = true
			zoneCounts[n.Zone]++
		}
		for _, zone := range p.Zones {
			if zoneCounts[zone] != want.before-want.after {
				t.Fatalf("zone %s has wrong batch count: %v", zone, zoneCounts)
			}
		}
	}
}

func TestSelectionIsMachineThenPodCountThenAgeThenName(t *testing.T) {
	s := testSnapshot(1, 5, false)
	s.Nodes[4].MachineType = "c3-standard-4"
	s.Nodes[2].CreatedAt = s.Nodes[1].CreatedAt
	s.Pods = []Pod{{Namespace: "default", Name: "one", UID: "one", Node: s.Nodes[0].Name, OwnerKind: "ReplicaSet", OwnerName: "app", OwnerUID: "app"}}
	s.Workloads = []Workload{{Namespace: "default", Name: "app", UID: "app", Kind: "ReplicaSet", Desired: 1, Ready: 1, Generation: 1, ObservedGeneration: 1}}
	o := testOptions(1, 4)
	o.PreferMachineType = "c3-standard-4"
	p, err := BuildPlan(s, o)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, n := range p.Batches[0].Candidates {
		got = append(got, n.Name)
	}
	want := []string{"worker-a-04", "worker-a-01", "worker-a-02", "worker-a-03"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selection %v; want %v", got, want)
	}
}

func TestPlanRejectsUnsafeOrInconsistentState(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Snapshot, *Options)
	}{
		{"zero target", func(_ *Snapshot, o *Options) { o.TargetPerZone = 0 }},
		{"negative target", func(_ *Snapshot, o *Options) { o.TargetPerZone = -1 }},
		{"zero batch", func(_ *Snapshot, o *Options) { o.BatchPerZone = 0 }},
		{"negative batch", func(_ *Snapshot, o *Options) { o.BatchPerZone = -1 }},
		{"scale up", func(_ *Snapshot, o *Options) { o.TargetPerZone = 4 }},
		{"zero drain deadline", func(_ *Snapshot, o *Options) { o.DrainTimeout = 0 }},
		{"zero operation deadline", func(_ *Snapshot, o *Options) { o.OperationTimeout = 0 }},
		{"zero poll interval", func(_ *Snapshot, o *Options) { o.PollInterval = 0 }},
		{"path injection", func(s *Snapshot, _ *Options) { s.Resource.Cluster = "../other" }},
		{"missing cluster identity", func(s *Snapshot, _ *Options) { s.ClusterID = "" }},
		{"autopilot", func(s *Snapshot, _ *Options) { s.Autopilot = true }},
		{"autoscaling", func(s *Snapshot, _ *Options) { s.Autoscaling = true }},
		{"busy", func(s *Snapshot, _ *Options) { s.Busy = true }},
		{"cluster not running", func(s *Snapshot, _ *Options) { s.ClusterStatus = "RECONCILING" }},
		{"pool not running", func(s *Snapshot, _ *Options) { s.PoolStatus = "ERROR" }},
		{"missing group", func(s *Snapshot, _ *Options) { s.Groups = s.Groups[:1] }},
		{"duplicate zone", func(s *Snapshot, _ *Options) { s.Locations[1] = s.Locations[0] }},
		{"unstable group", func(s *Snapshot, _ *Options) { s.Groups[0].Stable = false }},
		{"unequal zone targets", func(s *Snapshot, _ *Options) { s.Groups[0].TargetSize-- }},
		{"missing managed member", func(s *Snapshot, _ *Options) { s.Groups[0].Instances = s.Groups[0].Instances[1:] }},
		{"wrong provider project", func(s *Snapshot, _ *Options) {
			s.Groups[0].Instances[0] = "gce://other-project/asia-southeast1-a/worker-a-00"
		}},
		{"wrong node zone", func(s *Snapshot, _ *Options) { s.Nodes[0].Zone = s.Locations[1] }},
		{"missing node uid", func(s *Snapshot, _ *Options) { s.Nodes[0].UID = "" }},
		{"missing resource version", func(s *Snapshot, _ *Options) { s.Nodes[0].ResourceVersion = "" }},
		{"duplicate node name", func(s *Snapshot, _ *Options) { s.Nodes[1].Name = s.Nodes[0].Name }},
		{"duplicate node provider id", func(s *Snapshot, _ *Options) { s.Nodes[1].ProviderID = s.Nodes[0].ProviderID }},
		{"not ready", func(s *Snapshot, _ *Options) { s.Nodes[0].Ready = false }},
		{"preexisting cordon", func(s *Snapshot, _ *Options) { s.Nodes[0].Unschedulable = true }},
		{"foreign drain ownership", func(s *Snapshot, _ *Options) { s.Nodes[0].Owner = "someone-else" }},
		{"unmanaged pod", func(s *Snapshot, _ *Options) { s.Pods[0].OwnerKind = "" }},
		{"job pod", func(s *Snapshot, _ *Options) { s.Pods[0].OwnerKind = "Job" }},
		{"hostPath", func(s *Snapshot, o *Options) { s.Pods[0].HostPath = true; o.AllowEmptyDir = true; o.AllowPVC = true }},
		{"emptyDir", func(s *Snapshot, _ *Options) { s.Pods[0].EmptyDir = true }},
		{"persistent volume", func(s *Snapshot, _ *Options) { s.Pods[0].PVC = true }},
		{"terminating pod", func(s *Snapshot, _ *Options) { s.Pods[0].Terminating = true }},
		{"missing controller", func(s *Snapshot, _ *Options) { s.Workloads = nil }},
		{"recreated controller", func(s *Snapshot, _ *Options) { s.Workloads[0].UID = "new-uid" }},
		{"unhealthy controller", func(s *Snapshot, _ *Options) { s.Workloads[0].Ready-- }},
		{"unobserved controller", func(s *Snapshot, _ *Options) { s.Workloads[0].ObservedGeneration = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, o := testSnapshot(2, 3, true), testOptions(1, 1)
			tc.mutate(&s, &o)
			if _, err := BuildPlan(s, o); err == nil {
				t.Fatal("unsafe/inconsistent state unexpectedly produced a plan")
			}
		})
	}
}

func TestAllowedVolumeAcknowledgementsAndIgnoredPods(t *testing.T) {
	s, o := testSnapshot(1, 2, true), testOptions(1, 1)
	s.Pods[0].EmptyDir, s.Pods[0].PVC = true, true
	o.AllowEmptyDir, o.AllowPVC = true, true
	for _, pod := range []Pod{
		{Namespace: "kube-system", Name: "daemon", Node: s.Nodes[0].Name, OwnerKind: "DaemonSet", HostPath: true},
		{Namespace: "kube-system", Name: "mirror", Node: s.Nodes[0].Name, Mirror: true, HostPath: true},
		{Namespace: "default", Name: "completed", Node: s.Nodes[0].Name, Completed: true, OwnerKind: "Job"},
		{Namespace: "other", Name: "outside-pool", Node: "another-node", HostPath: true},
	} {
		s.Pods = append(s.Pods, pod)
	}
	if _, err := BuildPlan(s, o); err != nil {
		t.Fatal(err)
	}
}

func TestPlanIsPureAndFingerprintIsStableAcrossAPIOrdering(t *testing.T) {
	s, o := testSnapshot(2, 3, true), testOptions(1, 1)
	before := cloneSnapshot(s)
	p, err := BuildPlan(s, o)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s, before) {
		t.Fatal("BuildPlan mutated its input snapshot")
	}
	s.Nodes[0], s.Nodes[len(s.Nodes)-1] = s.Nodes[len(s.Nodes)-1], s.Nodes[0]
	s.Groups[0], s.Groups[1] = s.Groups[1], s.Groups[0]
	s.Locations[0], s.Locations[1] = s.Locations[1], s.Locations[0]
	s.Pods[0], s.Pods[len(s.Pods)-1] = s.Pods[len(s.Pods)-1], s.Pods[0]
	s.Nodes[0].ResourceVersion = "999"
	s.Workloads[0].Ready++
	s.Workloads[0].ObservedGeneration++
	q, err := BuildPlan(s, o)
	if err != nil {
		t.Fatal(err)
	}
	if p.Fingerprint != q.Fingerprint {
		t.Fatal("API ordering and volatile resource versions/readiness changed the fingerprint")
	}
	s.Nodes[0].UID = "replacement-node-uid"
	r, err := BuildPlan(s, o)
	if err != nil {
		t.Fatal(err)
	}
	if p.Fingerprint == r.Fingerprint {
		t.Fatal("recreated node failed to invalidate plan")
	}
}

func TestAlreadyAtTargetIsNoOp(t *testing.T) {
	p, err := BuildPlan(testSnapshot(3, 2, false), testOptions(2, 99))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Batches) != 0 || p.TotalBefore != p.TotalAfter || !strings.Contains(strings.Join(p.Warnings, " "), "per zone") {
		t.Fatalf("unexpected no-op plan: %#v", p)
	}
}
