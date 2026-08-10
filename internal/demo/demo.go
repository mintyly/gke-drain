// Package demo provides a deterministic synthetic cluster for offline planning.
// It contains no credentials and implements no mutating backend.
package demo

import (
	"fmt"
	"time"

	"github.com/mintyly/gke-drain/internal/drain"
)

func Snapshot() drain.Snapshot {
	r := drain.Resource{Project: "demo-project", Location: "asia-southeast1", Cluster: "demo-cluster", Pool: "workers"}
	s := drain.Snapshot{
		Resource: r, ClusterID: "synthetic-demo-cluster", ClusterStatus: "RUNNING", PoolStatus: "RUNNING",
		Locations: []string{"asia-southeast1-a", "asia-southeast1-b"},
		Workloads: []drain.Workload{{Namespace: "demo", Name: "web-6cdb7", UID: "demo-replicaset-uid", Kind: "ReplicaSet", Desired: 6, Ready: 6, Generation: 1, ObservedGeneration: 1}},
	}
	for _, suffix := range []string{"a", "b"} {
		zone := "asia-southeast1-" + suffix
		g := drain.Group{URL: "https://www.googleapis.com/compute/v1/projects/demo-project/zones/" + zone + "/instanceGroupManagers/demo-workers-" + suffix, Zone: zone, TargetSize: 3, Stable: true}
		for i := 0; i < 3; i++ {
			name := fmt.Sprintf("gke-demo-workers-%s-%d", suffix, i)
			provider := "gce://demo-project/" + zone + "/" + name
			g.Instances = append(g.Instances, provider)
			s.Nodes = append(s.Nodes, drain.Node{Name: name, UID: "uid-" + name, ResourceVersion: "1", Zone: zone, ProviderID: provider,
				MachineType: "e2-standard-2", CreatedAt: time.Date(2026, 10, 1+i, 0, 0, 0, 0, time.UTC), Ready: true})
			s.Pods = append(s.Pods, drain.Pod{Namespace: "demo", Name: fmt.Sprintf("web-6cdb7-%s%d", suffix, i), UID: fmt.Sprintf("pod-uid-%s%d", suffix, i),
				Node: name, OwnerKind: "ReplicaSet", OwnerName: "web-6cdb7", OwnerUID: "demo-replicaset-uid"})
		}
		s.Groups = append(s.Groups, g)
	}
	return s
}
