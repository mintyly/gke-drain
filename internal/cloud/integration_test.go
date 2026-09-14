package cloud

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mintyly/gke-drain/internal/drain"
)

// This joins the real planner, runner, and REST adapter against simulated
// control planes. It catches contract mismatches that separate backend/domain
// mocks cannot, while making no requests to Google Cloud.
func TestRunnerThroughRESTControlPlanes(t *testing.T) {
	for _, scenario := range []string{"healthy resize", "PDB blocks resize"} {
		t.Run(scenario, func(t *testing.T) {
			var mu sync.Mutex
			nodes := map[string]nodeJSON{}
			pods := map[string]podJSON{}
			for _, name := range []string{"node-a", "node-b", "node-c"} {
				nodes[name] = fixtureNode(name)
				pods["web-"+name] = fixturePod("web-"+name, name)
			}
			cordons, evictions, resizes, cleanups := 0, 0, 0, 0
			operation := map[string]string{"name": "operation-resize", "status": "DONE", "location": testResource.Location,
				"targetLink": "https://container.googleapis.com/v1/" + testResource.PoolPath()}
			c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch {
				case r.URL.Path == "/gke/"+testResource.ClusterPath():
					respond(t, w, map[string]any{"id": "cluster-id", "status": "RUNNING", "locations": []string{testResource.Location}})
				case r.URL.Path == "/gke/"+testResource.PoolPath():
					respond(t, w, map[string]any{"name": testResource.Pool, "status": "RUNNING", "locations": []string{testResource.Location},
						"instanceGroupUrls": []string{"https://www.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instanceGroupManagers/mig"}})
				case r.URL.Path == "/gke/projects/demo/locations/us-central1-a/operations":
					respond(t, w, map[string]any{"operations": []any{}})
				case r.URL.Path == "/compute/projects/demo/zones/us-central1-a/instanceGroupManagers/mig":
					respond(t, w, map[string]any{"targetSize": len(nodes), "status": map[string]bool{"isStable": true}})
				case strings.HasSuffix(r.URL.Path, "/listManagedInstances"):
					instances := []map[string]string{}
					for name := range nodes {
						instances = append(instances, map[string]string{"instance": "https://compute.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instances/" + name, "currentAction": "NONE"})
					}
					respond(t, w, map[string]any{"managedInstances": instances})
				case r.URL.Path == "/api/v1/nodes":
					items := []nodeJSON{}
					for _, node := range nodes {
						items = append(items, node)
					}
					respond(t, w, map[string]any{"items": items})
				case strings.HasPrefix(r.URL.Path, "/api/v1/nodes/"):
					name := strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/")
					node, exists := nodes[name]
					if !exists {
						http.NotFound(w, r)
						return
					}
					if r.Method == http.MethodGet {
						respond(t, w, node)
						return
					}
					if r.Method != http.MethodPatch {
						t.Errorf("unexpected node mutation: %s", r.Method)
					}
					var patch []patchOperation
					if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
						t.Error(err)
					}
					for _, p := range patch {
						switch {
						case p.Op == "add" && p.Path == "/spec/unschedulable":
							node.Spec.Unschedulable, _ = p.Value.(bool)
							if node.Spec.Unschedulable {
								cordons++
							} else {
								cleanups++
							}
						case p.Op == "add" && p.Path == "/metadata/annotations":
							node.Metadata.Annotations = map[string]string{}
							for key, value := range p.Value.(map[string]any) {
								node.Metadata.Annotations[key] = value.(string)
							}
						case p.Op == "remove":
							delete(node.Metadata.Annotations, drain.OwnershipAnnotation)
						}
					}
					nodes[name] = node
					respond(t, w, node)
				case r.URL.Path == "/api/v1/pods":
					nodeName := strings.TrimPrefix(r.URL.Query().Get("fieldSelector"), "spec.nodeName=")
					items := []podJSON{}
					for _, pod := range pods {
						if pod.Spec.NodeName == nodeName {
							items = append(items, pod)
						}
					}
					respond(t, w, map[string]any{"items": items})
				case r.URL.Path == "/apis/apps/v1/namespaces/default/replicasets/web":
					respond(t, w, map[string]any{"metadata": map[string]any{"name": "web", "namespace": "default", "uid": "rs-uid", "generation": 1},
						"spec": map[string]int{"replicas": 3}, "status": map[string]int{"readyReplicas": len(pods), "observedGeneration": 1}})
				case strings.HasSuffix(r.URL.Path, "/eviction"):
					evictions++
					if r.Method != http.MethodPost {
						t.Errorf("eviction used %s", r.Method)
					}
					if scenario == "PDB blocks resize" {
						w.WriteHeader(http.StatusTooManyRequests)
						respond(t, w, map[string]string{"message": "eviction would violate disruption budget"})
						return
					}
					parts := strings.Split(r.URL.Path, "/")
					name := parts[len(parts)-2]
					delete(pods, name)
					pods[name+"-replacement"] = fixturePod(name+"-replacement", "node-c")
					respond(t, w, map[string]string{"status": "Success"})
				case r.URL.Path == "/gke/"+testResource.PoolPath()+":setSize":
					resizes++
					if r.Method != http.MethodPost || evictions == 0 {
						t.Error("resize was not a POST after eviction")
					}
					var body map[string]int
					_ = json.NewDecoder(r.Body).Decode(&body)
					if body["nodeCount"] != 2 {
						t.Errorf("wrong final size: %v", body)
					}
					victim := "node-a"
					delete(nodes, victim)
					respond(t, w, operation)
				case r.URL.Path == "/gke/projects/demo/locations/us-central1-a/operations/operation-resize":
					respond(t, w, operation)
				default:
					t.Errorf("unexpected HTTP request: %s %s", r.Method, r.URL)
					http.NotFound(w, r)
				}
			})
			opts := drain.DefaultOptions()
			opts.TargetPerZone, opts.PollInterval = 2, time.Millisecond
			opts.DrainTimeout, opts.OperationTimeout = time.Second, time.Second
			if scenario == "PDB blocks resize" {
				opts.DrainTimeout = 100 * time.Millisecond
			}
			snapshot, err := c.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			plan, err := drain.BuildPlan(snapshot, opts)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Batches) != 1 || plan.Batches[0].Candidates[0].Name != "node-a" {
				t.Fatalf("unexpected plan: %+v", plan)
			}
			err = (drain.Runner{Backend: c, Options: opts, RunID: "integration-run"}).Run(context.Background(), plan)
			mu.Lock()
			defer mu.Unlock()
			var surviving []string
			for name, n := range nodes {
				surviving = append(surviving, name)
				if n.Spec.Unschedulable || n.Metadata.Annotations[drain.OwnershipAnnotation] != "" {
					t.Errorf("unexpected owned cordon survived completed scenario: %s", name)
				}
			}
			sort.Strings(surviving)
			if cordons != 1 || evictions == 0 {
				t.Fatalf("cordons=%d evictions=%d", cordons, evictions)
			}
			switch scenario {
			case "healthy resize":
				if err != nil || resizes != 1 || strings.Join(surviving, ",") != "node-b,node-c" {
					t.Fatalf("err=%v resizes=%d nodes=%v", err, resizes, surviving)
				}
			case "PDB blocks resize":
				if err == nil || resizes != 0 || len(nodes) != 3 || cleanups != 1 {
					t.Fatalf("err=%v resizes=%d nodes=%v cleanups=%d", err, resizes, surviving, cleanups)
				}
			}
		})
	}
}
