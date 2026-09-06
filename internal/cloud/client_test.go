package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mintyly/gke-drain/internal/drain"
	"golang.org/x/oauth2"
)

var testResource = drain.Resource{Project: "demo", Location: "us-central1-a", Cluster: "demo", Pool: "workers"}

func fixtureClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &Client{resource: testResource, cloudHTTP: server.Client(), kubeHTTP: server.Client(),
		gkeBase: server.URL + "/gke", computeBase: server.URL + "/compute", kubeBase: server.URL, clusterID: "cluster-id"}
}

func respond(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

func fixtureNode(name string) nodeJSON {
	n := nodeJSON{Metadata: metadataJSON{Name: name, UID: "uid-" + name, ResourceVersion: "new-version",
		Labels:            map[string]string{"cloud.google.com/gke-nodepool": "workers", "topology.kubernetes.io/zone": "us-central1-a", "node.kubernetes.io/instance-type": "e2-standard-2"},
		CreationTimestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}}
	n.Spec.ProviderID = "gce://demo/us-central1-a/" + name
	n.Status.Conditions = append(n.Status.Conditions, struct {
		Type   string `json:"type"`
		Status string `json:"status"`
	}{Type: "Ready", Status: "True"})
	return n
}

func fixturePod(name, node string) podJSON {
	p := podJSON{Metadata: metadataJSON{Name: name, Namespace: "default", UID: "uid-" + name,
		OwnerReferences: []ownerJSON{{Kind: "ReplicaSet", Name: "web", UID: "rs-uid", Controller: true}}}}
	p.Spec.NodeName = node
	p.Status.Phase = "Running"
	return p
}

func TestSnapshotPaginatesAndMapsExactMIGMembership(t *testing.T) {
	var nodePages, migPages, opPages, podPages atomic.Int32
	c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/gke/" + testResource.ClusterPath():
			respond(t, w, map[string]any{"id": "cluster-id", "status": "RUNNING", "locations": []string{"us-central1-a"}})
		case "/gke/" + testResource.PoolPath():
			respond(t, w, map[string]any{"name": "workers", "status": "RUNNING", "instanceGroupUrls": []string{"https://www.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instanceGroupManagers/mig"}})
		case "/gke/projects/demo/locations/us-central1-a/operations":
			opPages.Add(1)
			if r.URL.Query().Get("pageToken") == "" {
				respond(t, w, map[string]any{"operations": []any{}, "nextPageToken": "op/next+token"})
			} else {
				if r.URL.Query().Get("pageToken") != "op/next+token" {
					t.Error("operation page token not preserved")
				}
				respond(t, w, map[string]any{"operations": []any{map[string]any{"status": "RUNNING", "targetLink": "https://container.googleapis.com/v1/projects/demo/zones/us-central1-a/clusters/unrelated"}}})
			}
		case "/compute/projects/demo/zones/us-central1-a/instanceGroupManagers/mig":
			respond(t, w, map[string]any{"targetSize": 2, "status": map[string]any{"isStable": true}})
		case "/compute/projects/demo/zones/us-central1-a/instanceGroupManagers/mig/listManagedInstances":
			migPages.Add(1)
			if r.Method != http.MethodPost {
				t.Errorf("listManagedInstances method %s", r.Method)
			}
			name, token := "node-a", "mig/next+token"
			if r.URL.Query().Get("pageToken") != "" {
				name, token = "node-b", ""
				if r.URL.Query().Get("pageToken") != "mig/next+token" {
					t.Error("MIG page token not preserved")
				}
			}
			respond(t, w, map[string]any{"managedInstances": []any{map[string]any{"instance": "https://compute.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instances/" + name, "currentAction": "NONE"}}, "nextPageToken": token})
		case "/api/v1/nodes":
			nodePages.Add(1)
			if r.URL.Query().Get("labelSelector") != "cloud.google.com/gke-nodepool=workers" {
				t.Error("wrong node pool selector")
			}
			name, token := "node-a", "nodes/next+token"
			if r.URL.Query().Get("continue") != "" {
				name, token = "node-b", ""
			}
			respond(t, w, map[string]any{"metadata": map[string]string{"continue": token}, "items": []nodeJSON{fixtureNode(name)}})
		case "/api/v1/pods":
			if r.URL.Query().Get("fieldSelector") == "spec.nodeName=node-b" {
				respond(t, w, map[string]any{"items": []any{}})
				return
			}
			if r.URL.Query().Get("fieldSelector") != "spec.nodeName=node-a" {
				t.Error("wrong pod field selector")
			}
			podPages.Add(1)
			if r.URL.Query().Get("continue") == "" {
				respond(t, w, map[string]any{"metadata": map[string]string{"continue": "pods/next+token"}, "items": []podJSON{fixturePod("web-1", "node-a")}})
			} else {
				daemon := fixturePod("agent", "node-a")
				daemon.Metadata.OwnerReferences[0].Kind = "DaemonSet"
				respond(t, w, map[string]any{"items": []podJSON{daemon}})
			}
		case "/apis/apps/v1/namespaces/default/replicasets/web":
			respond(t, w, map[string]any{"metadata": map[string]any{"name": "web", "namespace": "default", "uid": "rs-uid", "generation": 3}, "spec": map[string]any{"replicas": 2}, "status": map[string]any{"readyReplicas": 2, "observedGeneration": 3}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	})
	s, err := c.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.Busy || s.Autoscaling || s.Autopilot || len(s.Nodes) != 2 || len(s.Groups) != 1 || len(s.Pods) != 2 || len(s.Workloads) != 1 {
		t.Fatalf("incorrect snapshot: %+v", s)
	}
	if len(s.Locations) != 1 || s.Locations[0] != "us-central1-a" {
		t.Fatalf("pool did not inherit cluster locations: %v", s.Locations)
	}
	if s.Groups[0].TargetSize != 2 || !s.Groups[0].Stable || len(s.Groups[0].Instances) != 2 || s.Groups[0].Instances[1] != "gce://demo/us-central1-a/node-b" {
		t.Fatalf("incorrect membership: %+v", s.Groups)
	}
	if s.Workloads[0].Ready != 2 || s.Workloads[0].UID != "rs-uid" {
		t.Fatalf("incorrect workload: %+v", s.Workloads)
	}
	if nodePages.Load() != 2 || migPages.Load() != 2 || opPages.Load() != 2 || podPages.Load() != 2 {
		t.Fatalf("pagination not exhausted: nodes %d MIG %d operations %d pods %d", nodePages.Load(), migPages.Load(), opPages.Load(), podPages.Load())
	}
}

func TestSnapshotRejectsRecreatedCluster(t *testing.T) {
	var calls atomic.Int32
	c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		respond(t, w, map[string]any{"id": "different-cluster"})
	})
	_, err := c.Snapshot(context.Background())
	if err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("expected cluster identity rejection, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("continued after cluster identity mismatch")
	}
}

func TestMIGURLCannotRedirectCredentials(t *testing.T) {
	var calls atomic.Int32
	c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); t.Error("unsafe URL reached HTTP client") })
	for _, raw := range []string{
		"https://attacker.example/compute/v1/projects/demo/zones/us-central1-a/instanceGroupManagers/mig",
		"http://compute.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instanceGroupManagers/mig",
		"https://compute.googleapis.com/compute/v1/projects/other/zones/us-central1-a/instanceGroupManagers/mig",
		"https://compute.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instanceGroupManagers/mig?token=1",
		"https://compute.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instanceGroupManagers/%2e%2e",
	} {
		if _, err := c.group(context.Background(), raw); err == nil {
			t.Errorf("accepted unsafe URL %q", raw)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("made HTTP request for an unsafe URL")
	}
}

func TestCordonRechecksIdentityAndUsesLatestResourceVersion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*nodeJSON)
		wantErr bool
	}{
		{name: "heartbeat version changed"},
		{name: "UID replaced", change: func(n *nodeJSON) { n.Metadata.UID = "new-uid" }, wantErr: true},
		{name: "provider changed", change: func(n *nodeJSON) { n.Spec.ProviderID = "gce://demo/us-central1-a/replaced" }, wantErr: true},
		{name: "zone changed", change: func(n *nodeJSON) { n.Metadata.Labels["topology.kubernetes.io/zone"] = "us-central1-b" }, wantErr: true},
		{name: "pool changed", change: func(n *nodeJSON) { n.Metadata.Labels["cloud.google.com/gke-nodepool"] = "other" }, wantErr: true},
		{name: "not Ready", change: func(n *nodeJSON) { n.Status.Conditions[0].Status = "False" }, wantErr: true},
		{name: "already cordoned", change: func(n *nodeJSON) { n.Spec.Unschedulable = true }, wantErr: true},
		{name: "foreign owner", change: func(n *nodeJSON) {
			n.Metadata.Annotations = map[string]string{drain.OwnershipAnnotation: "someone-else"}
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := fixtureNode("node-a")
			current.Metadata.Annotations = map[string]string{"keep": "yes"}
			if tc.change != nil {
				tc.change(&current)
			}
			var patches atomic.Int32
			c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					respond(t, w, current)
					return
				}
				patches.Add(1)
				if r.Method != http.MethodPatch || r.Header.Get("Content-Type") != "application/json-patch+json" {
					t.Errorf("unguarded mutation method/content type: %s %s", r.Method, r.Header.Get("Content-Type"))
				}
				var patch []patchOperation
				if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
					t.Error(err)
				}
				if len(patch) != 4 || patch[0].Op != "test" || patch[0].Path != "/metadata/uid" || patch[0].Value != "uid-node-a" || patch[1].Path != "/metadata/resourceVersion" || patch[1].Value != "new-version" {
					t.Errorf("missing latest UID/RV guards: %+v", patch)
				}
				if len(patch) == 4 {
					annotations, _ := patch[3].Value.(map[string]any)
					if annotations["keep"] != "yes" || annotations[drain.OwnershipAnnotation] != "run-1" {
						t.Errorf("ownership or existing annotation lost: %v", annotations)
					}
				}
				respond(t, w, map[string]any{})
			})
			planned := toNode(fixtureNode("node-a"))
			planned.ResourceVersion = "old-version"
			err := c.Cordon(context.Background(), planned, "run-1")
			if (err != nil) != tc.wantErr {
				t.Fatalf("Cordon error %v, wantErr %v", err, tc.wantErr)
			}
			want := int32(1)
			if tc.wantErr {
				want = 0
			}
			if patches.Load() != want {
				t.Fatalf("patch count %d, want %d", patches.Load(), want)
			}
		})
	}
}

func TestUncordonNeverTouchesAnotherRunsCordon(t *testing.T) {
	for _, tc := range []struct {
		name, owner, uid string
		missing, wantErr bool
	}{
		{name: "own node", owner: "run-1", uid: "uid-node-a"},
		{name: "another run", owner: "someone-else", uid: "uid-node-a", wantErr: true},
		{name: "recreated node", owner: "run-1", uid: "different-uid", wantErr: true},
		{name: "already removed", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var patches atomic.Int32
			c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					if tc.missing {
						http.NotFound(w, r)
						return
					}
					node := fixtureNode("node-a")
					node.Metadata.UID = tc.uid
					node.Spec.Unschedulable = true
					node.Metadata.Annotations = map[string]string{drain.OwnershipAnnotation: tc.owner}
					respond(t, w, node)
					return
				}
				patches.Add(1)
				var patch []map[string]any
				if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
					t.Error(err)
				}
				if len(patch) != 5 || patch[2]["op"] != "test" || patch[2]["path"] != "/metadata/annotations/gke-drain.mintyly.dev~1run-id" || patch[2]["value"] != "run-1" || patch[3]["value"] != false {
					t.Errorf("unsafe uncordon patch: %v", patch)
				}
				respond(t, w, map[string]any{})
			})
			err := c.Uncordon(context.Background(), toNode(fixtureNode("node-a")), "run-1")
			if (err != nil) != tc.wantErr {
				t.Fatalf("error %v, wantErr %v", err, tc.wantErr)
			}
			want := int32(1)
			if tc.missing || tc.wantErr {
				want = 0
			}
			if patches.Load() != want {
				t.Fatalf("patch count %d, want %d", patches.Load(), want)
			}
		})
	}
}

func TestEvictionUsesUIDNeverDeletesAndOnlyRetries429(t *testing.T) {
	for _, code := range []int{200, 404, 409, 429, 500, 502, 503, 504} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var calls atomic.Int32
			c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPost || r.URL.Path != "/api/v1/namespaces/default/pods/web-1/eviction" {
					t.Errorf("unexpected destructive request: %s %s", r.Method, r.URL.Path)
				}
				var body struct {
					APIVersion    string `json:"apiVersion"`
					DeleteOptions struct {
						Preconditions map[string]string `json:"preconditions"`
					} `json:"deleteOptions"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if body.APIVersion != "policy/v1" || body.DeleteOptions.Preconditions["uid"] != "uid-web-1" {
					t.Errorf("missing eviction API or UID precondition: %+v", body)
				}
				w.WriteHeader(code)
				respond(t, w, map[string]string{"message": "test budget response"})
			})
			err := c.Evict(context.Background(), toPod(fixturePod("web-1", "node-a")))
			if errors.Is(err, drain.ErrRetryable) != (code == 429) {
				t.Errorf("retryable %v for %d: %v", errors.Is(err, drain.ErrRetryable), code, err)
			}
			if errors.Is(err, drain.ErrNotFound) != (code == 404) {
				t.Errorf("notfound %v for %d", errors.Is(err, drain.ErrNotFound), code)
			}
			if (err == nil) != (code == 200) {
				t.Errorf("error %v for code %d", err, code)
			}
			if calls.Load() != 1 {
				t.Errorf("mutation retried %d times", calls.Load())
			}
		})
	}
}

func TestResizeDoesNotRetryAmbiguousMutation(t *testing.T) {
	var calls atomic.Int32
	c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, ":setSize") {
			t.Errorf("wrong resize request %s %s", r.Method, r.URL.Path)
		}
		var body map[string]int
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["nodeCount"] != 2 {
			t.Errorf("wrong resize count %v", body)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		respond(t, w, map[string]any{"error": map[string]string{"message": "backend unavailable"}})
	})
	_, err := c.Resize(context.Background(), 2)
	if err == nil || !strings.Contains(err.Error(), "outcome may be unknown") {
		t.Fatalf("missing uncertainty error: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("non-idempotent resize retried: %d", calls.Load())
	}
}

func TestWorkloadsRejectControllerRecreation(t *testing.T) {
	c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		respond(t, w, map[string]any{"metadata": map[string]string{"name": "web", "namespace": "default", "uid": "replacement-uid"}, "spec": map[string]int{"replicas": 2}, "status": map[string]int{"readyReplicas": 2}})
	})
	_, err := c.Workloads(context.Background(), []drain.Workload{{Kind: "ReplicaSet", Namespace: "default", Name: "web", UID: "original-uid"}})
	if err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("controller replacement not rejected: %v", err)
	}
}

func TestOAuthClientDoesNotFollowRedirect(t *testing.T) {
	var leaked atomic.Int32
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer foreign.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("expected authenticated initial request")
		}
		http.Redirect(w, r, foreign.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	client := authenticatedClient(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"}), http.DefaultTransport)
	res, err := client.Get(origin.URL)
	if res != nil {
		res.Body.Close()
	}
	if err == nil {
		t.Fatal("redirect unexpectedly followed")
	}
	if leaked.Load() != 0 {
		t.Fatal("OAuth token request reached foreign host")
	}
}

func TestClusterConnectionRefusesDNSAndInvalidCA(t *testing.T) {
	for _, cluster := range []clusterJSON{{Endpoint: "attacker.example"}, {Endpoint: "127.0.0.1"}} {
		if _, _, err := clusterClient(cluster, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"})); err == nil {
			t.Fatalf("accepted invalid cluster connection %+v", cluster)
		}
	}
}

func TestCredentialsRefreshAfterRunCancellationButAPIRequestsStillCancel(t *testing.T) {
	var refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshes.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "test-refresh" {
			t.Errorf("unexpected token grant")
		}
		respond(t, w, map[string]any{"access_token": "refreshed-token", "token_type": "Bearer", "expires_in": 3600})
	}))
	defer server.Close()
	runCtx, cancel := context.WithCancel(context.Background())
	credentialsCtx := credentialContext(runCtx)
	configuredClient, ok := credentialsCtx.Value(oauth2.HTTPClient).(*http.Client)
	if !ok || configuredClient.Timeout != 30*time.Second {
		t.Fatal("credential refresh lacks a bounded HTTP client")
	}
	config := oauth2.Config{ClientID: "test-client", Endpoint: oauth2.Endpoint{TokenURL: server.URL, AuthStyle: oauth2.AuthStyleInParams}}
	source := config.TokenSource(credentialsCtx, &oauth2.Token{RefreshToken: "test-refresh", Expiry: time.Now().Add(-time.Hour)})
	cancel()
	token, err := source.Token()
	if err != nil || token.AccessToken != "refreshed-token" {
		t.Fatalf("cleanup credential refresh failed after run cancellation: token present %v, error %v", token != nil, err)
	}
	if refreshes.Load() != 1 {
		t.Fatalf("refresh count %d", refreshes.Load())
	}
	// Detaching token refresh must not detach actual cloud mutations from their
	// caller's canceled context.
	client := authenticatedClient(source, http.DefaultTransport)
	req, err := http.NewRequestWithContext(runCtx, http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(req)
	if response != nil {
		response.Body.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("API request escaped run cancellation: %v", err)
	}
	if refreshes.Load() != 1 {
		t.Fatal("canceled API request reached server")
	}
}

func TestActiveOperationsFailClosedForUnknownTargets(t *testing.T) {
	for _, target := range []string{"", "https://attacker.example/cluster", "https://container.googleapis.com/unrecognized", "https://container.googleapis.com/v1/projects/demo/locations/us-central1-a/clusters/demo/nodePools/workers"} {
		c := fixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
			respond(t, w, map[string]any{"operations": []operationJSON{{Status: "RUNNING", TargetLink: target}}})
		})
		busy, err := c.activeOperations(context.Background())
		if err != nil || !busy {
			t.Errorf("unknown/selected target was not blocked: %q busy %v err %v", target, busy, err)
		}
	}
}
