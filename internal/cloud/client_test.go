package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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
