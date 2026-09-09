// Package cloud implements the GKE, Compute read-only, and Kubernetes REST
// adapters. Kubernetes credentials are bound to the selected GKE cluster's CA
// and IP endpoint; the user's current kubeconfig context is never consulted.
package cloud

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/mintyly/gke-drain/internal/drain"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const maxResponseBytes = 32 << 20

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var projectID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,61}[a-z0-9]$`)

// Client deliberately has no user-configurable API URL or insecure TLS option.
type Client struct {
	resource    drain.Resource
	cloudHTTP   *http.Client
	kubeHTTP    *http.Client
	gkeBase     string
	computeBase string
	kubeBase    string
	clusterID   string
	endpoint    string
	ca          string
	quota       string
}

var _ drain.Backend = (*Client)(nil)

// New resolves Application Default Credentials and obtains the selected
// cluster's connection information from the authenticated GKE API.
func New(ctx context.Context, resource drain.Resource) (*Client, error) {
	if err := validateResource(resource); err != nil {
		return nil, err
	}
	// ADC token sources retain their construction context. Keep token refresh
	// alive for the bounded cleanup phase after SIGINT cancels the run; each
	// refresh still has its own HTTP timeout, and API requests retain their
	// caller's deadline/cancellation below.
	creds, err := google.FindDefaultCredentials(credentialContext(ctx), "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("application default credentials: %w", err)
	}
	quota := os.Getenv("GOOGLE_CLOUD_QUOTA_PROJECT")
	if quota == "" && len(creds.JSON) > 0 {
		var data struct {
			QuotaProject string `json:"quota_project_id"`
		}
		if json.Unmarshal(creds.JSON, &data) == nil {
			quota = data.QuotaProject
		}
	}
	if quota != "" && !identifier.MatchString(quota) {
		return nil, errors.New("ADC quota project is not a valid project identifier")
	}
	c := &Client{resource: resource, gkeBase: "https://container.googleapis.com/v1",
		computeBase: "https://compute.googleapis.com/compute/v1", quota: quota}
	c.cloudHTTP = authenticatedClient(creds.TokenSource, http.DefaultTransport.(*http.Transport).Clone())
	var cluster clusterJSON
	if err := c.cloud(ctx, http.MethodGet, c.gkeBase+"/"+resource.ClusterPath(), nil, &cluster, true); err != nil {
		return nil, fmt.Errorf("read selected GKE cluster: %w", err)
	}
	if cluster.Autopilot.Enabled {
		return nil, errors.New("Autopilot clusters are not supported; select a GKE Standard cluster")
	}
	if cluster.ID == "" {
		return nil, errors.New("GKE did not return a stable cluster ID")
	}
	kubeHTTP, endpoint, err := clusterClient(cluster, creds.TokenSource)
	if err != nil {
		return nil, err
	}
	c.kubeHTTP, c.kubeBase = kubeHTTP, endpoint
	c.clusterID, c.endpoint, c.ca = cluster.ID, cluster.Endpoint, cluster.MasterAuth.ClusterCACertificate
	return c, nil
}

func credentialContext(ctx context.Context) context.Context {
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: http.DefaultTransport.(*http.Transport).Clone(),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("credential redirects are not permitted")
		},
	}
	return context.WithValue(context.WithoutCancel(ctx), oauth2.HTTPClient, client)
}

func validateResource(r drain.Resource) error {
	if !projectID.MatchString(r.Project) {
		return errors.New("project must be a Google Cloud project ID, not a numeric project number")
	}
	for name, value := range map[string]string{"project": r.Project, "location": r.Location, "cluster": r.Cluster, "node pool": r.Pool} {
		if !identifier.MatchString(value) {
			return fmt.Errorf("invalid %s identifier", name)
		}
	}
	return nil
}

func authenticatedClient(source oauth2.TokenSource, base http.RoundTripper) *http.Client {
	return &http.Client{Timeout: 30 * time.Second,
		Transport:     &oauth2.Transport{Source: oauth2.ReuseTokenSource(nil, source), Base: base},
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("API redirects are not permitted") }}
}

func clusterClient(cluster clusterJSON, source oauth2.TokenSource) (*http.Client, string, error) {
	// Support IP endpoints only. DNS endpoints use a different access model and
	// must not silently fall back to an arbitrary kubeconfig or insecure TLS.
	ip := net.ParseIP(cluster.Endpoint)
	if ip == nil {
		return nil, "", errors.New("selected GKE cluster has no usable IP endpoint; DNS-only endpoints are not supported")
	}
	cert, err := base64.StdEncoding.DecodeString(cluster.MasterAuth.ClusterCACertificate)
	if err != nil {
		return nil, "", errors.New("GKE returned an invalid base64 cluster CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert) {
		return nil, "", errors.New("GKE returned an invalid PEM cluster CA")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	return authenticatedClient(source, transport), "https://" + net.JoinHostPort(ip.String(), "443"), nil
}

type apiError struct {
	Method, Path string
	Code         int
	Detail       string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.Method, e.Path, e.Code, e.Detail)
}
func (e *apiError) Is(target error) bool {
	return (target == drain.ErrNotFound && e.Code == http.StatusNotFound) ||
		(target == drain.ErrRetryable && (e.Code == 429 || e.Code == 500 || e.Code == 502 || e.Code == 503 || e.Code == 504))
}

func (c *Client) cloud(ctx context.Context, method, address string, input, output any, safeRead bool) error {
	return c.request(ctx, c.cloudHTTP, method, address, "application/json", input, output, safeRead, true)
}
func (c *Client) kube(ctx context.Context, method, path, contentType string, input, output any) error {
	return c.request(ctx, c.kubeHTTP, method, c.kubeBase+path, contentType, input, output, method == http.MethodGet, false)
}

// request retries only explicitly read-only requests. Resize, cordon, uncordon,
// and eviction are never retransmitted here, even after a transport failure.
func (c *Client) request(ctx context.Context, client *http.Client, method, address, contentType string, input, output any, safeRead, cloud bool) error {
	var payload []byte
	var err error
	if input != nil {
		payload, err = json.Marshal(input)
		if err != nil {
			return err
		}
	}
	for attempt := 0; ; attempt++ {
		err = c.once(ctx, client, method, address, contentType, payload, output, cloud)
		if err == nil || !safeRead || attempt >= 2 || ctx.Err() != nil || !isTransientRead(err) {
			return err
		}
		if err := sleep(ctx, time.Duration(attempt+1)*100*time.Millisecond); err != nil {
			return err
		}
	}
}

func isTransientRead(err error) bool {
	var ae *apiError
	if errors.As(err, &ae) {
		return errors.Is(ae, drain.ErrRetryable)
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func (c *Client) once(ctx context.Context, client *http.Client, method, address, contentType string, payload []byte, output any, cloud bool) error {
	req, err := http.NewRequestWithContext(ctx, method, address, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("construct API request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "gke-drain/0.1")
	if payload != nil {
		req.Header.Set("Content-Type", contentType)
	}
	if cloud && c.quota != "" {
		req.Header.Set("x-goog-user-project", c.quota)
	}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, req.URL.Path, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read API response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return errors.New("API response exceeds the 32 MiB safety limit; use a smaller cluster")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var data struct {
			Message string          `json:"message"`
			Error   json.RawMessage `json:"error"`
		}
		_ = json.Unmarshal(body, &data)
		message := data.Message
		if message == "" && len(data.Error) > 0 {
			var nested struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(data.Error, &nested) == nil {
				message = nested.Message
			}
		}
		if message == "" {
			message = http.StatusText(res.StatusCode)
		}
		message = shortMessage(message)
		return &apiError{Method: method, Path: req.URL.Path, Code: res.StatusCode, Detail: message}
	}
	if output != nil {
		if err := json.Unmarshal(body, output); err != nil {
			return fmt.Errorf("decode %s response: %w", req.URL.Path, err)
		}
	}
	return nil
}

func shortMessage(message string) string {
	if len(message) > 1024 {
		message = message[:1024] + "…"
	}
	return strings.Join(strings.Fields(message), " ")
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// computePath validates all resource URLs received from APIs before a token is
// sent anywhere. It returns a reconstructed path under our fixed Compute host.
func computePath(raw, project, kind string) (path, zone, name string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || (u.Host != "compute.googleapis.com" && u.Host != "www.googleapis.com") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
		return "", "", "", errors.New("unexpected Compute resource URL")
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 8 || parts[0] != "compute" || parts[1] != "v1" || parts[2] != "projects" || parts[3] != project || parts[4] != "zones" || parts[6] != kind || !identifier.MatchString(parts[5]) || !identifier.MatchString(parts[7]) {
		return "", "", "", errors.New("Compute resource URL does not match the selected project, zone, or resource type")
	}
	return "/" + strings.Join(parts[2:], "/"), parts[5], parts[7], nil
}
