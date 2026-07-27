// Package drain plans and orchestrates bounded GKE Standard node-pool reductions.
// It does not write Compute Engine instance groups or change workload replicas.
package drain

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const OwnershipAnnotation = "gke-drain.mintyly.dev/run-id"

// ErrRetryable means a read or eviction may be retried within its deadline.
// In particular, an eviction rejected with HTTP 429 must never become a DELETE.
var ErrRetryable = errors.New("retryable API response")
var ErrNotFound = errors.New("resource not found")

type Resource struct {
	Project  string `json:"project"`
	Location string `json:"location"`
	Cluster  string `json:"cluster"`
	Pool     string `json:"nodePool"`
}

func (r Resource) ClusterPath() string {
	return fmt.Sprintf("projects/%s/locations/%s/clusters/%s", r.Project, r.Location, r.Cluster)
}

func (r Resource) PoolPath() string { return r.ClusterPath() + "/nodePools/" + r.Pool }

type Group struct {
	URL        string `json:"url"`
	Zone       string `json:"zone"`
	TargetSize int    `json:"targetSize"`
	Stable     bool   `json:"stable"`
	// Instances are canonical gce://project/zone/name provider IDs, obtained
	// from Compute's read-only listManagedInstances operation.
	Instances []string `json:"instances"`
}

type Node struct {
	Name            string    `json:"name"`
	UID             string    `json:"uid"`
	ResourceVersion string    `json:"resourceVersion"`
	Zone            string    `json:"zone"`
	ProviderID      string    `json:"providerID"`
	MachineType     string    `json:"machineType"`
	CreatedAt       time.Time `json:"createdAt"`
	Ready           bool      `json:"ready"`
	Unschedulable   bool      `json:"unschedulable"`
	Owner           string    `json:"drainOwner,omitempty"`
}

type Pod struct {
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	UID         string `json:"uid"`
	Node        string `json:"node"`
	OwnerKind   string `json:"ownerKind"`
	OwnerName   string `json:"ownerName"`
	OwnerUID    string `json:"ownerUID"`
	Mirror      bool   `json:"mirror"`
	Completed   bool   `json:"completed"`
	Terminating bool   `json:"terminating"`
	EmptyDir    bool   `json:"emptyDir"`
	HostPath    bool   `json:"hostPath"`
	PVC         bool   `json:"persistentVolumeClaim"`
}

func (p Pod) Key() string { return p.Namespace + "/" + p.Name }

func (p Pod) Ignored() bool { return p.Mirror || p.Completed || p.OwnerKind == "DaemonSet" }

type Workload struct {
	Namespace          string `json:"namespace"`
	Name               string `json:"name"`
	UID                string `json:"uid"`
	Kind               string `json:"kind"`
	Desired            int    `json:"desired"`
	Ready              int    `json:"ready"`
	Generation         int64  `json:"generation"`
	ObservedGeneration int64  `json:"observedGeneration"`
}

type Snapshot struct {
	Resource      Resource   `json:"resource"`
	ClusterID     string     `json:"clusterID"`
	ClusterStatus string     `json:"clusterStatus"`
	PoolStatus    string     `json:"poolStatus"`
	Autopilot     bool       `json:"autopilot"`
	Autoscaling   bool       `json:"autoscaling"`
	Busy          bool       `json:"busy"`
	Locations     []string   `json:"locations"`
	Groups        []Group    `json:"groups"`
	Nodes         []Node     `json:"nodes"`
	Pods          []Pod      `json:"pods"`
	Workloads     []Workload `json:"workloads"`
}

type Options struct {
	TargetPerZone     int           `json:"targetPerZone"`
	BatchPerZone      int           `json:"batchPerZone"`
	PreferMachineType string        `json:"preferMachineType,omitempty"`
	AllowEmptyDir     bool          `json:"allowEmptyDir"`
	AllowPVC          bool          `json:"allowPersistentVolumes"`
	DrainTimeout      time.Duration `json:"drainTimeout"`
	OperationTimeout  time.Duration `json:"operationTimeout"`
	PollInterval      time.Duration `json:"pollInterval"`
}

func DefaultOptions() Options {
	return Options{TargetPerZone: -1, BatchPerZone: 1, DrainTimeout: 10 * time.Minute,
		OperationTimeout: 75 * time.Minute, PollInterval: 5 * time.Second}
}

type Batch struct {
	Number        int    `json:"number"`
	BeforePerZone int    `json:"beforePerZone"`
	AfterPerZone  int    `json:"afterPerZone"`
	Candidates    []Node `json:"preferredDrainCandidates"`
}

type Plan struct {
	SchemaVersion  string   `json:"schemaVersion"`
	Resource       Resource `json:"resource"`
	ClusterID      string   `json:"clusterID"`
	Fingerprint    string   `json:"fingerprint"`
	CurrentPerZone int      `json:"currentPerZone"`
	TargetPerZone  int      `json:"targetPerZone"`
	Zones          []string `json:"zones"`
	TotalBefore    int      `json:"totalBefore"`
	TotalAfter     int      `json:"totalAfter"`
	Batches        []Batch  `json:"batches"`
	Warnings       []string `json:"warnings"`
}

// Backend's mutating methods must use UID/resourceVersion guards where the
// provider supports them. Resize must never be retried automatically: GKE's
// setSize endpoint offers no request-id or compare-and-swap precondition.
type Backend interface {
	Snapshot(context.Context) (Snapshot, error)
	Cordon(context.Context, Node, string) error
	Uncordon(context.Context, Node, string) error
	PodsOnNode(context.Context, string) ([]Pod, error)
	Evict(context.Context, Pod) error
	Workloads(context.Context, []Workload) ([]Workload, error)
	Resize(context.Context, int) (string, error)
	WaitOperation(context.Context, string, time.Duration) error
}

type Event struct {
	Time      time.Time `json:"time"`
	Kind      string    `json:"kind"`
	Message   string    `json:"message"`
	Batch     int       `json:"batch,omitempty"`
	Node      string    `json:"node,omitempty"`
	Operation string    `json:"operation,omitempty"`
}

type Reporter func(Event)
