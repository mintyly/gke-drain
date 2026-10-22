# gke-drain

Controlled scale-downs of GKE Standard node pools, in Go. Picks drain candidates, evicts their workloads through the Kubernetes eviction API, shrinks the pool through the GKE API, then checks what GKE actually removed before the next batch. Replica counts don't change — this shrinks node capacity, not application capacity. Google Cloud port of [ecs-drain](https://github.com/mintyly/ecs-drain).

**Status:** tested offline and against a simulated API, never in production. See the [validation record](docs/validation.md), [demo runbook](docs/demo-runbook.md) and [design notes](docs/design.md).

## What GKE does not guarantee

GKE prefers cordoned and drained nodes when it resizes, but does not guarantee which nodes it removes. Candidates are a preference, nothing stronger. If the removals differ, `gke-drain` restores the cordons it owns and stops.

Its own evictions use `policy/v1` and never fall back to deleting Pods. Once the resize is submitted GKE runs its own drain, [documented](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/resizing-a-cluster) as honouring PDBs and termination grace for up to one hour. There is no zero-downtime promise here.

## Demo

Go 1.26+. Plans only — rejects `--execute`, never contacts Google Cloud.

```sh
go run ./cmd/gke-drain --demo --target-per-zone 2
```

Three nodes in each of two zones, so a target of two takes six to four.

## Real use

```sh
gcloud auth application-default login
make build
./bin/gke-drain --project=my-project --location=asia-southeast1 \
  --cluster=drain-lab --node-pool=workers --target-per-zone=2 --batch-per-zone=1
```

Without `--execute` it reads state and prints a plan, mutating nothing. With it, state is refetched and checked against the plan you saw before the first mutation; later batches revalidate against the candidate UIDs you reviewed. A saved JSON plan is for reading, not an approval token.

Target and batch are both per zone — `--batch-per-zone=1` on a three-zone pool evacuates three nodes in a batch. Plan goes to stdout, progress and errors to stderr, both as JSON under `--output=json`. Full flags and defaults in `--help`.

`gke-drain` takes the cluster endpoint and CA from GKE and verifies TLS itself, ignoring your kubeconfig. DNS-only control-plane endpoints are out of scope, as are Autopilot clusters, direct MIG or VM mutation, replica scaling, and storage migration.

## What it checks first

Cluster is Standard; pool is neither autoscaled nor mid-operation; one stable managed instance group per zone with equal target sizes; Kubernetes nodes matching real VM membership; nodes Ready, schedulable and unclaimed by another run; Pods under healthy `ReplicaSet`, `ReplicationController` or `StatefulSet` owners.

It refuses host-mounted local data, active Jobs, custom controller types and unmanaged Pods. `emptyDir` and PVCs need an explicit opt-in — and that opt-in doesn't prove a volume can actually move, so check zone affinity and attachment limits yourself. Each resize goes out once; an uncertain outcome stops the run instead of retrying.

## Permissions

**IAM:** `container.clusters.get`, `container.operations.list`, `container.operations.get`, `compute.instanceGroupManagers.get`, plus `container.clusters.update` to execute (omit it for a planning-only identity). A custom role built from these still applies project-wide, not per-cluster.

**Kubernetes:** [examples/rbac.yaml](examples/rbac.yaml) has node read/patch, Pod read/eviction and controller read. It binds nothing — bind the ADC identity yourself. No Pod delete, no Secrets, no workload scaling, but it does need cluster-wide Pod visibility.

## Recovery

Killing the CLI doesn't stop a resize GKE already accepted. On an uncertain result `gke-drain` keeps its cordons and prints the context — check GKE operations and the real pool state before rerunning or uncordoning. [Failure matrix](docs/design.md#failure-and-recovery).
