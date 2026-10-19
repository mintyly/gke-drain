# Validation record

Validation date: 22 October 2026. Toolchain: Go 1.26.10, Linux amd64.

This record covers local code checks and simulated control planes. No live GKE
cluster was accessed or resized during this validation.

## Checks executed

| Check | Result |
| --- | --- |
| `gofmt -l .` | Passed; no unformatted files |
| `go mod verify` | Passed; all modules verified |
| `go vet ./...` | Passed |
| `go test -race -coverprofile=coverage.out ./...` | Passed |
| `go build -buildvcs=false -trimpath -o gke-drain ./cmd/gke-drain` | Passed |
| Built executable: `--version` | `gke-drain 0.1.0-dev` |
| Built executable: `--demo --target-per-zone=1 --output=json` | Valid JSON; two batches; six nodes to two total |

The build above disables VCS stamping for reproducible source-archive validation.
The repository also includes a CI workflow for running checks from a Git checkout;
this record does not assert that a remote GitHub Actions run has occurred.

There are **46 top-level test functions**, including table-driven subtests. The
statement coverage reported by the command above was:

| Package | Statement coverage |
| --- | ---: |
| `cmd/gke-drain` | 82.0% |
| `internal/cloud` | 75.1% |
| `internal/drain` | 84.8% |
| `internal/demo` | 0.0% |
| Total | 79.7% |

The demo fixture is exercised by CLI tests and the built-executable smoke check,
but Go's default per-package coverage instrumentation does not attribute those
cross-package calls to `internal/demo`. Coverage is a local statement metric,
not an availability or production-safety guarantee.

## Behaviours exercised

- Balanced regional batching, including a short final batch and an already-met target.
- Rejection of unsafe configuration, wrong resource membership, stale identities,
  enabled autoscaling, active operations, local data and unsupported workloads.
- Deterministic planning, unchanged input snapshots and state fingerprints.
- Retaining the reviewed candidate identities when Pod redistribution changes
  the ranking that a new plan would produce.
- UID-guarded node patches and Pod evictions; cleanup of only this run's cordons.
- PDB rejection, graceful termination waits, workload readiness and state drift.
- Cancellation before resize, failed cleanup, ambiguous resize submission and
  uncertain operation polling.
- Pagination, API errors, operation scope, credential-destination restrictions,
  and OAuth refresh after the run context is canceled.
- Integrated planner + executor + REST adapter against simulated GKE, Compute,
  and Kubernetes APIs: successful reduction, blocked eviction, and unexpected
  GKE victim selection.

## Remaining validation

Run the [sandbox demonstration](demo-runbook.md) before claiming live GKE
compatibility or operational impact. In particular, validate ADC/IAM/RBAC in the
target project, real control-plane connectivity, provider operation responses,
actual node selection, application readiness and request-level availability.
No cost savings, zero-downtime claim, production use, or real-cloud benchmark is
established by these tests.
