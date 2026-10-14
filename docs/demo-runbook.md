# Demo runbook

This runbook separates reproducible local checks from a later live-cloud experiment. The commands below have not, by themselves, established that the tool works in a real GKE cluster. Record actual results before making that claim.

## 1. Offline demonstration

From the repository root:

```sh
make check
make build
./bin/gke-drain --demo --target-per-zone=2
./bin/gke-drain --demo --target-per-zone=1 --output=json > demo-plan.json
```

Explain three observable properties:

- The fixture starts at three nodes per zone in two zones, so a target of two leaves four nodes total.
- A target of one with the default batch size produces two batches, each reducing one node per zone.
- Adding `--execute` to a demo command is refused. Offline planning cannot change cloud resources.

The JSON includes preferred candidates and a state fingerprint. Candidate names describe the fixture and are not evidence of a live cluster.

## 2. Prepare a disposable GKE Standard sandbox

The following sections are an **optional paid-cloud experiment**. Use a dedicated project/cluster and review its current pricing and quotas. This runbook does not create a cluster automatically.

Start with:

- A GKE Standard cluster and a manually sized node pool named `workers`.
- Three Ready, schedulable nodes per pool zone and adequate capacity at the eventual target of two per zone.
- Autoscaling disabled for this pool, no ongoing upgrade, and no concurrent maintenance or deployment.
- ADC for the intended operator, the IAM permissions in the README, and Kubernetes RBAC for that same identity.
- Network access to the cluster's IP endpoint. Use an explicit kubeconfig context for all manual `kubectl` commands.

Confirm the target before applying anything:

```sh
gcloud container node-pools describe workers \
  --project=PROJECT --location=LOCATION --cluster=CLUSTER

kubectl --context=EXPECTED_CONTEXT get nodes \
  -l cloud.google.com/gke-nodepool=workers -o wide
```

[examples/rbac.yaml](../examples/rbac.yaml) defines a role only. Have an administrator review and bind it to the correct operator identity; it is not a ready-made blanket grant.

## 3. Deploy the disposable workload

The example is three nginx replicas, an HTTP readiness probe, modest resource requests, and a PDB allowing one unavailable replica. It uses no persistent or host-local storage and selects the `workers` pool.

```sh
kubectl --context=EXPECTED_CONTEXT apply -f examples/demo-workload.yaml
kubectl --context=EXPECTED_CONTEXT -n gke-drain-demo \
  rollout status deployment/drain-demo --timeout=180s
kubectl --context=EXPECTED_CONTEXT -n gke-drain-demo get pods -o wide
kubectl --context=EXPECTED_CONTEXT -n gke-drain-demo get pdb
```

Check that the application Pods actually occupy a planned candidate. The planner prefers lightly loaded nodes; an empty candidate demonstrates capacity reduction but not application evacuation. Use the displayed plan to choose an appropriate test state rather than claiming that every run moves a demo Pod.

Other pool workloads also undergo preflight. Resolve a rejection on its merits; do not turn on storage opt-ins merely to make the demonstration pass.

## 4. Inspect and execute one reduction

Authenticate ADC if needed, then create a plan:

```sh
gcloud auth application-default login

./bin/gke-drain \
  --project=PROJECT --location=LOCATION --cluster=CLUSTER \
  --node-pool=workers --target-per-zone=2 --output=json \
  > live-plan.json
```

Read the resource identity, zones, totals, candidates, and warnings. The next command produces a fresh plan and checks live state; it does not consume the saved JSON file.

```sh
./bin/gke-drain \
  --project=PROJECT --location=LOCATION --cluster=CLUSTER \
  --node-pool=workers --target-per-zone=2 --output=json --execute \
  > live-run-plan.json 2> live-events.jsonl
```

This command opts into mutations. `live-run-plan.json` contains the checked plan, including candidate UIDs; `live-events.jsonl` contains progress or error events and the operation identifier when one was returned. Keep both files and the process exit status.

Observe the run from another terminal:

```sh
kubectl --context=EXPECTED_CONTEXT -n gke-drain-demo get pods -o wide --watch
```

After completion, compare old and new Node UIDs, each zone's node count, the reported operation, and deployment readiness. If GKE removes different nodes from the candidates, record the mismatch and the tool's stop behaviour as the result.

An independent request probe through your chosen test traffic path is needed for an availability measurement. Pod readiness alone does not establish that no request failed.

