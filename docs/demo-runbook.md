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

