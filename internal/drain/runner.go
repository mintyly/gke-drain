package drain

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Runner executes a reviewed plan against fresh state. It never changes
// workload replicas or mutates the managed instance groups behind GKE.
type Runner struct {
	Backend Backend
	Options Options
	Report  Reporter
	RunID   string
}

func (r Runner) emit(kind, message string, batch int, node, operation string) {
	if r.Report != nil {
		r.Report(Event{Time: time.Now().UTC(), Kind: kind, Message: message, Batch: batch, Node: node, Operation: operation})
	}
}

func (r Runner) Run(ctx context.Context, initial Plan) error {
	if err := ValidateOptions(r.Options); err != nil {
		return err
	}
	if r.Backend == nil || strings.TrimSpace(r.RunID) == "" {
		return fmt.Errorf("runner requires a backend and a nonempty run ID")
	}
	if initial.SchemaVersion != planVersion || initial.Fingerprint == "" {
		return fmt.Errorf("plan is missing a supported schema version or fingerprint")
	}
	s, err := r.readSnapshot(ctx, r.Options.DrainTimeout)
	if err != nil {
		return err
	}
	fresh, err := BuildPlan(s, r.Options)
	if err != nil {
		return fmt.Errorf("preflight: %w", err)
	}
	if fresh.Fingerprint != initial.Fingerprint || fresh.Resource != initial.Resource || fresh.ClusterID != initial.ClusterID {
		return fmt.Errorf("plan is stale or belongs to a different resource; create and review a new plan before applying")
	}
	if err := sameReviewedPlan(initial, fresh); err != nil {
		return fmt.Errorf("reviewed plan does not match its fingerprint: %w", err)
	}
	reviewedBatches := append([]Batch(nil), fresh.Batches...)
	refs, _, err := requiredWorkloads(s, r.Options, nodesByName(s.Nodes))
	if err != nil {
		return err
	}
	if len(fresh.Batches) == 0 {
		r.emit("complete", "Pool is already at the requested per-zone size; no changes made.", 0, "", "")
		return nil
	}
	baseline := s
	for number := 1; ; number++ {
		if number > 1 {
			s, err = r.readSnapshot(ctx, r.Options.DrainTimeout)
			if err != nil {
				return err
			}
			if err := sameTopology(baseline, s); err != nil {
				return fmt.Errorf("pool changed between batches: %w", err)
			}
			fresh, err = BuildPlan(s, r.Options)
			if err != nil {
				return fmt.Errorf("batch %d preflight: %w", number, err)
			}
			newRefs, _, err := requiredWorkloads(s, r.Options, nodesByName(s.Nodes))
			if err != nil {
				return err
			}
			refs, err = mergeWorkloadRefs(refs, newRefs)
			if err != nil {
				return err
			}
		}
		if len(fresh.Batches) == 0 {
			r.emit("complete", fmt.Sprintf("Verified %d nodes per zone across %d zones.", fresh.CurrentPerZone, len(fresh.Zones)), number-1, "", "")
			return nil
		}
		if number > len(reviewedBatches) {
			return fmt.Errorf("pool still requires reduction after the reviewed batches were exhausted")
		}
		batch := reviewedBatches[number-1]
		if fresh.CurrentPerZone != batch.BeforePerZone {
			return fmt.Errorf("reviewed batch %d expected %d nodes per zone, observed %d", number, batch.BeforePerZone, fresh.CurrentPerZone)
		}
		batch.Candidates = append([]Node(nil), batch.Candidates...)
		currentNodes := nodesByName(s.Nodes)
		for i, planned := range batch.Candidates {
			current, exists := currentNodes[planned.Name]
			if !exists || current.UID != planned.UID || current.ProviderID != planned.ProviderID {
				return fmt.Errorf("reviewed candidate %s disappeared or changed identity; create a new plan", planned.Name)
			}
			batch.Candidates[i] = current
		}
		r.emit("batch", fmt.Sprintf("Preparing reduction from %d to %d nodes per zone.", batch.BeforePerZone, batch.AfterPerZone), number, "", "")
		baseline, err = r.runBatch(ctx, s, batch, refs)
		if err != nil {
			return fmt.Errorf("batch %d: %w", number, err)
		}
	}
}

func (r Runner) runBatch(ctx context.Context, before Snapshot, batch Batch, refs []Workload) (Snapshot, error) {
	owned := make([]Node, 0, len(batch.Candidates))
	failBeforeResize := func(err error) (Snapshot, error) {
		return Snapshot{}, errors.Join(err, r.restoreCordons(owned, batch.Number))
	}
	preCtx, cancelPre := context.WithTimeout(ctx, r.Options.DrainTimeout)
	defer cancelPre()
	for _, n := range batch.Candidates {
		// A PATCH response can be lost after commit. Include the candidate before
		// calling Cordon; ownership guards make cleanup safe in either outcome.
		owned = append(owned, n)
		if err := r.Backend.Cordon(preCtx, n, r.RunID); err != nil {
			return failBeforeResize(fmt.Errorf("cordon %s: %w", n.Name, err))
		}
		r.emit("cordon", "Node cordoned with a UID-guarded ownership annotation.", batch.Number, n.Name, "")
	}
	if err := r.drainPods(preCtx, owned, refs, batch.Number); err != nil {
		return failBeforeResize(fmt.Errorf("drain stopped before any resize was submitted: %w", err))
	}
	if err := r.waitHealthy(preCtx, refs); err != nil {
		return failBeforeResize(fmt.Errorf("workload recovery stopped before resize: %w", err))
	}
	latest, err := r.readSnapshot(preCtx, r.Options.DrainTimeout)
	if err != nil {
		return failBeforeResize(err)
	}
	if err := sameTopology(before, latest); err != nil {
		return failBeforeResize(fmt.Errorf("pool changed during drain: %w", err))
	}
	clean, err := normalizeOwnedCordons(latest, owned, r.RunID, true)
	if err != nil {
		return failBeforeResize(err)
	}
	if _, err := BuildPlan(clean, r.Options); err != nil {
		return failBeforeResize(fmt.Errorf("final pre-resize preflight: %w", err))
	}
	if err := checkKnownWorkloadState(refs, latest.Workloads); err != nil {
		return failBeforeResize(err)
	}
	knownControllers := make(map[string]bool)
	for _, w := range refs {
		knownControllers[w.UID] = true
	}
	latestNodes := nodesByName(latest.Nodes)
	for _, p := range latest.Pods {
		if _, inPool := latestNodes[p.Node]; inPool && !p.Ignored() && !knownControllers[p.OwnerUID] {
			return failBeforeResize(fmt.Errorf("new controller appeared for pod %s during drain; replan before resizing", p.Key()))
		}
	}
	candidateSet := nodesByName(owned)
	for _, p := range latest.Pods {
		if _, candidate := candidateSet[p.Node]; candidate && !p.Ignored() {
			return failBeforeResize(fmt.Errorf("pod %s appeared or remains on prepared node %s; no resize submitted", p.Key(), p.Node))
		}
	}
	if err := preCtx.Err(); err != nil {
		return failBeforeResize(err)
	}
	cancelPre()

	opCtx, cancelOp := context.WithTimeout(ctx, r.Options.OperationTimeout)
	defer cancelOp()
	if err := opCtx.Err(); err != nil {
		return failBeforeResize(err)
	}
	r.emit("resize", fmt.Sprintf("Submitting one GKE setSize request for %d nodes per zone.", batch.AfterPerZone), batch.Number, "", "")
	operation, err := r.Backend.Resize(opCtx, batch.AfterPerZone)
	if err != nil {
		return Snapshot{}, r.uncertain(err, operation, owned, batch.Number)
	}
	if operation == "" {
		return Snapshot{}, r.uncertain(fmt.Errorf("GKE returned no operation name"), operation, owned, batch.Number)
	}
	r.emit("operation", "Resize accepted; waiting for GKE operation completion.", batch.Number, "", operation)
	if err := r.Backend.WaitOperation(opCtx, operation, r.Options.PollInterval); err != nil {
		return Snapshot{}, r.uncertain(err, operation, owned, batch.Number)
	}
	r.emit("operation-complete", "GKE operation completed; verifying actual pool membership.", batch.Number, "", operation)

	after, err := r.waitConverged(opCtx, before, owned, batch.AfterPerZone)
	if err != nil {
		return Snapshot{}, errors.Join(fmt.Errorf("operation %s completed but pool convergence could not be verified: %w", operation, err), r.restoreCordons(owned, batch.Number))
	}
	mismatch := verifyVictims(before, after, owned)
	cleanupErr := r.restoreCordons(owned, batch.Number)
	if mismatch != nil || cleanupErr != nil {
		if mismatch != nil {
			r.emit("victim-mismatch", mismatch.Error()+" No further resize batches will be submitted.", batch.Number, "", operation)
		}
		return Snapshot{}, errors.Join(mismatch, cleanupErr)
	}

	healthCtx, cancelHealth := context.WithTimeout(ctx, r.Options.DrainTimeout)
	defer cancelHealth()
	if err := r.waitHealthy(healthCtx, refs); err != nil {
		return Snapshot{}, fmt.Errorf("resize completed but post-resize workload health was not verified: %w", err)
	}
	verified, err := r.readSnapshot(healthCtx, r.Options.DrainTimeout)
	if err != nil {
		return Snapshot{}, err
	}
	if err := sameTopology(after, verified); err != nil {
		return Snapshot{}, fmt.Errorf("pool changed after resize verification: %w", err)
	}
	if _, err := BuildPlan(verified, r.Options); err != nil {
		return Snapshot{}, fmt.Errorf("post-resize preflight: %w", err)
	}
	r.emit("batch-complete", fmt.Sprintf("Verified removal of the prepared nodes and recovery at %d nodes per zone.", batch.AfterPerZone), batch.Number, "", operation)
	return verified, nil
}

func (r Runner) uncertain(cause error, operation string, owned []Node, batch int) error {
	names := make([]string, 0, len(owned))
	for _, n := range owned {
		names = append(names, n.Name)
	}
	sort.Strings(names)
	message := fmt.Sprintf("resize outcome is uncertain; retained owned cordons on [%s] for run %s. Inspect GKE operation %q and actual pool membership before recovery; do not blindly rerun apply", strings.Join(names, ", "), r.RunID, operation)
	r.emit("recovery-required", message, batch, "", operation)
	return fmt.Errorf("%s: %w", message, cause)
}

func (r Runner) readSnapshot(ctx context.Context, timeout time.Duration) (Snapshot, error) {
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if err := readCtx.Err(); err != nil {
			return Snapshot{}, err
		}
		s, err := r.Backend.Snapshot(readCtx)
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, ErrRetryable) {
			return Snapshot{}, fmt.Errorf("read pool snapshot: %w", err)
		}
		if err := pause(readCtx, r.Options.PollInterval); err != nil {
			return Snapshot{}, err
		}
	}
}

func (r Runner) drainPods(ctx context.Context, nodes []Node, refs []Workload, batch int) error {
	controllers := make(map[string]Workload)
	for _, w := range refs {
		controllers[w.UID] = w
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pending := false
		for _, n := range nodes {
			pods, err := r.Backend.PodsOnNode(ctx, n.Name)
			if errors.Is(err, ErrRetryable) {
				pending = true
				continue
			}
			if err != nil {
				return fmt.Errorf("list pods on %s: %w", n.Name, err)
			}
			for _, p := range pods {
				if p.Ignored() {
					continue
				}
				if p.Node != n.Name {
					return fmt.Errorf("pod listing for %s contains a pod assigned to another node", n.Name)
				}
				if err := validatePod(p, r.Options); err != nil {
					return err
				}
				w, known := controllers[p.OwnerUID]
				if !known || w.Namespace != p.Namespace || w.Kind != p.OwnerKind || w.Name != p.OwnerName {
					return fmt.Errorf("new or changed controller for pod %s; replan before draining", p.Key())
				}
				pending = true
				if p.Terminating {
					continue
				}
				err = r.Backend.Evict(ctx, p)
				if err == nil {
					r.emit("eviction", "Kubernetes accepted a UID-guarded policy/v1 eviction.", batch, n.Name, "")
					continue
				}
				if errors.Is(err, ErrRetryable) {
					r.emit("eviction-blocked", "Eviction was rate limited or blocked by a disruption budget; waiting within the drain deadline.", batch, n.Name, "")
					continue
				}
				if !errors.Is(err, ErrNotFound) {
					return fmt.Errorf("evict %s: %w", p.Key(), err)
				}
			}
		}
		if !pending {
			return nil
		}
		if err := pause(ctx, r.Options.PollInterval); err != nil {
			return err
		}
	}
}

func (r Runner) waitHealthy(ctx context.Context, refs []Workload) error {
	if len(refs) == 0 {
		return ctx.Err()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		actual, err := r.Backend.Workloads(ctx, refs)
		if err != nil && !errors.Is(err, ErrRetryable) {
			return fmt.Errorf("read workload health: %w", err)
		}
		if err == nil {
			ready, err := workloadsHealthy(refs, actual)
			if err != nil {
				return err
			}
			if ready {
				return nil
			}
		}
		if err := pause(ctx, r.Options.PollInterval); err != nil {
			return fmt.Errorf("waiting for workload replicas to recover: %w", err)
		}
	}
}

func workloadsHealthy(expected, actual []Workload) (bool, error) {
	byName := make(map[string]Workload)
	for _, w := range actual {
		key := workloadIdentity(w)
		if _, exists := byName[key]; exists {
			return false, fmt.Errorf("duplicate workload identity %s", key)
		}
		byName[key] = w
	}
	ready := true
	for _, want := range expected {
		got, exists := byName[workloadIdentity(want)]
		if !exists || got.UID != want.UID || got.Desired != want.Desired || got.Generation != want.Generation {
			return false, fmt.Errorf("workload %s changed UID, desired replicas, or generation during the run", workloadIdentity(want))
		}
		if got.Ready < got.Desired || got.ObservedGeneration < got.Generation {
			ready = false
		}
	}
	return ready, nil
}

// Snapshots can omit old controllers once all of their pods have left this
// pool. Check every controller still present; waitHealthy separately fetches
// all initial references, including controllers now running elsewhere.
func checkKnownWorkloadState(expected, actual []Workload) error {
	wanted := make(map[string]Workload)
	for _, w := range expected {
		wanted[workloadIdentity(w)] = w
	}
	for _, w := range actual {
		if original, exists := wanted[workloadIdentity(w)]; exists {
			if w.UID != original.UID || w.Desired != original.Desired || w.Generation != original.Generation {
				return fmt.Errorf("workload %s changed while preparing the resize", workloadIdentity(w))
			}
		}
	}
	return nil
}

func mergeWorkloadRefs(old, current []Workload) ([]Workload, error) {
	if err := checkKnownWorkloadState(old, current); err != nil {
		return nil, err
	}
	result := append([]Workload(nil), old...)
	known := make(map[string]bool)
	for _, w := range old {
		known[w.UID] = true
	}
	for _, w := range current {
		if !known[w.UID] {
			result = append(result, w)
		}
	}
	sort.Slice(result, func(i, j int) bool { return workloadIdentity(result[i]) < workloadIdentity(result[j]) })
	return result, nil
}

func (r Runner) waitConverged(ctx context.Context, before Snapshot, owned []Node, target int) (Snapshot, error) {
	var lastErr error
	for {
		s, err := r.Backend.Snapshot(ctx)
		if err == nil {
			if s.Resource != before.Resource || s.ClusterID != before.ClusterID {
				return Snapshot{}, fmt.Errorf("resource identity changed after the resize")
			}
			clean, normalizeErr := normalizeOwnedCordons(s, owned, r.RunID, false)
			if normalizeErr == nil {
				t, topologyErr := validateTopology(clean)
				if topologyErr == nil && t.current == target && sameStrings(t.zones, before.Locations) {
					return s, nil
				}
				if topologyErr != nil {
					lastErr = topologyErr
				} else {
					lastErr = fmt.Errorf("observed per-zone size %d, expected %d", t.current, target)
				}
			} else {
				lastErr = normalizeErr
			}
		} else {
			if !errors.Is(err, ErrRetryable) {
				return Snapshot{}, err
			}
			lastErr = err
		}
		if err := pause(ctx, r.Options.PollInterval); err != nil {
			return Snapshot{}, errors.Join(err, lastErr)
		}
	}
}

func normalizeOwnedCordons(s Snapshot, owned []Node, runID string, requireAll bool) (Snapshot, error) {
	copy := s
	copy.Nodes = append([]Node(nil), s.Nodes...)
	expected := nodesByName(owned)
	found := 0
	for i, n := range copy.Nodes {
		want, exists := expected[n.Name]
		if !exists {
			continue
		}
		if n.UID != want.UID || n.ProviderID != want.ProviderID || n.Owner != runID || !n.Unschedulable {
			return Snapshot{}, fmt.Errorf("prepared node %s changed identity or drain ownership", n.Name)
		}
		found++
		copy.Nodes[i].Unschedulable, copy.Nodes[i].Owner = false, ""
	}
	if requireAll && found != len(owned) {
		return Snapshot{}, fmt.Errorf("a prepared node disappeared before resize submission")
	}
	return copy, nil
}

func (r Runner) restoreCordons(owned []Node, batch int) error {
	// Cleanup is deliberately detached from the canceled operation context.
	// Backend.Uncordon must reread and verify UID and our ownership annotation;
	// it must not clear a cordon owned by another operator.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var failures []error
	for _, n := range owned {
		if err := r.Backend.Uncordon(ctx, n, r.RunID); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			failures = append(failures, fmt.Errorf("restore owned cordon on %s: %w", n.Name, err))
			continue
		}
		r.emit("uncordon", "Removed this run's cordon if the original node still exists and remains owned by this run.", batch, n.Name, "")
	}
	if len(failures) > 0 {
		r.emit("recovery-required", "Some owned cordons could not be restored; inspect nodes with annotation "+OwnershipAnnotation+"="+r.RunID+".", batch, "", "")
	}
	return errors.Join(failures...)
}

func nodesByName(nodes []Node) map[string]Node {
	result := make(map[string]Node, len(nodes))
	for _, n := range nodes {
		result[n.Name] = n
	}
	return result
}

func sameReviewedPlan(reviewed, fresh Plan) error {
	if reviewed.CurrentPerZone != fresh.CurrentPerZone || reviewed.TargetPerZone != fresh.TargetPerZone || reviewed.TotalBefore != fresh.TotalBefore || reviewed.TotalAfter != fresh.TotalAfter || !sameStrings(reviewed.Zones, fresh.Zones) || len(reviewed.Batches) != len(fresh.Batches) {
		return fmt.Errorf("size, zones, or number of batches was changed")
	}
	for i, expected := range fresh.Batches {
		got := reviewed.Batches[i]
		if got.Number != expected.Number || got.BeforePerZone != expected.BeforePerZone || got.AfterPerZone != expected.AfterPerZone || len(got.Candidates) != len(expected.Candidates) {
			return fmt.Errorf("batch %d was changed", i+1)
		}
		for j, candidate := range expected.Candidates {
			other := got.Candidates[j]
			if candidate.Name != other.Name || candidate.UID != other.UID || candidate.ProviderID != other.ProviderID || candidate.Zone != other.Zone {
				return fmt.Errorf("candidate selection in batch %d was changed", i+1)
			}
		}
	}
	return nil
}

func sameTopology(before, after Snapshot) error {
	if before.Resource != after.Resource || before.ClusterID != after.ClusterID || !sameStrings(before.Locations, after.Locations) {
		return fmt.Errorf("cluster or pool identity changed")
	}
	groups := make(map[string]Group)
	for _, g := range before.Groups {
		groups[g.URL] = g
	}
	if len(groups) != len(after.Groups) {
		return fmt.Errorf("managed instance group membership changed")
	}
	for _, g := range after.Groups {
		old, exists := groups[g.URL]
		if !exists || old.Zone != g.Zone || old.TargetSize != g.TargetSize || !sameStrings(old.Instances, g.Instances) {
			return fmt.Errorf("managed instance group target or instance membership changed in zone %s", g.Zone)
		}
	}
	oldNodes := nodesByName(before.Nodes)
	if len(oldNodes) != len(after.Nodes) {
		return fmt.Errorf("node count changed")
	}
	for _, n := range after.Nodes {
		old, exists := oldNodes[n.Name]
		if !exists || old.UID != n.UID || old.ProviderID != n.ProviderID || old.Zone != n.Zone {
			return fmt.Errorf("node identity changed for %s", n.Name)
		}
	}
	return nil
}

func verifyVictims(before, after Snapshot, candidates []Node) error {
	oldNodes, newNodes := nodesByName(before.Nodes), nodesByName(after.Nodes)
	expected := make(map[string]bool)
	for _, n := range candidates {
		expected[n.Name] = true
	}
	var unexpected, survived []string
	for _, n := range before.Nodes {
		current, remains := newNodes[n.Name]
		if remains && (current.UID != n.UID || current.ProviderID != n.ProviderID) {
			return fmt.Errorf("GKE replaced node identity %s instead of only removing prepared nodes; stopping", n.Name)
		}
		if !remains && !expected[n.Name] {
			unexpected = append(unexpected, n.Name)
		}
		if remains && expected[n.Name] {
			survived = append(survived, n.Name)
		}
	}
	for _, n := range after.Nodes {
		if _, exists := oldNodes[n.Name]; !exists {
			return fmt.Errorf("unexpected new node %s appeared during resize; stopping", n.Name)
		}
	}
	if len(unexpected) != 0 || len(survived) != 0 {
		sort.Strings(unexpected)
		sort.Strings(survived)
		return fmt.Errorf("GKE selected different victims: unprepared nodes removed [%s], prepared nodes surviving [%s]; surviving owned cordons were scheduled for restoration; stopping", strings.Join(unexpected, ", "), strings.Join(survived, ", "))
	}
	return nil
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aCopy, bCopy := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(aCopy)
	sort.Strings(bCopy)
	for i := range aCopy {
		if aCopy[i] != bCopy[i] {
			return false
		}
	}
	return true
}

func pause(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
