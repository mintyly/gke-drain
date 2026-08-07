package drain

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type fakeBackend struct {
	snapshot        Snapshot
	resizeTargets   []int
	cordoned        []string
	uncordoned      []string
	evictionCalls   int
	snapshotCalls   int
	listCalls       int
	healthCalls     int
	pendingTarget   int
	evictErr        error
	resizeErr       error
	waitErr         error
	uncordonErr     error
	wrongVictim     bool
	onSnapshot      func(*fakeBackend)
	onCordon        func(*fakeBackend, Node)
	onEvict         func(*fakeBackend, Pod)
	onList          func(*fakeBackend)
	onHealth        func(*fakeBackend)
	onWait          func(*fakeBackend)
	evictOverride   func(*fakeBackend, Pod) error
	cleanupCanceled bool
}

func (f *fakeBackend) Snapshot(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	f.snapshotCalls++
	if f.onSnapshot != nil {
		f.onSnapshot(f)
	}
	return cloneSnapshot(f.snapshot), nil
}

func (f *fakeBackend) Cordon(ctx context.Context, n Node, owner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for i, got := range f.snapshot.Nodes {
		if got.Name == n.Name {
			if got.UID != n.UID || got.ResourceVersion != n.ResourceVersion || got.Unschedulable || got.Owner != "" {
				return errors.New("cordon precondition failed")
			}
			f.snapshot.Nodes[i].Unschedulable = true
			f.snapshot.Nodes[i].Owner = owner
			f.snapshot.Nodes[i].ResourceVersion += "1"
			f.cordoned = append(f.cordoned, n.Name)
			if f.onCordon != nil {
				f.onCordon(f, n)
			}
			return nil
		}
	}
	return ErrNotFound
}

func (f *fakeBackend) Uncordon(ctx context.Context, n Node, owner string) error {
	if ctx.Err() != nil {
		f.cleanupCanceled = true
		return ctx.Err()
	}
	if f.uncordonErr != nil {
		return f.uncordonErr
	}
	for i, got := range f.snapshot.Nodes {
		if got.Name == n.Name {
			if got.UID != n.UID || got.Owner != owner {
				return nil
			}
			f.snapshot.Nodes[i].Unschedulable = false
			f.snapshot.Nodes[i].Owner = ""
			f.snapshot.Nodes[i].ResourceVersion += "1"
			f.uncordoned = append(f.uncordoned, n.Name)
			return nil
		}
	}
	return ErrNotFound
}

func (f *fakeBackend) PodsOnNode(ctx context.Context, node string) ([]Pod, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.listCalls++
	if f.onList != nil {
		f.onList(f)
	}
	var pods []Pod
	for _, p := range f.snapshot.Pods {
		if p.Node == node {
			pods = append(pods, p)
		}
	}
	return pods, nil
}

func (f *fakeBackend) Evict(ctx context.Context, pod Pod) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.evictionCalls++
	if f.evictOverride != nil {
		return f.evictOverride(f, pod)
	}
	if f.evictErr != nil {
		return f.evictErr
	}
	for i, p := range f.snapshot.Pods {
		if p.UID == pod.UID {
			f.snapshot.Pods = append(f.snapshot.Pods[:i], f.snapshot.Pods[i+1:]...)
			// Simulate the controller replacing its evicted pod on a surviving
			// schedulable node. Replacement identity and pod distribution change.
			for _, n := range f.snapshot.Nodes {
				if !n.Unschedulable {
					p.UID += "-replacement"
					p.Node = n.Name
					f.snapshot.Pods = append(f.snapshot.Pods, p)
					break
				}
			}
			if f.onEvict != nil {
				f.onEvict(f, pod)
			}
			return nil
		}
	}
	return ErrNotFound
}

func (f *fakeBackend) Workloads(ctx context.Context, _ []Workload) ([]Workload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.healthCalls++
	if f.onHealth != nil {
		f.onHealth(f)
	}
	return append([]Workload(nil), f.snapshot.Workloads...), nil
}

func (f *fakeBackend) Resize(ctx context.Context, target int) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.resizeTargets = append(f.resizeTargets, target)
	f.pendingTarget = target
	if f.resizeErr != nil {
		return "", f.resizeErr
	}
	return fmt.Sprintf("operation-%d", len(f.resizeTargets)), nil
}

func (f *fakeBackend) WaitOperation(ctx context.Context, _ string, _ time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.waitErr != nil {
		return f.waitErr
	}
	remove := make(map[string]bool)
	for _, g := range f.snapshot.Groups {
		var candidates []Node
		for _, n := range f.snapshot.Nodes {
			if n.Zone == g.Zone && (n.Owner != "") != f.wrongVictim {
				candidates = append(candidates, n)
			}
		}
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })
		count := g.TargetSize - f.pendingTarget
		if len(candidates) < count {
			return errors.New("mock has insufficient resize victims")
		}
		for _, n := range candidates[:count] {
			remove[n.Name] = true
		}
	}
	var kept []Node
	for _, n := range f.snapshot.Nodes {
		if !remove[n.Name] {
			kept = append(kept, n)
		}
	}
	f.snapshot.Nodes = kept
	for i := range f.snapshot.Groups {
		g := &f.snapshot.Groups[i]
		g.TargetSize = f.pendingTarget
		g.Instances = nil
		for _, n := range kept {
			if n.Zone == g.Zone {
				g.Instances = append(g.Instances, n.ProviderID)
			}
		}
	}
	for i, p := range f.snapshot.Pods {
		if remove[p.Node] {
			f.snapshot.Pods[i].Node = kept[len(kept)-1].Name
			f.snapshot.Pods[i].UID += "-gke-replacement"
		}
	}
	if f.onWait != nil {
		f.onWait(f)
	}
	return nil
}

func runFake(t *testing.T, f *fakeBackend, opts Options) error {
	t.Helper()
	p, err := BuildPlan(cloneSnapshot(f.snapshot), opts)
	if err != nil {
		t.Fatal(err)
	}
	return (Runner{Backend: f, Options: opts, RunID: "test-run"}).Run(context.Background(), p)
}

func assertNoOwnedCordons(t *testing.T, f *fakeBackend) {
	t.Helper()
	for _, n := range f.snapshot.Nodes {
		if n.Unschedulable || n.Owner != "" {
			t.Fatalf("node %s was left cordoned: %#v", n.Name, n)
		}
	}
	if f.cleanupCanceled {
		t.Fatal("cleanup reused an already canceled context")
	}
}

func TestRunnerRegionalFinalShortBatch(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(2, 6, true)}
	if err := runFake(t, f, testOptions(1, 2)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.resizeTargets, []int{4, 2, 1}) || len(f.snapshot.Nodes) != 2 {
		t.Fatalf("wrong final batch/count: targets=%v nodes=%d", f.resizeTargets, len(f.snapshot.Nodes))
	}
	assertNoOwnedCordons(t, f)
}

func TestRunnerRejectsStalePlanBeforeMutation(t *testing.T) {
	f, opts := &fakeBackend{snapshot: testSnapshot(1, 3, true)}, testOptions(1, 1)
	p, err := BuildPlan(f.snapshot, opts)
	if err != nil {
		t.Fatal(err)
	}
	f.snapshot.Nodes[0].UID = "different-node-uid"
	err = (Runner{Backend: f, Options: opts, RunID: "test-run"}).Run(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), "stale") || len(f.cordoned) != 0 || len(f.resizeTargets) != 0 {
		t.Fatalf("stale plan was not rejected before mutations: %v", err)
	}
}

func TestRunnerRejectsTamperedCandidateList(t *testing.T) {
	f, opts := &fakeBackend{snapshot: testSnapshot(1, 3, false)}, testOptions(1, 1)
	p, err := BuildPlan(f.snapshot, opts)
	if err != nil {
		t.Fatal(err)
	}
	p.Batches[0].Candidates[0] = f.snapshot.Nodes[2]
	err = (Runner{Backend: f, Options: opts, RunID: "test-run"}).Run(context.Background(), p)
	if err == nil || len(f.cordoned) != 0 {
		t.Fatalf("tampered plan was not rejected: %v", err)
	}
}

func TestRunnerAlreadyAtTargetMakesNoMutation(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(2, 3, true)}
	if err := runFake(t, f, testOptions(3, 1)); err != nil {
		t.Fatal(err)
	}
	if len(f.cordoned) != 0 || len(f.resizeTargets) != 0 || f.evictionCalls != 0 {
		t.Fatal("no-op run made mutations")
	}
}

func TestRunnerPDBBlockNeverSubmitsResize(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 3, true), evictErr: fmt.Errorf("HTTP 429: %w", ErrRetryable)}
	o := testOptions(1, 1)
	o.DrainTimeout = 15 * time.Millisecond
	err := runFake(t, f, o)
	if !errors.Is(err, context.DeadlineExceeded) || len(f.resizeTargets) != 0 || f.evictionCalls < 2 {
		t.Fatalf("blocked eviction did not fail closed: err=%v resizes=%v evictions=%d", err, f.resizeTargets, f.evictionCalls)
	}
	assertNoOwnedCordons(t, f)
}

func TestRunnerCancellationUsesIndependentCleanupContext(t *testing.T) {
	f, o := &fakeBackend{snapshot: testSnapshot(1, 3, true)}, testOptions(1, 1)
	p, err := BuildPlan(f.snapshot, o)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.onEvict = func(_ *fakeBackend, _ Pod) { cancel() }
	err = (Runner{Backend: f, Options: o, RunID: "test-run"}).Run(ctx, p)
	if !errors.Is(err, context.Canceled) || len(f.resizeTargets) != 0 {
		t.Fatalf("cancellation failed closed incorrectly: %v", err)
	}
	assertNoOwnedCordons(t, f)
}

func TestRunnerAmbiguousResizeRetainsCordonsAndDoesNotRetry(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 3, true), resizeErr: errors.New("connection lost after POST")}
	err := runFake(t, f, testOptions(1, 1))
	if err == nil || !strings.Contains(err.Error(), "outcome is uncertain") || len(f.resizeTargets) != 1 || len(f.uncordoned) != 0 {
		t.Fatalf("ambiguous write incorrectly retried or cleaned up: %v", err)
	}
	if !f.snapshot.Nodes[0].Unschedulable || f.snapshot.Nodes[0].Owner != "test-run" {
		t.Fatal("ambiguous resize did not retain owned cordon")
	}
}

func TestRunnerOperationTimeoutRetainsCordons(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 3, false), waitErr: context.DeadlineExceeded}
	err := runFake(t, f, testOptions(1, 1))
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "operation-1") || len(f.uncordoned) != 0 {
		t.Fatalf("operation timeout did not preserve recovery information/cordons: %v", err)
	}
}

func TestRunnerDifferentGKEVictimsStopFurtherBatches(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 4, true), wrongVictim: true}
	err := runFake(t, f, testOptions(1, 1))
	if err == nil || !strings.Contains(err.Error(), "different victims") || len(f.resizeTargets) != 1 {
		t.Fatalf("unexpected victims did not stop the run: %v; resizes=%v", err, f.resizeTargets)
	}
	if len(f.uncordoned) != 1 || len(f.snapshot.Nodes) != 3 {
		t.Fatalf("surviving prepared node was not restored: %v", f.uncordoned)
	}
	assertNoOwnedCordons(t, f)
}

func TestRunnerFreezesReviewedCandidatesAcrossPodRedistribution(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 4, true)}
	// Eviction moves pods onto the next oldest node. A fresh ranking would
	// prefer the younger, less-loaded nodes; the reviewed order must survive.
	if err := runFake(t, f, testOptions(1, 1)); err != nil {
		t.Fatal(err)
	}
	want := []string{"worker-a-00", "worker-a-01", "worker-a-02"}
	if !reflect.DeepEqual(f.cordoned, want) {
		t.Fatalf("reviewed candidates changed after redistribution: got %v, want %v", f.cordoned, want)
	}
}

func TestRunnerUnsafePodAppearingAfterCordonAborts(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 3, true)}
	f.onCordon = func(f *fakeBackend, n Node) {
		f.snapshot.Pods = append(f.snapshot.Pods, Pod{Namespace: "default", Name: "unmanaged", UID: "new-pod", Node: n.Name})
	}
	err := runFake(t, f, testOptions(1, 1))
	if err == nil || len(f.resizeTargets) != 0 {
		t.Fatalf("new unmanaged pod was ignored: %v", err)
	}
	assertNoOwnedCordons(t, f)
}

func TestRunnerAutoscalingEnabledDuringDrainAborts(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 3, true)}
	f.onSnapshot = func(f *fakeBackend) {
		if f.snapshotCalls == 2 {
			f.snapshot.Autoscaling = true
		}
	}
	err := runFake(t, f, testOptions(1, 1))
	if err == nil || !strings.Contains(err.Error(), "autoscaling") || len(f.resizeTargets) != 0 {
		t.Fatalf("external autoscaling change did not block resize: %v", err)
	}
	assertNoOwnedCordons(t, f)
}

func TestRunnerWorkloadDesiredStateDriftAborts(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 3, true)}
	f.onEvict = func(f *fakeBackend, _ Pod) {
		f.snapshot.Workloads[0].Desired++
		f.snapshot.Workloads[0].Ready++
		f.snapshot.Workloads[0].Generation++
		f.snapshot.Workloads[0].ObservedGeneration++
	}
	err := runFake(t, f, testOptions(1, 1))
	if err == nil || !strings.Contains(err.Error(), "changed UID, desired replicas, or generation") || len(f.resizeTargets) != 0 {
		t.Fatalf("workload desired-state drift did not stop resize: %v", err)
	}
	assertNoOwnedCordons(t, f)
}

func TestRunnerNewControllerOnSurvivingNodeAborts(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 3, true)}
	f.onEvict = func(f *fakeBackend, _ Pod) {
		f.snapshot.Workloads = append(f.snapshot.Workloads, Workload{Namespace: "default", Name: "new-app", UID: "new-controller", Kind: "ReplicaSet", Desired: 1, Ready: 1, Generation: 1, ObservedGeneration: 1})
		f.snapshot.Pods = append(f.snapshot.Pods, Pod{Namespace: "default", Name: "new-app-pod", UID: "new-pod", Node: "worker-a-02", OwnerKind: "ReplicaSet", OwnerName: "new-app", OwnerUID: "new-controller"})
	}
	err := runFake(t, f, testOptions(1, 1))
	if err == nil || !strings.Contains(err.Error(), "new controller appeared") || len(f.resizeTargets) != 0 {
		t.Fatalf("new controller was not rejected before resize: %v", err)
	}
	assertNoOwnedCordons(t, f)
}

func TestRunnerCleanupFailureIsReportedAlongsideDrainFailure(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 3, true), evictErr: errors.New("eviction forbidden"), uncordonErr: errors.New("patch forbidden")}
	err := runFake(t, f, testOptions(1, 1))
	if err == nil || !strings.Contains(err.Error(), "eviction forbidden") || !strings.Contains(err.Error(), "restore owned cordon") || !strings.Contains(err.Error(), "patch forbidden") {
		t.Fatalf("cleanup failure was hidden: %v", err)
	}
	if len(f.resizeTargets) != 0 {
		t.Fatal("resize called after failed eviction")
	}
}

func TestRunnerWaitsForTerminatingPodsWithoutReeviction(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 2, true)}
	f.evictOverride = func(f *fakeBackend, p Pod) error {
		for i := range f.snapshot.Pods {
			if f.snapshot.Pods[i].UID == p.UID {
				f.snapshot.Pods[i].Terminating = true
			}
		}
		return nil
	}
	f.onList = func(f *fakeBackend) {
		if f.listCalls == 3 {
			var kept []Pod
			for _, p := range f.snapshot.Pods {
				if !p.Terminating {
					kept = append(kept, p)
				}
			}
			f.snapshot.Pods = kept
		}
	}
	if err := runFake(t, f, testOptions(1, 1)); err != nil {
		t.Fatal(err)
	}
	if f.evictionCalls != 1 || f.listCalls < 3 {
		t.Fatalf("terminating pod was re-evicted or not waited for: evictions=%d lists=%d", f.evictionCalls, f.listCalls)
	}
}

func TestRunnerWaitsForWorkloadHealthBeforeResize(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 2, true)}
	f.onEvict = func(f *fakeBackend, _ Pod) { f.snapshot.Workloads[0].Ready-- }
	f.onHealth = func(f *fakeBackend) {
		if f.healthCalls == 3 {
			f.snapshot.Workloads[0].Ready = f.snapshot.Workloads[0].Desired
		}
	}
	if err := runFake(t, f, testOptions(1, 1)); err != nil {
		t.Fatal(err)
	}
	if f.healthCalls < 3 || len(f.resizeTargets) != 1 {
		t.Fatalf("health gate was skipped: health reads=%d resizes=%v", f.healthCalls, f.resizeTargets)
	}
}

func TestRunnerPostOperationCancellationRestoresSurvivors(t *testing.T) {
	f, opts := &fakeBackend{snapshot: testSnapshot(1, 3, true), wrongVictim: true}, testOptions(1, 1)
	p, err := BuildPlan(f.snapshot, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.onWait = func(_ *fakeBackend) { cancel() }
	err = (Runner{Backend: f, Options: opts, RunID: "test-run"}).Run(ctx, p)
	if !errors.Is(err, context.Canceled) || len(f.resizeTargets) != 1 {
		t.Fatalf("post-operation cancellation returned unexpected result: %v", err)
	}
	assertNoOwnedCordons(t, f)
}

func TestRunnerDoesNotClearAnotherOwnersCordon(t *testing.T) {
	f := &fakeBackend{snapshot: testSnapshot(1, 3, true)}
	f.onCordon = func(f *fakeBackend, _ Node) {
		f.snapshot.Nodes[0].Owner = "other-operator"
	}
	f.evictErr = errors.New("eviction forbidden")
	err := runFake(t, f, testOptions(1, 1))
	if err == nil || !f.snapshot.Nodes[0].Unschedulable || f.snapshot.Nodes[0].Owner != "other-operator" || len(f.uncordoned) != 0 {
		t.Fatalf("cleanup cleared another operator's cordon: %v", err)
	}
}
