package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mintyly/gke-drain/internal/demo"
	"github.com/mintyly/gke-drain/internal/drain"
)

func forbiddenFactory(t *testing.T) backendFactory {
	t.Helper()
	return func(context.Context, drain.Resource) (drain.Backend, error) {
		t.Fatal("unexpected credential/network access")
		return nil, errors.New("unexpected cloud access")
	}
}

func TestDemoJSONIsOfflineAndUsesPerZoneUnits(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--demo", "--output", "json"}, &stdout, &stderr, forbiddenFactory(t))
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	var p drain.Plan
	if err := json.Unmarshal(stdout.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.TotalBefore != 6 || p.TotalAfter != 4 || p.TargetPerZone != 2 || len(p.Batches) != 1 || len(p.Batches[0].Candidates) != 2 {
		t.Fatalf("incorrect regional plan: %+v", p)
	}
	if !strings.Contains(stderr.String(), "Synthetic offline") {
		t.Fatal("demo not identified as synthetic")
	}
}

func TestCLIRejectsInvalidInputsBeforeCloudAccess(t *testing.T) {
	cases := [][]string{
		{"--demo", "--execute"},
		{"--demo", "--project", "actual-project"},
		{"--demo", "--batch-per-zone", "0"},
		{"--demo", "--target-per-zone", "0"},
		{"--demo", "--drain-timeout", "0s"},
		{"--demo", "--output", "yaml"},
		{"--target-per-zone", "2"},
		{"--demo", "unexpected"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(context.Background(), args, &stdout, &stderr, forbiddenFactory(t)); code == 0 {
				t.Fatalf("expected rejection; stdout=%s", stdout.String())
			}
		})
	}
}

func TestHelpAndVersionNeverAccessCloud(t *testing.T) {
	for _, arg := range []string{"--help", "--version"} {
		var stdout, stderr bytes.Buffer
		if code := run(context.Background(), []string{arg}, &stdout, &stderr, forbiddenFactory(t)); code != 0 {
			t.Fatalf("%s exited %d", arg, code)
		}
	}
}

type readOnlyBackend struct {
	drain.Backend // Any accidental mutation panics instead of reaching a cloud.
	reads         int
}

func (b *readOnlyBackend) Snapshot(context.Context) (drain.Snapshot, error) {
	b.reads++
	return demo.Snapshot(), nil
}

func TestLivePreviewOnlyReads(t *testing.T) {
	backend := &readOnlyBackend{}
	var stdout, stderr bytes.Buffer
	args := []string{"--project", "demo-project", "--location", "asia-southeast1", "--cluster", "demo-cluster", "--node-pool", "workers", "--target-per-zone", "2"}
	code := run(context.Background(), args, &stdout, &stderr, func(_ context.Context, r drain.Resource) (drain.Backend, error) {
		if r.Project != "demo-project" || r.Pool != "workers" {
			t.Fatalf("wrong target: %+v", r)
		}
		return backend, nil
	})
	if code != 0 || backend.reads != 1 || !strings.Contains(stderr.String(), "Read-only preview") {
		t.Fatalf("code=%d reads=%d stderr=%s", code, backend.reads, stderr.String())
	}
}

func TestDiscoveryFailureReturnsNonzeroAndNoPlan(t *testing.T) {
	var stdout, stderr bytes.Buffer
	args := []string{"--project", "demo-project", "--location", "asia-southeast1", "--cluster", "demo-cluster", "--node-pool", "workers", "--target-per-zone", "2", "--output", "json"}
	code := run(context.Background(), args, &stdout, &stderr, func(context.Context, drain.Resource) (drain.Backend, error) {
		return nil, errors.New("ADC unavailable")
	})
	if code != 1 || stdout.Len() != 0 {
		t.Fatalf("code=%d stdout=%s", code, stdout.String())
	}
	var event drain.Event
	if err := json.Unmarshal(stderr.Bytes(), &event); err != nil || event.Kind != "error" {
		t.Fatalf("bad JSON diagnostic: %s", stderr.String())
	}
}

func TestCanceledCommandNeverAccessesCloud(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	if code := run(ctx, []string{"--demo"}, &stdout, &stderr, forbiddenFactory(t)); code != 1 {
		t.Fatalf("exit=%d", code)
	}
}
