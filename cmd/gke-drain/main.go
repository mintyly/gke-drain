// gke-drain previews and performs bounded GKE Standard node-pool reductions.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mintyly/gke-drain/internal/cloud"
	"github.com/mintyly/gke-drain/internal/demo"
	"github.com/mintyly/gke-drain/internal/drain"
)

var version = "0.1.0-dev"

type backendFactory func(context.Context, drain.Resource) (drain.Backend, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr, func(ctx context.Context, r drain.Resource) (drain.Backend, error) {
		return cloud.New(ctx, r)
	}))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, factory backendFactory) int {
	flags := flag.NewFlagSet("gke-drain", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var resource drain.Resource
	opts := drain.DefaultOptions()
	var execute, offline, showVersion bool
	var output string
	flags.StringVar(&resource.Project, "project", "", "Google Cloud project ID (use the ID, not its numeric project number)")
	flags.StringVar(&resource.Location, "location", "", "GKE control-plane region or zone")
	flags.StringVar(&resource.Cluster, "cluster", "", "GKE Standard cluster name")
	flags.StringVar(&resource.Pool, "node-pool", "", "Node pool to reduce")
	flags.IntVar(&opts.TargetPerZone, "target-per-zone", -1, "Required final node count in EACH node-pool zone (at least 1)")
	flags.IntVar(&opts.BatchPerZone, "batch-per-zone", 1, "Maximum nodes to drain in EACH zone per batch")
	flags.StringVar(&opts.PreferMachineType, "prefer-machine-type", "", "Prefer this machine type for draining; actual GKE removal is not guaranteed")
	flags.BoolVar(&opts.AllowEmptyDir, "allow-emptydir", false, "Allow evacuation of pods whose emptyDir contents will be lost")
	flags.BoolVar(&opts.AllowPVC, "allow-persistent-volumes", false, "Allow PVC-backed pods after manually checking storage and zone constraints")
	flags.DurationVar(&opts.DrainTimeout, "drain-timeout", opts.DrainTimeout, "Deadline for each batch's evictions and workload readiness")
	flags.DurationVar(&opts.OperationTimeout, "operation-timeout", opts.OperationTimeout, "Deadline for each GKE resize and reconciliation")
	flags.DurationVar(&opts.PollInterval, "poll-interval", opts.PollInterval, "Interval for bounded polling and retryable evictions")
	flags.BoolVar(&execute, "execute", false, "Execute the displayed plan; default is read-only preview")
	flags.BoolVar(&offline, "demo", false, "Preview a synthetic two-zone pool without cloud access; cannot execute")
	flags.BoolVar(&showVersion, "version", false, "Print build version and exit")
	flags.StringVar(&output, "output", "text", "Plan format: text or json; events go to stderr")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "gke-drain — preview-first GKE Standard node-pool scale-down")
		fmt.Fprintln(stderr, "\nUsage: gke-drain --project PROJECT --location LOCATION --cluster CLUSTER --node-pool POOL --target-per-zone N [--execute]")
		fmt.Fprintln(stderr, "       gke-drain --demo [--target-per-zone 2] [--output json]")
		fmt.Fprintln(stderr, "\nGKE chooses the nodes it removes. Preferred drain candidates are not guaranteed victims.")
		fmt.Fprintln(stderr, "Node and batch counts are PER ZONE. Workload replicas are unchanged.\n\nOptions:")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if output != "text" && output != "json" {
		fmt.Fprintln(stderr, "error: --output must be text or json")
		return 2
	}
	report := func(e drain.Event) {
		if e.Time.IsZero() {
			e.Time = time.Now().UTC()
		}
		if output == "json" {
			_ = json.NewEncoder(stderr).Encode(e)
			return
		}
		fmt.Fprintf(stderr, "[%s] %s", e.Kind, e.Message)
		if e.Node != "" {
			fmt.Fprintf(stderr, " node=%s", e.Node)
		}
		if e.Operation != "" {
			fmt.Fprintf(stderr, " operation=%s", e.Operation)
		}
		fmt.Fprintln(stderr)
	}
	fail := func(err error, code int) int {
		report(drain.Event{Kind: "error", Message: err.Error()})
		return code
	}
	if flags.NArg() != 0 {
		return fail(fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " ")), 2)
	}
	if showVersion {
		fmt.Fprintln(stdout, "gke-drain "+version)
		return 0
	}
	if offline && execute {
		return fail(errors.New("--demo is offline and plan-only; it cannot be combined with --execute"), 2)
	}
	if offline && resource != (drain.Resource{}) {
		return fail(errors.New("--demo uses a synthetic resource; omit --project, --location, --cluster and --node-pool"), 2)
	}
	if offline && opts.TargetPerZone == -1 {
		opts.TargetPerZone = 2
	}
	if err := drain.ValidateOptions(opts); err != nil {
		return fail(err, 2)
	}
	if !offline && (resource.Project == "" || resource.Location == "" || resource.Cluster == "" || resource.Pool == "") {
		return fail(errors.New("--project, --location, --cluster and --node-pool are required for live preview or execution"), 2)
	}
	if err := ctx.Err(); err != nil {
		return fail(err, 1)
	}
	var backend drain.Backend
	var snapshot drain.Snapshot
	if offline {
		snapshot = demo.Snapshot()
		report(drain.Event{Kind: "demo", Message: "Synthetic offline plan. No cloud APIs or credentials are used."})
	} else {
		// Bound discovery independently of the mutation deadlines. No cloud
		// mutation is possible until the complete plan has passed validation.
		var err error
		// ADC token sources can retain their construction context for refresh.
		// Keep that context alive for the run, while bounding snapshot reads.
		backend, err = factory(ctx, resource)
		discoveryCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		if err == nil {
			snapshot, err = backend.Snapshot(discoveryCtx)
		}
		cancel()
		if err != nil {
			return fail(fmt.Errorf("discover target: %w", err), 1)
		}
	}
	plan, err := drain.BuildPlan(snapshot, opts)
	if err != nil {
		return fail(fmt.Errorf("preflight: %w", err), 1)
	}
	if err := writePlan(stdout, plan, output); err != nil {
		return fail(fmt.Errorf("write plan: %w", err), 1)
	}
	if !execute {
		if !offline {
			report(drain.Event{Kind: "preview", Message: "Read-only preview complete. Add --execute to perform a fresh, revalidated run."})
		}
		return 0
	}
	if len(plan.Batches) == 0 {
		report(drain.Event{Kind: "complete", Message: "Target already reached; no changes made."})
		return 0
	}
	runBytes := make([]byte, 16)
	if _, err := rand.Read(runBytes); err != nil {
		return fail(fmt.Errorf("create unique run ID: %w", err), 1)
	}
	runID := hex.EncodeToString(runBytes)
	report(drain.Event{Kind: "start", Message: "Executing run " + runID + ". GKE removal selection is not guaranteed; inspect recovery events if interrupted."})
	runner := drain.Runner{Backend: backend, Options: opts, Report: report, RunID: runID}
	if err := runner.Run(ctx, plan); err != nil {
		return fail(err, 1)
	}
	report(drain.Event{Kind: "complete", Message: "All batches completed and actual node removals reconciled."})
	return 0
}

func writePlan(w io.Writer, p drain.Plan, format string) error {
	if format == "json" {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(p)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Target: %s\n", p.Resource.PoolPath())
	fmt.Fprintf(&b, "Zones: %s\n", strings.Join(p.Zones, ", "))
	fmt.Fprintf(&b, "Capacity: %d -> %d per zone | %d -> %d total nodes\n", p.CurrentPerZone, p.TargetPerZone, p.TotalBefore, p.TotalAfter)
	fmt.Fprintf(&b, "Plan: %s\n", p.Fingerprint)
	if len(p.Batches) == 0 {
		fmt.Fprintln(&b, "No scale-down needed.")
	}
	for _, batch := range p.Batches {
		fmt.Fprintf(&b, "\nBatch %d: %d -> %d per zone; %d preferred drain candidates\n", batch.Number, batch.BeforePerZone, batch.AfterPerZone, len(batch.Candidates))
		for _, node := range batch.Candidates {
			fmt.Fprintf(&b, "  %-24s %s\n", node.Zone, node.Name)
		}
	}
	if len(p.Warnings) > 0 {
		fmt.Fprintln(&b, "\nOperational limits:")
		for _, warning := range p.Warnings {
			fmt.Fprintf(&b, "  - %s\n", warning)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}
