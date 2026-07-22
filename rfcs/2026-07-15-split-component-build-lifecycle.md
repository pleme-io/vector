# RFC - Split Component Lifecycle into Four Distinct Phases

Component config traits currently conflate structural validation, environment validation, pure
construction, and task spawning into a single `build()` method. This is most acute for
`TransformConfig`, but the same shape of problem exists for `SinkConfig`. This RFC proposes a
shared `ComponentConfig` trait with three explicit config-time phases (`validate_structure`,
`validate_environment`, and `build`), implemented by `TransformConfig` and `SinkConfig`,
to make `vector validate` reliable, prevent resource leaks on topology reload rollback, and
simplify unit testing.

## Context

- Immediate motivation: [#25161](https://github.com/vectordotdev/vector/pull/25161) fixed
  `vector validate --no-environment` silently skipping VRL/condition errors, but needed ~540 lines
  (`validate_env()` plus `TransformContext::key` guard clauses) to work around the lack of a clean
  trait contract.
- Investigating the fix surfaced that `SinkConfig` has the same entanglement in a milder form.
  `build()` already returns `(VectorSink, Healthcheck)`, an unstarted sink plus a deferred
  environment check, and the topology builder already treats `Healthcheck` as a distinct phase
  (`run_healthchecks` gates topology commit in `src/topology/running.rs`, before `spawn_diff`
  actually starts driving events). Formalizing this as a named phase on a shared trait, rather than
  an implicit convention, closes the same `vector validate` gap for sinks that this RFC closes for
  transforms.

## Scope

### In scope

- A new `ComponentConfig` trait with associated types `Context` and `Built`, defining
  `validate_structure`, `validate_environment`, and `build` as the three config-time phases.
- `TransformConfig: ComponentConfig<Context = TransformContext, Built = Transform>`: introduce
  `validate_structure`, `validate_environment`, and `build` as construction only, no task spawning.
  Phase 4 (task construction and channel wiring) stays in `TopologyPiecesBuilder::build_transform`
  unchanged.
- `SinkConfig: ComponentConfig<Context = SinkContext, Built = VectorSink>`: hoist `Healthcheck`
  construction out of `build()` and into `validate_environment`; redefine `build` as construction
  of an unstarted `VectorSink`, no task spawning.
- Update `TopologyPiecesBuilder` and `vector validate` to call each phase at the right point for
  both transforms and sinks.
- Migrate all existing transforms and sinks.

### Out of scope

- `SourceConfig` is deferred. `Source` is defined as `BoxFuture<'static, Result<(), ()>>`
  (`lib/vector-core/src/source.rs`), so `build()`'s return value *is* the run loop rather than an
  inert handle. Applying `ComponentConfig` to sources is doable but a larger effort than this RFC's
  scope; see Future Improvements.
- Changes to user-visible configuration format or component behavior.

## Motivation

- `vector validate` has no clean way to "check VRL without starting threads." The current workaround
  (stub enrichment tables, `validate_env()`, `context.key` guards) must be replicated per-transform.
- `build()` spawns background tokio tasks before a topology reload is committed. If the reload is
  rolled back, those tasks leak.
- Testing transform logic requires spinning up background machinery because construction and startup
  are inseparable.
- The `build()` signature gives no signal about whether an implementation is safe to call
  speculatively (during validation) or whether it has observable side effects.
- For sinks, `--no-environment` is all-or-nothing: `vector validate` either skips `build()` entirely
  for every sink (no config validation beyond deserialization) or calls the real `build()`, which
  today also constructs a live `Healthcheck` future tied to real credentials/endpoints
  (e.g. `src/sinks/http/config.rs`, `src/sinks/kafka/config.rs`). There is no way to validate a
  sink's config-level construction (auth parsing, encoder setup) without also being able to reach the
  real endpoint.
- [#25840](https://github.com/vectordotdev/vector/issues/25840) is a concrete, currently-open
  instance of the same gap on the `validate_structure` side. The routing-field template
  confinement check is purely lexical (no I/O), but it lives in `SinkConfig::build()`
  (e.g. `src/sinks/aws_s3/config.rs`), so `--no-environment` skips it and a confinement-violating
  config is only caught at real boot.

## Proposal

### User Experience

No user-visible change. `vector validate` and `vector validate --no-environment` behave the same
externally; the difference is that `validate` now exercises the same VRL compilation and sink
construction paths as normal startup, rather than separate, potentially divergent ones.

### Implementation

A shared `ComponentConfig` trait defines the three config-time phases. Phase 4 (startup) is not
part of the config trait: for sinks it is the existing `VectorSink::run()`; for transforms it stays
in `TopologyPiecesBuilder::build_transform()`, which owns the topology channels that cannot be
plumbed through a config-trait method.

```
ComponentConfig:
    // Phase 1: structural checks (malformed URIs, duplicate keys, out-of-range values) plus
    // internal consistency (VRL/condition compilation against stub or real context). Default: always ok.
    validate_structure()

    // Phase 2: answers one question: are the external dependencies this component needs reachable?
    // For sinks this means healthchecks. Other complex interactions with external dependencies
    // belong in run(). For transforms: no-op (nothing external to probe).
    validate_environment()

    // Phase 3: construct the component. No task spawning. Safe to discard on rollback.
    build(context)

// TransformConfig: validate_environment is a no-op
// SinkConfig:      validate_environment returns a Healthcheck future

// Phase 4 is unchanged for both:
//   Sinks:      VectorSink::run() (existing, no change).
//   Transforms: TopologyPiecesBuilder::build_transform() (existing, no change).
```

Existing component wiring and serialization registration are unaffected.

**Call sites:**

| Call site | Phases invoked (transforms) | Phases invoked (sinks) |
| --- | --- | --- |
| `vector validate --no-environment` | `validate_structure` | `validate_structure` |
| `vector validate` | `validate_structure` + `build` | `validate_structure` + `build` + `validate_environment` → await returned `Healthcheck` directly |
| Normal startup / reload (pre-commit) | `validate_structure` + `build` | `validate_structure` + `build` + `validate_environment` → await or spawn returned `Healthcheck` per `require_healthy` |
| Normal startup / reload (post-commit) | `TopologyPiecesBuilder::build_transform` (unchanged) | `run` (existing `VectorSink::run`) |

`--skip-healthchecks` short-circuits only the probe execution. The sink `build` phase runs
regardless; only the `validate_environment` healthcheck probe is skipped, matching current
behaviour. The per-sink and global `healthcheck.enabled` gates and the configured timeout remain
the caller's responsibility (`TopologyPiecesBuilder`), not the component's. `validate_environment`
returns the raw probe future unchanged.

**Migration:**

1. Add `ComponentConfig` with `validate_structure`, `validate_environment`, and `build` as
   required methods. Provide a blanket adapter for un-migrated components where `validate_structure`
   is always a no-op (never calls legacy `build()`), `validate_environment` calls legacy `build()`
   for sinks (to extract the `Healthcheck`) and is a no-op for transforms, and `build()` calls
   legacy `build()` and returns the component. Un-migrated sinks therefore call legacy `build()`
   twice during full startup; this is acceptable during the migration window.
2. Migrate transforms one at a time, starting with `remap` (VRL) and `filter` / `route` (conditions).
   Prerequisite for `remap`: move VRL file reading (`file:`/`files:` options) to config load time so
   `compile_vrl_program` never does file I/O. By the time any lifecycle phase runs, the source is
   already a `String` in memory.
3. Migrate sinks one at a time: hoist `Healthcheck` construction out of `build()` into
   `validate_environment`, starting with `http` and `kafka` as representative cases, since their
   `build()` impls already construct `Healthcheck` as a clearly separable step
   (`src/sinks/http/config.rs`, `src/sinks/kafka/config.rs`). Prerequisite for calling `build()`
   under `--no-environment`: move credential and client creation out of `build()` and into `run()`,
   so `build()` is credential-free. Until that refactor lands for a given sink, `--no-environment`
   stops at `validate_structure` for that sink. For sinks where `validate_environment` and `run()`
   both need a client, prefer lazy credential resolution so the client is constructed once in `run()`
   and the healthcheck probe uses it via a shared handle, rather than building the client twice.
4. Update `TopologyPiecesBuilder` to invoke phases at the appropriate points for both transforms and
   sinks. For sinks this mostly formalizes the existing `build`, `run_healthchecks`, `spawn_diff`
   ordering in `src/topology/running.rs` rather than restructuring it.
5. Update `vector validate` to call `validate_structure` for all components under both
   `--no-environment` and full validation. VRL/condition compilation moves into `validate_structure`
   (alongside the existing pure checks). `validate_environment` is only called for sinks, and only
   under full validation and startup. Remove the `validate_env()` workaround method.
6. Remove the blanket adapter once all transforms and sinks are migrated.

## Alternatives

- **Keep the current approach and add more per-transform workarounds.** Already proven insufficient:
  the fix PR added hundreds of lines of guard logic with no improvement to the trait contract.
- **Return a compiled artifact from `validate_environment` so `build()` doesn't recompile.**
  `build()` runs once per topology build or reload, not per event, so the duplicate VRL/condition
  compilation is a one-off, compile-time cost of a few milliseconds at most. Not worth the added
  type-system complexity (GAT vs. `Box<dyn Any>`) this would require.

## Plan Of Attack

1. Spike `ComponentConfig` behind a blanket adapter for one transform and one sink to prove the
   pattern compiles and holds under `TopologyPiecesBuilder` and `vector validate`.
2. Open a tracking issue listing every transform and sink still on the blanket adapter; migrate them
   incrementally, checking each off as it moves over.
3. Remove the blanket adapter and the `validate_env()` workaround once the tracking issue is clear.

## Future Improvements

- Apply the same `ComponentConfig` contract to `SourceConfig`. This requires introducing a new
  unstarted-source type to serve as `ComponentConfig::Built` (today `Source` is
  `BoxFuture<'static, Result<(), ()>>`, the run loop itself, not an inert handle), and auditing each
  source implementation individually: some (e.g. `socket`) already defer all work into the returned
  future and would migrate cheaply; others (e.g. `file`) perform environment-dependent work eagerly
  inside `build()` today and would need that work relocated to `validate_environment`.
