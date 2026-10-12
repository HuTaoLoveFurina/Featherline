# PK differences

Featherline's pharmacokinetic math borrows the model shape and many
parameter values from
[`LaoZhong-Mihari/HRT-Recorder-PKcomponent-Test`](https://github.com/LaoZhong-Mihari/HRT-Recorder-PKcomponent-Test).
This page lists what's the same, what's different, and why; it does
not restate the math itself.

## Upstream reference

The upstream ships analytical PK models for estradiol and testosterone across
injection / patch / gel / oral / sublingual routes, a unified parameter
catalog (`PKSharedCatalog.json`), and Swift implementations
(`PKparameter.swift`, `PKcore.swift`, `SimulationEngine`) targeting iOS
and watchOS. The math, the route catalog, and the population-average
parameters in
[`PkCatalog`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L1203)
trace back to that reference. Anyone wanting the math itself —
compartment equations, parameter provenance, literature anchors —
should read the upstream README and its `pk_research/` workspace.

## What Featherline reuses

- **Model shape.** Two parallel depots → esterase hydrolysis →
  clearance for injections; first-order Bateman absorption for oral,
  gel, and fallback patches; constant-rate input for labelled patches;
  dual-pathway (fast mucosal + slow swallowed-oral) for sublingual.
  Implemented in
  [`ThreeCompartmentModel`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L849)
  and consumed by
  [`PkSimulationEngine`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L586).
- **Route catalog and compound metadata.** `PkRoute`, `PkCompound`,
  and `PkCatalog`'s `compounds` / `twoPartDepot` / `formationFraction`
  / `hydrolysisK2` / `oralKAbs` / `oralBioavailability` mirror the
  upstream catalog for the routes it supports.
- **Population-average parameter values.** Clearance constants
  (`kClear`, `kClearInjection`), volume-of-distribution (`vdPerKg =
  2.0` for both hormones), gel/patch absorption rates, and the
  per-compound `k1Fast` / `k1Slow` / `k2` / formation fractions are
  taken from upstream's anchored parameters. There are two exceptions.
  **Estradiol undecylate (EU)** takes its depot parameters from
  [`Transmtf-HRT-Tracker`](https://github.com/TransmtfTeam/Transmtf-HRT-Tracker)
  (the "im-depot-v1" model), because the LaoZhong upstream has no EU
  model. **Oral and sublingual estradiol** use Featherline's own
  values, described below.

## What Featherline changes

- **Single-hormone simulation runtime.** Upstream's catalog and
  simulator drive both estradiol and testosterone end-to-end.
  Featherline keeps the testosterone constants in
  [`PkCatalog`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L1203)
  for parity but wires only the estradiol path through
  [`toEstradiolPkDoseEvent`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L1069),
  `buildEstradiolEvents`, and
  [`simulateMainEstradiolTrend`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L314);
  the testosterone surface stays dormant until the planned PK engine
  swap lands.
- **Planned-dose projection.** Upstream simulates the doses you give
  it. Featherline synthesizes virtual future-dose events for
  unfulfilled scheduled slots via
  [`buildEstradiolPkSimulationEntries`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkPlannedEntries.kt#L25)
  so the home chart can show a "if you take everything on schedule"
  curve to the right of the prediction marker. The `isPlanned` flag
  propagates to dose markers so the UI can distinguish logged from
  projected doses.
- **Option-aware chart-sample placement.** Upstream's
  `SimulationEngine` is a math reference, not a chart engine, so it
  samples on a single fixed step. Featherline drives a user-selectable
  home chart through `HomeE2ChartWindowOption`: `SEVEN_DAYS` renders a
  3-day-past / 4-day-future window on a 0.1 h dense interval, while
  `THIRTY_DAYS` renders a 16-day-past / 14-day-future window on a
  budgeted sampler (`segmentCount = 2240`) with extra post-dose
  offsets.

  The seven-day path preserves the original six-minute resolution.
  The 30-day path trades uniform density for a bounded sample budget
  and then pins absorption-phase offsets after each dose (0.25 h
  through 48 h) so fast-Tmax routes such as oral and sublingual do not
  underplot their local peaks. Both options also pin the prediction
  instant, the previous-day instant, and every logged-event instant.
  The trend
  ([`mainChartSampleTimeH`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L490))
  and projection
  ([`mainProjectionSampleTimeH`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L533))
  paths share the strategy but differ in which "exact" instants they
  pin.
- **Option-aware chart-window rounding contract.**
  `simulateMainEstradiolProjection` rounds the projection-cache window
  to local midnight in `zoneId`: start = today minus `option.pastDays`
  at 00:00, end = today plus `option.projectionFutureDays()` at
  00:00. `projectionFutureDays()` adds a 10-day forward buffer beyond
  the visible future span, so the 30-day chart stores a 40-day
  projection while rendering only the 30-day visible window.
  `HomeSnapshotRepository.snapshotUsabilityFailure` independently
  recomputes the visible window and rejects cached snapshots whose
  coverage or sampling fingerprint (`chartWindowHours`, dense policy,
  post-dose-offset flag) does not match the selected option. Upstream
  has no such contract because it has no cache.
- **Projection cache bundled into home snapshot.** Two projection
  records are bundled into `HomeSnapshotRecord` alongside the
  plan-and-fulfillment cache: `pkProjection` (the chart's
  option-sized projection — 7-day or 30-day visible window with the
  forward buffer) and `widgetPkProjection` (a smaller projection
  sized for the widget's E2 estimate, which has its own validity
  window). Every home-data mutation re-simulates both, even when no
  PK input changed. The leaky seam and its fallback path
  (`MainViewModel` re-simulates the chart projection directly when
  the cached expiry is on-or-before the live `now`) are documented
  in
  [`architecture.md#home-snapshot-and-pk-projection-cache`](architecture.md#home-snapshot-and-pk-projection-cache);
  the widget consumes `widgetPkProjection` via
  [`WidgetSnapshotBuilder`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/widget/WidgetSnapshotBuilder.kt)
  with its own expiry check on the widget render path. Upstream has
  no equivalent because it has no persistent home snapshot.
- **Active-mg conversion on the data path.** Upstream resolves dose
  events directly inside its simulator. Featherline computes the
  per-event active-mg-as-E2 upstream of the simulator in
  [`DoseInstructionCalculator`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/medication/DoseInstructionCalculator.kt),
  which resolves a `Medicine`'s `MedicinePreparation` +
  `DoseInstruction` into mg-as-E2 using the calculator's own
  per-compound molecular-weight constants and persists it on the log
  row as `equivalentE2Mg`. Inside the simulator,
  [`pkDoseAmounts`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L1106)
  prefers the snapshotted value for non-patch routes (falling back to
  `DoseInstructionCalculator.perUnitEquivalentE2Mg` for planned
  entries or legacy rows that have no snapshot) and reads the patch
  specification directly for `PATCH_APPLY` / `PATCH_REMOVE`; the
  engine's `formationFraction` values are calibrated against
  active-E2 input, matching upstream's convention. `PkCatalog`'s
  per-compound `activeFactor` is *not* applied at dose-event
  construction or in the current runtime simulation path; equivalent-E2
  input comes from the snapshotted value or `DoseInstructionCalculator`.
- **Imported injection/gel preparations skip fabricated package
  geometry.** External tracker imports do not know vial concentration,
  vial volume, gel sachet weight, or gel concentration percent. Rather
  than inventing those fields, Featherline stores imported injection
  and gel support medicines as `MedicinePreparation.ImportedInjection`
  and `MedicinePreparation.ImportedGel`. `DoseInstructionCalculator`
  reads the administered/applied mg directly for whole-unit imported
  logs, converts imported injection esters to E2-equivalent mg with the
  same molecular-weight table as other logged doses, and treats
  imported gel as already-applied estradiol. `PkSimulation` maps these
  preparations directly to `PkCompound` (`ImportedInjection.ester` or
  `E2` for gel), so imported rows feed the simulator without requiring
  a `Catalog` medicine selection.
- **Duplicated molecular-weight constants.** Estradiol
  molecular-weight constants live in `PkCatalog.compounds` and again
  in
  [`DoseInstructionCalculator`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/medication/DoseInstructionCalculator.kt),
  which log creation uses to snapshot `equivalentE2Mg` and planned PK
  entries use to rederive it when no snapshot exists. Medication UI
  code also uses the calculator for raw dose amount and patch
  release-rate display, but not for a dose-equivalence label. The two
  copies disagree at the second decimal
  (`PkCatalog` uses 272.38 / 376.5 / 356.5 / 396.58 / 384.56 / 440.66;
  `DoseInstructionCalculator` uses 272.4 / 376.4 / 356.5 / 396.6 /
  384.5 / 440.66 — the estradiol undecylate value is the one that
  agrees across both copies). The disagreement is below the resolution any user cares
  about, but the redundancy is flagged — both copies collapse into
  the planned PK engine.
- **Default gel application area baked in.** `DefaultGelAreaCm2 =
  750.0` is set at the dose-event factory; upstream's catalog exposes
  area-related parameters per event. Featherline doesn't expose this in
  the UI because the current gel model doesn't read `areaCm2` —
  `PkParameterResolver`'s `GEL` branch uses `core.gelK1` and
  `core.gelFMax` directly. The field is kept so a future skin-depot
  gel model has somewhere to put per-event area without a data-class
  bump.
- **Sublingual θ is fixed, not user-settable.** Upstream lets the user
  configure the sublingual θ — the fraction of a sublingual dose
  absorbed through the buccal mucosa (fast pathway) versus swallowed
  and absorbed orally (slow pathway). Featherline's
  [`PkDoseEvent`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L73)
  carries an optional `sublingualTheta: Double?` field for parity, but
  nothing in the UI ever sets it. The
  [`PkParameterResolver.SUBLINGUAL`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L820)
  branch always falls back to the catalog default
  [`PkCatalog.standardSublingualTheta = 0.03`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/pk/PkSimulation.kt#L1204).
  Real per-person variation in mucosal-vs-swallowed fraction can be
  substantial; Featherline does not currently expose it for tuning.
- **Oral and sublingual estradiol are refit to blood levels.** Upstream's
  oral E2 values (`oralKAbs = 0.32`, `oralBioavailability = 0.03`)
  give a terminal half-life of about 2 h. Oral E2's is 13–20 h, because
  estrone sulfate keeps converting back to E2. With upstream's values,
  2 mg once daily falls to under 1 pg/mL by the next dose. Those
  near-zero troughs fall under the calibration floor, so trough results
  were set aside. Upstream's 2026 source audit did not cover estradiol.
  Featherline uses `oralKAbs[E2] = 0.06` and
  `oralBioavailability[E2] = 0.043`; the slow absorption rate stands in
  for the estrone-sulfate reservoir. At 70 kg, 2 mg once daily reaches
  a steady state of 89 / 33 / 62.9 pg/mL (peak / trough / average). The
  label's figures are 89 / 35 / 62.9. A single 1 mg dose peaks at
  33 pg/mL at 5.5 h; Doll 2022 measured about 35. Upstream's θ = 0.11
  comes from a dissolve-or-swallow model, not from blood levels. It put
  the 1 mg sublingual peak near 550 pg/mL. Featherline uses θ = 0.03,
  which gives 154 pg/mL at 1.2 h; Doll 2022 measured about 144 at
  1–2 h. Total exposure is 1.7× oral (Price 1997 measured about 2.5×).
  The swallowed part goes through the refit oral path. Over 0–8 h the
  model gives 3.3× the oral exposure, against 1.8× measured by Doll
  2022. Doll's full time course would show which part of the curve is
  off.
- **Estradiol valerate (EV) is modeled as E2 when swallowed or held
  under the tongue.** Doses are already converted to E2 content, and EV
  now uses E2's oral values and θ. Upstream used `oralKAbs[EV] = 0.05`
  and `oralBioavailability[EV] = 0.03`, and sent the mouth-absorbed part
  through the injection hydrolysis rate (`hydrolysisK2[EV] = 0.07`).
  In Wiegratz 2001, 2 mg of EV gave about 60 pg/mL at day 21, against
  80 for 2 mg of micronized E2. That is about the ratio of their E2
  content (0.76). The two products carried different progestins. No
  study has measured sublingual EV levels over time. TFS's
  [review](https://transfemscience.org/articles/sublingual-ev/) expects
  it to work like sublingual E2 at equal E2 content.

## Route-scale estimate calibration

Featherline 1.4.0 adds a per-user estimate calibration of its own, not taken
from upstream, on top of the unchanged population engine, in
[`model/pk`](https://github.com/mkx173/Featherline/tree/main/app/src/main/java/com/mkx/hrttracker/model/pk)
(`PkCalibration*`, `PkE2ForwardModel`). It does not touch the
compartment parameters: it fits one log-scale `beta_r` per route and
multiplies that route's population contribution by `e^{beta_r}`.

- **Forward model.** For each E2 result `i`, the population engine
  splits the modeled drug-attributable E2 at the collection time into
  per-route parts `d_ir`. The adjusted prediction is
  `m_i(beta) = sum_r e^{beta_r} d_ir`, so a result informs every route
  in proportion to its modeled share.
- **Evidence filter.** A result is set aside when the modeled total is
  below 5 pg/mL (`drugMinInformativePgml`), when its value is zero or
  below, or when the forward model cannot be evaluated. Results the user
  excluded are skipped. There is no endogenous-E2 term. The fit reads
  estradiol doses from 180 days before the earliest result; if every dose
  is older than that, the latest one is kept, so those results read as
  below the modeled signal rather than as having no dose logged.
- **Objective.** A joint MAP over the supported routes (some result
  draws at least 20% of its modeled signal from the route) with a
  Student-t (`nu = 4`) likelihood on the log residual
  `log y_i - log m_i`, scaled by `R_LOG = 0.0225` (about 15% log-SD),
  plus a Gaussian prior `beta_r ~ N(0, 0.30^2)`. Every other route keeps
  `beta_r = 0` and shows as population.
- **Solver.** Deterministic multi-start damped Newton. Starts are
  `beta = 0`, each route's 1-D conditional minimum (grid plus
  bisection), and every pairwise combination of those. If two distinct
  minima survive, the best one is used and every route is flagged
  ambiguous.
- **Uncertainty.** A Laplace approximation at the MAP gives each route's
  posterior SD. The Home band is the 68% and 95% prediction interval
  for a new result (Laplace covariance plus Student-t
  observation noise, integrated with Gauss-Hermite quadrature). It is
  not a therapeutic range.
- **Warn, don't gate.** Every supported route is applied. Warnings are
  attached when the scale is outside the route's usual
  range (0.5 to 2 for injection, patch, and oral; 0.25 to 3 for gel and
  sublingual) or is a large shift backed by fewer than three results,
  when the posterior SD is above 0.20 or the supporting results span
  less than a 2x signal range, when the weighted log RMSE exceeds
  `2 * sqrt(R_LOG)`, when a supporting result has a Student-t weight
  below 0.25 and hasn't been reviewed, or when the fit is ambiguous.
- **What "calibrated" means on screen.** The Home hero and the calibration status
  row call the estimate calibrated when a fitted route is at least 20%
  (`PROMOTION_SUPPORT_SHARE_MIN`) of the drawn signal at some knot of
  Home's visible chart window; the calibration page judges over the
  same window, so the two agree. The band is drawn only over the times
  where that holds, so "calibrated" and a visible band go together, and
  each band knot carries its contiguous-run tag so the chart breaks the
  band at gaps instead of bridging them. The routes sheet marks a fitted
  route that never reaches that share with a "Small share" pill. An old route's exponential tail stays
  positive for months, so "any contribution" would label a curve from a
  new, uncalibrated route as calibrated.
- **Review.** Per-result choices (`REVIEWED`, `EXCLUDED`) live in
  `e2_calibration_metadata`, `AUTO` as no row; see [data-model.md](data-model.md).
  `REVIEWED` only hides the review tip and never changes the fit.

The fit runs entirely on device, inside the Home snapshot build, so the
Home chart, the band, and the widgets all draw from the same adjusted
parameters. Hipparchus supplies the bisection and Brent root solvers,
Cholesky decomposition, Gauss-Hermite integration, and the Student-t
distribution (the band uses the closed-form t4 CDF, falling back to the
library for any other degrees of freedom); see
[third-party-notices.md](third-party-notices.md). The band solves its
quantile law once per distinct variance, with variance + R_LOG rounded to a
0.5% geometric grid (under 0.25% in SD, below a pixel at chart scale), so a
dense multi-route chart costs a few hundred solves at most.

## What Featherline plans to change

The PK engine swap gates three deferred refactors, all enumerated in
[`architecture.md#known-limitations-and-planned-refactors`](architecture.md#known-limitations-and-planned-refactors):

- Consolidate the fragmented PK math (constants, three-compartment
  simulation, dose-equivalence) so `PkCatalog` and
  [`DoseInstructionCalculator`](https://github.com/mkx173/Featherline/blob/main/app/src/main/java/com/mkx/hrttracker/model/medication/DoseInstructionCalculator.kt)'s
  overlapping molecular-weight constants collapse into one source of
  truth.
- Untangle the projection cache from `HomeSnapshotRepository` so PK
  re-simulation isn't triggered by unrelated home mutations.

## Out of scope

- **The PK math itself.** Compartment equations, parameter
  derivations, and the literature anchors for each constant live in
  the upstream README and its `pk_research/` workspace.
- **Testosterone as a user-facing simulation.** `PkHormone.TESTOSTERONE`
  exists in the catalog with full parameters, but no user-logged
  category currently produces testosterone PK events, so testosterone
  trends are not rendered.
