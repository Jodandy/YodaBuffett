# Build Spec — Focus-Narrowing Engine (the "what do I have to believe" funnel)

## What this is
A single-pass triage funnel over the full equity universe. For every company it makes the market's *implied expectations* legible, measures how far those sit from what the business can plausibly deliver, and ranks names by the size of that gap — tagged by which side of the mispricing they fall on.

Its job is to **point human attention at the handful worth deep work**. It is **not** a buy/sell verdict and **not** a precise valuation. Optimise for *recall*: it must never silently drop a fat pitch. The deep-dive judgment happens downstream, by a human, on the survivors.

## Hard design rules (do not violate)
1. **No verdicts.** The engine surfaces and ranks. It never outputs a recommendation or a "rating." If you produce a composite number, label it `triage_priority`, never `score`/`rating`.
2. **All numbers deterministic.** Every figure is computed from the input table. Nothing fabricated or inferred by a model.
3. **Flat parameters across all names.** Same discount rate, same horizons, for every company — comparability is the entire point of a first pass. **Never** vary the discount rate or horizon per company.
4. **Two-sided by construction.** It must surface both the cheap-and-feared side *and* the underpriced-durable-compounder side. A plain cheapness screen would systematically miss compounders — this must not.
5. **Stay lean.** One cheap stage, not a platform. No UI, no database, no extra horizons, no Monte Carlo, no per-company discount rate. Resist scope creep.

## Config (defaults shown; all overridable in a config file)
```
required_return        r   = 0.12
horizon_primary        N1  = 12      # the comparability yardstick
horizon_long           N2  = 24      # the compounder diagnostic
exit_multiple_ref          = 13      # neutral/conservative terminal PE; keep low so implied growth isn't gamed by a heroic terminal
# flag thresholds (gap sizes, ROIC cutoffs) also live here
roic_quality_cutoff        = 0.15
gap_material               = 0.03    # 3 percentage points of growth = "material"
```

## Input data contract
A table (CSV or dataframe), one row per company. Build the loader with a **configurable column mapping** so it binds to the user's existing file headers — do not assume column names.

Required per row:
- `ticker` / `name`
- `price` (per share)
- `eps_norm` — **normalized** earnings power per share (or normalized FCF/share). Must be mid-cycle / normalized, not raw trailing, or cyclicals produce garbage. If only raw is available, add a preprocessing step to normalize (e.g. multi-year average EPS, or average margin × current revenue).
- `growth_hist` — the business's demonstrated growth (revenue or EPS CAGR, ~5–10yr). The reference for "what it has actually done."
- `roic` — return on invested capital (ROCE/ROE acceptable as proxy). Sets the sustainable-growth ceiling and disambiguates the side flag.
- `nav_ps` — book or NAV per share (the asset-play reference).

Optional:
- `div_ps` / dividend yield — for a dividend-PV adjustment; may be skipped in v1.
- `sector` / cyclicality marker — helps route asset/cyclical names.

## Core computation (per company)

**Step 1 — invert price into implied growth, at both horizons.**
Forward model: `Price = eps_norm * (1+g)^N * exit_multiple_ref / (1+r)^N`
Solve for g:
```
g_implied(N) = ( (price * (1+r)**N) / (eps_norm * exit_multiple_ref) ) ** (1/N) - 1
```
Compute `g_short = g_implied(N1=12)` and `g_long = g_implied(N2=24)`.
Edge case: if `eps_norm <= 0`, the inversion is undefined — route the name to the asset/NAV lens (`side = ASSET_ONLY`), don't emit NaN or crash.

**Step 2 — reference and gap.**
```
ceiling = roic_based_sustainable_growth        # e.g. roic * assumed_reinvestment_rate
ref_growth = min(growth_hist, ceiling)          # sane cap on what to believe
gap = ref_growth - g_short                       # positive = market requires LESS than the business can deliver -> potentially cheap
disc_to_nav = (nav_ps - price) / nav_ps
```

**Step 3 — compounder diagnostic (two-horizon spread).**
```
duration_dependence = g_short - g_long           # large positive = value is long-duration; ~0 = near-term/cyclical
```
A name with a high `g_short` (looks "expensive") but a `g_long` its ROIC and history can support, plus large `duration_dependence`, is a long-duration compounder the single-horizon view would wrongly bin. This is the catch that stops the funnel missing the next Fortnox.

**Step 4 — side flag (two-sided classification).**
- `FEAR_PREMIUM` — low/negative `g_short`, or deep `disc_to_nav`. Market pricing pessimism.
- `UNDERPRICED_DURABILITY` — `g_short` looks high, **but** `roic >= roic_quality_cutoff`, `growth_hist` is supportive, and `g_long` is achievable. The "expensive" multiple hides conservative required growth relative to a proven franchise. *(The compounder catch.)*
- `EXPENSIVE_FRAGILE` — `g_long` still implausible and ROIC mediocre / no runway. Heroic assumptions on a business that can't deliver. De-prioritise / short-watch.
- `FAIR_NO_EDGE` — gap immaterial in both directions.
- `ASSET_ONLY` — no usable earnings base; judged on `disc_to_nav`.

**Step 5 — rank and surface.**
Rank by `abs(gap)` (and `disc_to_nav` for asset names) as `triage_priority`, grouped/filterable by `side`. Each row carries:
`ticker, price, g_short, g_long, ref_growth, roic, gap, disc_to_nav, side, hinge` — where `hinge` is a short generated string priming the human handoff, e.g. `"needs 18% growth for 12y; has done 7%"` or `"priced for ~4y of growth; franchise supports ~12y"`.

## Output
- A ranked CSV plus a printed top-N to console, sorted by `triage_priority`, filterable by `side`.
- Each row includes the `hinge` "what you'd have to believe" summary.
- No field that masquerades as a verdict.

## Out of scope for v1 (resist these)
No UI/web app (CLI + CSV is the deliverable). No third horizon. No per-company discount rate. No Monte Carlo. No automated buy/sell. No backtest harness. No live data fetching — assume the universe table is provided.

## Stack
Python + pandas. ~825 rows is trivial. One module + a config file, CSV in / CSV out. (Port to the existing .NET stack later if wanted — the math is a one-liner; build and validate the logic in Python first.)

## Validation the agent must build in
- Assert the same `r`, `N1`, `N2` are applied to every row.
- Handle `eps_norm <= 0`, missing `nav_ps`, missing `roic` gracefully — route, never crash.
- **Compounder non-drop test (required):** include a synthetic fixture for a high-ROIC business priced for long-duration growth. Assert it lands in `UNDERPRICED_DURABILITY` — *not* `EXPENSIVE_FRAGILE`. This test is the guardrail against the engine re-encoding the cheapness blind spot; it must pass.
- A cyclical fixture (value not duration-dependent) should show `duration_dependence ≈ 0` and route on its NAV/near-term figures, not on a flattering 12-year growth assumption.