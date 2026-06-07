# Route Profitability — Findings & Verdict

Generated from the Gold tables built by the pipeline. Supporting data is in this
folder; the full walk-through is [`analysis.html`](analysis.html).

## Headline

Of **120 routes**, **22 are underperforming** and **98 are OK**. Underperformance is
**concentrated** (a fifth of the fleet) and **structural**, not bad luck.

| Verdict | Routes | Action |
|---|---:|---|
| **Tier 1 — Loss-making** | 4 | Re-price or cut |
| **Tier 2 — Margin leak** | 18 | Optimise (routing, sequencing, disposal) |
| OK | 98 | — |

## How "underperforming" is defined

A flat margin threshold would be indefensible: margins differ **structurally** by
material (General Waste median **35.6%** vs Cardboard **76.5%** — a ~40-point gap).
So each route is judged **against its own cohort** (`waste_stream × customer_segment`)
and on **persistence**:

- **Below-peer day** — a route-day whose `gross_margin_pct` is below its cohort median.
- A route is flagged when it is below-peer on **>70%** of its days.
- **Tier 1** if it is also loss-making (median gross profit < 0, or it loses money on
  >50% of days); otherwise **Tier 2**.

See [`route_scorecard.csv`](route_scorecard.csv) for all 120 routes and
[`underperforming_routes.csv`](underperforming_routes.csv) for the flagged 22.

## Tier 1 — Loss-making (re-price or cut)

All four lose money at the median and are in the **General Waste** stream:

| Route | Cohort | Median margin | Loss-day rate |
|---|---|---:|---:|
| RTE-0114 | General Waste · Retail | −14.8% | 75% |
| RTE-0035 | General Waste · Office & Commercial | −10.6% | 78% |
| RTE-0061 | General Waste · Retail | −5.5% | 58% |
| RTE-0059 | General Waste · Office & Commercial | −2.1% | 52% |

These don't have a pricing *leak* — they have a pricing *problem*. They lose money on
the majority of their operating days. The recommendation is to **re-price the contracts
or exit them**; operational tuning won't close a negative median margin.

## Tier 2 — Margin leak (optimise)

18 routes are profitable but sit chronically below their cohort peers (>70% of days).
The gap is operational — routing, stop sequencing, or disposal cost — so the lever is
**efficiency**, not price. Per-route detail is in `underperforming_routes.csv`.

## Why this is structural, not episodic

- **717 loss-days** (6% of all route-days), but only **3.5%** of them coincide with an
  incident, and maintenance cost on loss-days is in line with normal days.
- Losses are baked into the route economics, not caused by one-off events — which is why
  the fix is pricing / route redesign rather than chasing incidents.

## Primary cost driver behind low-margin route-days

Decomposing `total_cost` into its components points to a single lever:

| Cost component | Share on loss-days | Share on profitable days | Avg £/day (loss vs profit) |
|---|---:|---:|---|
| **disposal_cost** | **75.3%** | 65.2% | **£4,257 vs £2,696** |
| labour_cost | 15.4% | 19.1% | £869 vs £791 |
| fuel_cost | 4.4% | 5.5% | £246 vs £228 |
| admin_cost | 3.5% | 8.3% | £199 vs £342 |
| maintenance_cost | 1.5% | 1.9% | £84 vs £79 |

**Disposal cost is the driver.** It is ~75% of cost on loss-days vs ~65% on profitable
days, and the average disposal bill is **~£4,257/day vs ~£2,696** — while loss-days earn
barely half the revenue (~£4,879 vs ~£8,517). Fuel, labour and maintenance are essentially
flat. Low-margin route-days are a **disposal-cost-vs-revenue** problem (heavy/low-value
material or under-priced tipping), not a fleet-efficiency one — which is why the Tier 1 fix
is **re-pricing**, not routing.

## 3-year trend — improving, not deteriorating

Revenue-weighted margin (the correct way to aggregate a ratio), 2022–2024:

| Year | Margin | Loss-day rate |
|---|---:|---:|
| 2022 | 48.3% | 6.8% |
| 2023 | 49.1% | 5.8% |
| 2024 | 49.8% | 5.4% |

Fleet margin drifts **up** and the loss-day rate **falls** year over year; the quarterly
series is stable with a slight upward trend (ending Q4-2024 at ~50.1%). The fleet as a whole
is healthy and slowly improving — which reinforces that the problem is **concentrated** in
the 22 flagged routes, not a system-wide decline.

## Files in this folder

| File | Contents |
|---|---|
| `analysis.html` | Rendered analysis notebook (full evidence) |
| `route_scorecard.csv` | All 120 routes with their tier verdict |
| `underperforming_routes.csv` | The 22 flagged routes |
| `cohort_margins.csv` | Median margin per cohort (the peer benchmark) |
| `waste_stream_margins.csv` | Median margin by waste stream (the structural gap) |
