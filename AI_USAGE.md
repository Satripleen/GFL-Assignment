# AI Usage Disclosure — GFL Commercial: Route Profitability

## Tool used
I used **Claude Code (Claude Opus)** as a **pair programmer** throughout this
project. The design & direction, implementation methodology, requirements interpretation and tasks breakdown philosophy were mine; the AI executed, generated boilerplate, and helped me move faster while I kept control of the decisions.

## What I used it for
- **PySpark + Delta Lake code** — the heavy boilerplate (Spark/Delta session
  setup, the enforced schemas, the idempotent `MERGE` helper) and the medallion
  layer builds (Bronze → Silver → Gold).
- **The scorecard logic** — the Part 2 route-profitability verdict.
- **The analysis notebook** — Part 2 evidence.
- **Documentation** — README, `spec.md`, `tasks.md`, the architecture/ERD
  diagram, docstrings.
- **Containerisation** — Dockerfile and docker-compose.
- **Tests** — the pytest suite covering every layer.
- **Cross-session code review** — I used a **separate, fresh AI session** to
  review the code produced in each task's implementation session, so the review
  was done with a clean context rather than by the same session that wrote the
  code.

## Decisions I owned (the AI executed them, it did not choose them)
- **Design first, then AI.** I made the design decisions myself, then asked
  Claude to structure the work as **spec- and task-driven development**. I wanted
  each task documented with *what* it does and *why*, so that in future I know
  exactly **where to make changes without touching the entire codebase**.
- **Two-tier classification.** I deliberately kept the analysis to **two tiers**
  rather than a finer-grained scheme. In this PySpark setup (small dataset, small
  partitions) more granularity would make the process **slower and messier** for
  little analytical gain.
- **Star schema.** I chose a **star schema** — there was no reason to
  over-complicate the model; a star is clean and sufficient.
- **Recompute in code, don't trust the source.** I decided to **recompute** the
  cost/profit/margin figures in code rather than trust the source file, because
  the source can contain **gibberish values**. (Original source values are
  retained as `*_src` with a `recon_flag` so any mismatch surfaces instead of
  flowing through.)
- **Architecture and folder structure.** I told Claude exactly how to lay out the
  project — where each piece of code lives (`lib/`, `src/`, `pipeline/`, `docs/`,
  `output/`) — so the project stays **scalable and clean**.
- **Reproducibility via Colima.** I chose to run the container workflow with
  **Colima** instead of Docker Desktop, so the project runs **without needing to
  install Docker Desktop** on the system.

## Where I directed, corrected, or overrode the AI
- **Authorship.** I enforced my own authorship on the commits rather than
  accepting any auto-generated AI attribution.
- **Gating changes.** When the AI was about to apply a configuration change, I
  **held it for review and discussion** rather than letting it apply
  automatically.
- **Validation over blind acceptance.** I did not take AI output on trust. The
  test suite was **run and iterated** until it passed — the AI's first draft had
  three real bugs (an in-place schema mutation, a type-inference failure on an
  all-null column, and a non-nullable-key conflict) that were caught and fixed by
  **actually executing the tests**, not by assuming the generated code was
  correct.

## Engineering quality (built under my direction)
- **Idempotency** via Delta `MERGE` on the business key, so re-runs are safe and
  row counts stay stable.
- **Defensive Silver layer** — guarded division (no divide-by-zero crashes), a
  **quarantine** table for bad rows instead of silently dropping them, and a
  reconciliation flag.
- **SCD Type 1 verified, not assumed** — a strict-hierarchy assertion fails
  loudly if a route's geography ever changes.
- **Correct ratio aggregation** — a **revenue-weighted** monthly margin, not a
  naive average of daily percentages.
- **Validated thresholds** — the tiering thresholds were pinned by **reproducing
  the spec's published figure** (22 of 120 routes below cohort).

## Summary
AI was a **force multiplier**, not the decision-maker. I owned the design, the
data-modelling choices, the architecture, and the analytical approach; I used
Claude Code to implement them quickly and to keep the documentation and tests
thorough. Every AI contribution was directed by me and validated by execution
before it was accepted.
