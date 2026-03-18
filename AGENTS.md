# AGENTS.md

This file defines repository-wide working instructions for `nfl-draft-scraper`.

## 1. Project Scope

- This repository is a single Python project.
- It is a personal-use data pipeline for collecting and merging NFL draft data.
- Do not assume a public package, GitHub release workflow, or multi-language roadmap.
- Do not introduce Rust-specific guidance or conversion plans unless explicitly requested.

## 2. Project Purpose

The codebase builds draft-analysis CSVs for NFL seasons `2018` through `2025` by combining several
data sources:

- consensus big board rankings from Mock Draft Database
- big board rankings from Jack Lichtenstein's board site
- cleaned draft pick data from a local CSV
- Approximate Value data from `sportsipy` with a direct Pro-Football-Reference fallback

The end products in `data/` include:

- per-source scraped big board CSVs
- combined big board CSVs
- cleaned draft pick CSVs
- cleaned draft picks enriched with AV
- per-year draft picks merged with big board ranks

## 3. Current Repository Layout

Top-level areas that matter:

- `src/nfl_draft_scraper/`: Python package with the scraping and merge scripts (src layout)
- `src/nfl_draft_scraper/utils/`: CSV, logging, and Selenium helpers
- `tests/`: pytest test suite with unit and integration tests
- `data/`: generated CSV inputs and outputs
- `wheels/`: pinned local `sportsipy` wheels
- `.github/skills/`: workspace skill documents used by coding agents
- `pyproject.toml`: project metadata plus Ruff, Pyright, and pytest configuration

Important modules:

- `mddb_bb_scraper.py`: scrape Mock Draft Database consensus big boards with `requests`
- `jl_bb_scraper.py`: scrape Jack Lichtenstein big boards with Selenium
- `big_board_combiner.py`: clean and fuzzy-match both sources into one combined board per year
- `draft_picks_cleaner.py`: reduce raw draft picks CSV to the fields used downstream
- `scrape_av.py`: enrich cleaned draft picks with yearly/career/weighted AV
- `merge_bb_ranks_to_picks.py`: merge combined big board ranks into cleaned picks with AV

## 4. Environment and Tooling

- Python version target is `3.12+`.
- Use the repository root virtual environment at `.venv/` for Python tooling.
- Preferred formatting workflow is Ruff, not Black.

Primary validation order:

1. `ruff format .`
2. `ruff check .`
3. `pyright .`
4. `markdownlint .`
5. `pytest`

Notes:

- `pyproject.toml` contains pytest and coverage settings.
- `requirements-dev.in` includes Ruff, Pyright, pytest, and pytest-cov.
- The `tests/` directory contains unit and integration tests at ~86% coverage.
- If a task introduces or updates tests, keep documentation consistent with the actual installed
  tooling or update dev dependencies as part of the same task.

## 5. Working Style for This Repository

- Prefer small, targeted edits over broad refactors.
- Preserve existing CSV schemas unless a task explicitly requires changing them.
- Treat files in `data/` as pipeline artifacts; do not rewrite or regenerate them unless the task
  requires it.
- Keep code pragmatic and script-friendly. This repo is primarily a local data workflow, not a
  framework.
- Avoid adding unnecessary abstraction layers or speculative extension points.

## 6. Python Code Guidelines

When editing Python in `src/nfl_draft_scraper/` or `tests/`:

- **Every module, class, and function must have a docstring.** This includes test methods.
- **Never use `noqa`, `type: ignore`, `pylint: disable`, or similar directives** to bypass linter
  or type-checker rules. Fix the underlying issue instead. If a suppression is truly unavoidable,
  explain why in a comment on the same line.
- Adhere to all Ruff linters enabled in `pyproject.toml` (B, C4, D, E, F, I, N, S, SIM, UP, W,
  NPY, PGH). The only per-file exception is S101 (assert) in `tests/`.
- Use `from __future__ import annotations` in new or materially rewritten modules.
- Add type hints for new or changed functions when practical.
- Prefer absolute imports from `nfl_draft_scraper`.
- Catch specific exceptions instead of using bare `except:`.
- Keep side effects explicit, especially around network calls and CSV writes.
- Preserve the current package style unless the task explicitly includes a cleanup or refactor.

## 7. Data Pipeline Guardrails

- Downstream scripts depend on the column names written by upstream scripts.
- Before changing a CSV column name, order, or null-handling rule, verify how later stages consume
  it.
- Fuzzy matching logic is part of the current workflow. When changing it, verify merged output
  assumptions carefully.
- `scrape_av.py` depends on the local `sportsipy` wheel and also performs direct PFR scraping as a
  fallback.

## 8. Scraping Guidance

- The project scrapes third-party public sites. Be conservative with request volume.
- Do not add parallel scraping against the same site.
- Keep delays and retries explicit.
- Prefer stable selectors over brittle absolute XPaths when touching scraper logic.
- Do not build CI or automated tests that hit live websites.

Current external sources:

- `nflmockdraftdatabase.com`
- `jacklich10.com`
- `pro-football-reference.com`

## 9. Testing Expectations

- Follow TDD when changing behavior: add or update tests alongside code.
- The `tests/` directory contains unit and integration tests at ~86% coverage.
- Every test class and test method must have a docstring.
- Avoid live network access in tests; use mocks, fixtures, or saved HTML.
- Run `pytest` after code changes to verify nothing is broken.
- For scraper logic, prefer saved HTML fixtures or mocked responses over real requests.
- Run targeted validation for the files you changed. Do not claim broader test coverage than what
  was actually run.

## 10. Documentation Expectations

- Keep `AGENTS.md` and `.github/skills/` aligned with the current repository reality.
- Do not reference non-existent workspace folders, languages, CI flows, or release processes.
- If you change developer workflow, update the relevant skill documents in the same task.
