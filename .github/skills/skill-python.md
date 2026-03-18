---
name: skill-python
description: Guides Python work in nfl-draft-scraper, including scraping scripts, CSV pipeline constraints, typing, and root-.venv validation. Use when editing code under nfl_draft_scraper/.
argument-hint: "[python file or module] [change goal] [validation scope]"
user-invocable: true
disable-model-invocation: false
---

# Skill: Python

## When to Use

- Use for any edits under `src/nfl_draft_scraper/` or `tests/`.
- Use for scraper changes, CSV pipeline changes, fuzzy matching logic, or AV enrichment logic.

## Environment

- Use Python `3.12+`.
- Run tooling from the repository root virtual environment at `.venv/`.

Validation order:

1. `ruff format .`
2. `ruff check .`
3. `pyright .`
4. `markdownlint .`
5. `pytest`

## Project-Specific Architecture

This repo is a local data pipeline, not a general-purpose framework.

Core patterns:

- each script performs one stage of the draft-data workflow
- `data/` contains intermediate and final CSV artifacts
- later stages depend on the schemas produced by earlier stages
- network and file-system side effects are expected, but should remain obvious in the code

Key modules:

- `mddb_bb_scraper.py`
- `jl_bb_scraper.py`
- `big_board_combiner.py`
- `draft_picks_cleaner.py`
- `scrape_av.py`
- `merge_bb_ranks_to_picks.py`

## Coding Guidelines

- **Every module, class, and function must have a docstring.** This includes test methods.
- **Never use `noqa`, `type: ignore`, `pylint: disable`, or similar directives** to bypass linter
  or type-checker rules. Fix the underlying issue instead. If a suppression is truly unavoidable,
  explain why in a comment on the same line.
- Adhere to all Ruff linters enabled in `pyproject.toml` (B, C4, D, E, F, I, N, S, SIM, UP, W,
  NPY, PGH). The only per-file exception is S101 (assert) in `tests/`.
- Prefer small functions with explicit inputs and outputs.
- Preserve existing behavior unless the task requires a behavior change.
- Prefer absolute imports from `nfl_draft_scraper`.
- Use specific exceptions.
- Add type hints for new or changed functions when practical.
- Avoid over-abstracting straightforward script logic.

## DataFrame and CSV Guidance

- Pandas is the primary DataFrame tool in the current code.
- Do not migrate modules to Polars unless the task explicitly requires it.
- Preserve CSV column names, ordering, and null conventions unless downstream consumers are updated
  in the same change.
- Check dependent scripts before renaming fields such as player names, ranks, AV columns, or season
  fields.

## Scraping and External Data

- Keep request volume conservative.
- Do not parallelize scrapes against the same site.
- Prefer stable selectors and simple parsing logic.
- Selenium is already used for Jack Lichtenstein's site; do not replace it unless there is a clear
  benefit and the task requires it.
- Avoid live-network dependence in tests.

## Validation Expectations

- Run the relevant Ruff, Pyright, and markdownlint checks after code changes.
- Run `pytest` after code changes to verify nothing is broken.
- If a task changes behavior, add or update tests.
