---
name: skill-tdd
description: Applies red-green-refactor to this repository's Python pipeline, with emphasis on targeted tests, fixture or mock-based scraping tests, and honest validation when no tests exist yet.
argument-hint: "[feature or bug] [target module] [expected behavior]"
user-invocable: true
disable-model-invocation: false
---

# Skill: Test-Driven Development

## When to Use

- Use whenever behavior is added, changed, or fixed.
- Use before refactors that could affect scraping, CSV schemas, fuzzy matching, or AV enrichment.

## Current Repository Reality

- `pyproject.toml` includes pytest configuration.
- The `tests/` directory contains unit and integration tests at ~86% coverage.
- Every test class and test method must have a docstring.
- Never use `noqa` or similar directives to bypass linter checks. Fix the underlying issue instead.
- When adding tests, adhere to all Ruff linters enabled in `pyproject.toml`.

## TDD Workflow

1. Write or update a test that captures the desired behavior.
2. Run that test and confirm the failure is expected.
3. Make the smallest code change that fixes the failure.
4. Re-run the targeted test.
5. Run Ruff and Pyright validation.
6. Expand test scope only as needed.

## Test Design Guidance

- Prefer unit tests for pure transformation logic such as cleaning, fuzzy matching, or column
    ordering.
- For scraper code, prefer mocked HTTP responses, saved HTML fixtures, or mocked Selenium
    interactions.
- Do not hit live sites in automated tests.
- Cover behavior and edge cases, not just happy-path execution.

## Suggested Test Layout

If tests are added, prefer a simple structure such as:

```text
tests/
    test_big_board_combiner.py
    test_merge_bb_ranks_to_picks.py
    test_scrape_av.py
```

Add subdirectories only when the suite grows enough to justify them.

## Practical Commands

```bash
pytest tests/test_big_board_combiner.py -q
pytest tests -q
```

## Definition of Done

- Changed behavior is covered by focused tests when practical.
- Validation was actually run and reported honestly.
- No automated test depends on live network access.
