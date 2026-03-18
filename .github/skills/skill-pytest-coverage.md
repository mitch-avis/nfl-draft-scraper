---
name: skill-pytest-coverage
description: Guides targeted pytest and coverage work for this repository's Python modules, especially when adding the first tests for data-cleaning, merge, or scraping helpers.
argument-hint: "[module or test path] [coverage target] [focus area]"
user-invocable: true
disable-model-invocation: false
---

# Skill: pytest Coverage

## When to Use

- Use when coverage for changed Python code is requested.
- Use after functional behavior is correct and the repo has pytest available.

## Current Repository Reality

- `pyproject.toml` has pytest and coverage settings.
- The `tests/` directory contains unit and integration tests at ~86% coverage.
- Dev dependencies include pytest and pytest-cov in `requirements-dev.in`.
- Every test class and test method must have a docstring.
- Never use `noqa` or similar directives to bypass linter checks in test code.

## Coverage Workflow

1. Add or update focused tests for the changed behavior.
2. Run only the relevant tests first.
3. Collect coverage for the touched module.
4. Inspect uncovered lines and add meaningful tests.
5. Re-run validation until the changed logic is adequately covered.

## Example Commands

```bash
pytest tests/test_big_board_combiner.py --cov=nfl_draft_scraper.big_board_combiner --cov-report=term-missing
pytest tests --cov=nfl_draft_scraper --cov-report=term-missing
```

For annotated reports:

```bash
pytest tests --cov=nfl_draft_scraper --cov-report=annotate:cov_annotate
```

## What to Prioritize

- fuzzy matching behavior
- column filtering and ordering logic
- AV calculation edge cases
- missing-data handling
- parser behavior on partial or malformed HTML

## Guardrails

- Prefer behavior-focused assertions over implementation-detail assertions.
- Do not create coverage-only tests with no practical regression value.
- Do not hit live websites in tests.
