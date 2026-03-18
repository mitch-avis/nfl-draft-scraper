---
name: skill-markdown
description: Keeps Markdown and instruction files accurate for this repository's actual Python-only workflow. Use when editing AGENTS.md, skill files, or other project documentation.
argument-hint: "[markdown path(s)] [documentation objective]"
user-invocable: true
disable-model-invocation: false
---

# Skill: Markdown and Documentation

## When to Use

- Use for any Markdown edits in this repository.
- Always use for `AGENTS.md` and files under `.github/skills/`.
- Use when developer workflow, tooling, or repository structure changes.

## Repository Documentation Rules

- Keep docs aligned with the current repo, not with a template or another project.
- This is a single Python project for local and personal use.
- Do not mention Rust, multi-root workspace routing, public release process, or GitHub PR workflow
  unless the task explicitly adds them.
- Do not document files, directories, tests, or commands that do not exist.

## Markdown Quality Gate

- Lint every Markdown file you edit with `markdownlint` when the command is available.
- If `markdownlint` is not installed in the environment, note that explicitly in the final report.
- Fix markdownlint issues before considering the work complete.
- Never use `<!-- markdownlint-disable -->` or similar directives to bypass lint rules. Fix the
  underlying issue instead.

## Style Guidelines

- Prefer short sections with descriptive headers.
- Keep wording direct and imperative.
- Keep examples accurate and executable.
- Avoid placeholder documentation and aspirational roadmap language unless the user asked for it.

## AI Instruction Files

Treat these as first-class documentation:

- `AGENTS.md`
- files in `.github/skills/`
- any future instruction or prompt files added to the repo

When editing instruction files:

- keep rules specific to this repository
- remove copied guidance that does not apply
- update related files together when workflows change

## Common Files in This Repo

### AGENTS.md

- Keep it focused on repository-wide behavior.
- Document the real package layout under `nfl_draft_scraper/`.
- Document the root `.venv` workflow and Ruff-first validation order.

### Skill Files

- Keep each skill narrow and action-oriented.
- Avoid overlap where possible.
- Cross-reference repository realities such as missing tests or local-only usage when relevant.

### README.md / CHANGELOG.md / TODO.md

- Only update these if they exist or the task explicitly creates them.
- Do not assume this repository already uses a release changelog or public-facing README workflow.
