---
name: skill-git-commit
description: Creates clean local git commits for this repository using conventional commit messages and safe staging practices. Use when asked to prepare or make a commit.
argument-hint: "[optional type/scope] [commit intent] [files or grouping notes]"
user-invocable: true
disable-model-invocation: false
---

# Skill: Git Commit

## When to Use

- Use when the user asks to commit changes.
- Use after validation for the affected files is complete.

## Repository Context

- This repo may be local-only and may not have a remote yet.
- Do not assume GitHub PR workflow, issue linking, or release automation.
- Focus on clean local history and safe staging.

## Commit Format

Follow Conventional Commits:

```text
<type>[optional scope]: <description>
```

Examples:

- `fix(scraper): handle missing AV table gracefully`
- `docs(skills): align agent docs with local pipeline`
- `refactor(combiner): simplify fuzzy match record lookup`

## Suggested Types

- `feat`
- `fix`
- `docs`
- `refactor`
- `test`
- `build`
- `chore`

## Workflow

1. Inspect status with `git status --short`.
2. Review the relevant diff with `git diff` or `git diff --staged`.
3. Stage only the files that belong to the same logical change.
4. Commit with a concise conventional message.

## Safety Rules

- Never commit secrets, `.env`, credentials, or unrelated generated artifacts.
- Never use destructive git commands without explicit user approval.
- Do not amend commits unless the user explicitly asks.
- If validation is incomplete or intentionally skipped, say so before committing.
