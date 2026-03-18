---
name: skill-web-scraping
description: Covers the scraping patterns used in nfl-draft-scraper, including requests, Selenium, respectful request pacing, fallback scraping from Pro-Football-Reference, and fixture-friendly validation.
argument-hint: "[source site] [scraper or parser change] [validation approach]"
user-invocable: true
disable-model-invocation: false
---

# Skill: Web Scraping

## When to Use

- Use when changing scraper logic, selectors, request pacing, or fallback HTML parsing.
- Use when modifying these modules:
  - `mddb_bb_scraper.py`
  - `jl_bb_scraper.py`
  - `scrape_av.py`

## Current Sources

This repository currently scrapes or derives data from:

- `nflmockdraftdatabase.com`
- `jacklich10.com`
- `pro-football-reference.com`
- `sportsipy` data that ultimately comes from Pro-Football-Reference

## Repository-Specific Patterns

### Mock Draft Database

- Uses `requests` plus `lxml.html`.
- Parses list items with player rank, name, position, and school.

### Jack Lichtenstein Big Board

- Uses Selenium with Firefox in headless mode.
- Current implementation relies on page interaction and pagination.
- Current selectors include brittle absolute XPaths. If you touch them, prefer more resilient
  selectors when possible.

### AV Fallback Scrape

- Uses `requests` plus `BeautifulSoup` against player pages on Pro-Football-Reference.
- Chooses a table id based on the player's position category.
- Must handle missing tables and partial data safely.

## Scraping Rules

- Be conservative with request volume.
- Keep explicit sleeps or pacing around repeated requests.
- Do not parallelize live requests to the same site.
- Use timeouts on HTTP requests.
- Handle missing elements defensively.
- Preserve current CSV output schemas unless the task explicitly changes them.

## Testing and Validation

- Do not build automated tests that hit live websites.
- Prefer saved HTML fixtures, mocked responses, or mocked browser interactions.
- When changing parser behavior, validate against representative saved input where possible.
- If no fixtures exist yet, do not guess at large structural changes without first capturing enough
  context from the current site.

## Change Risks to Check

- changed class names or DOM structure on source sites
- missing pagination or disabled next-button handling
- renamed or missing AV table ids on PFR pages
- altered CSV column names that break downstream merge steps
