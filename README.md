# NFL Draft Scraper

A Python package for scraping, cleaning, and analyzing NFL draft data. Collects big board rankings
from multiple sources, merges them with actual draft picks, and enriches the data with Approximate
Value (AV) metrics from Pro Football Reference.

## Table of Contents

- [NFL Draft Scraper](#nfl-draft-scraper)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Project Structure](#project-structure)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Usage](#usage)
    - [1. Scrape Big Boards](#1-scrape-big-boards)
    - [2. Combine Big Boards](#2-combine-big-boards)
    - [3. Clean Draft Picks](#3-clean-draft-picks)
    - [4. Scrape Approximate Value](#4-scrape-approximate-value)
    - [5. Merge Big Board Ranks to Picks](#5-merge-big-board-ranks-to-picks)
  - [Development](#development)
    - [Setup](#setup)
    - [Linting and Formatting](#linting-and-formatting)
    - [Testing](#testing)
  - [Data Pipeline](#data-pipeline)
  - [License](#license)

## Features

- **Multi-source big board scraping** — collects prospect rankings from NFL Mock Draft Database
  (MDDB) and Jacklich10 (JLBB)
- **Fuzzy name matching** — reconciles player names across sources using `difflib` similarity
  matching
- **Combined rankings** — averages rankings from multiple big boards into a unified prospect ranking
- **Draft picks cleaning** — filters and normalizes raw draft pick data
- **Approximate Value enrichment** — retrieves AV stats via `sportsipy` with Pro Football Reference
  HTML fallback
- **Weighted career AV** — computes a time-discounted career AV giving 5% less weight to each
  subsequent year
- **Checkpoint/resume** — AV scraping saves periodic checkpoints to allow resuming interrupted runs

## Project Structure

```text
nfl-draft-scraper/
├── src/
│   └── nfl_draft_scraper/
│       ├── __init__.py
│       ├── constants.py              # Paths, year range, base URLs
│       ├── big_board_combiner.py     # Combine MDDB + JLBB boards
│       ├── draft_picks_cleaner.py    # Clean raw draft picks CSV
│       ├── jl_bb_scraper.py          # Selenium scraper for jacklich10.com
│       ├── mddb_bb_scraper.py        # Requests scraper for nflmockdraftdatabase.com
│       ├── merge_bb_ranks_to_picks.py # Merge big board ranks into draft picks
│       ├── scrape_av.py              # AV enrichment via sportsipy + PFR fallback
│       └── utils/
│           ├── __init__.py
│           ├── csv_utils.py          # CSV read/write helpers
│           ├── logger.py             # Colored logging configuration
│           └── webdriver_utils.py    # Selenium WebDriver management
├── tests/                            # Unit and integration tests
├── data/                             # Generated CSV output (gitignored)
├── wheels/                           # Local Python wheel dependencies
├── pyproject.toml                    # Build config, tool settings
├── requirements.in                   # Runtime dependency pins
├── requirements-dev.in               # Development dependency pins
└── AGENTS.md                         # AI agent instructions
```

## Prerequisites

- **Python 3.12+**
- **Firefox** and **geckodriver** (for Selenium-based scrapers)
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

## Installation

```bash
# Clone the repository
git clone https://github.com/mitch-avis/nfl-draft-scraper.git
cd nfl-draft-scraper

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install runtime dependencies
pip install -r requirements.txt

# Install the package in editable mode
pip install -e .
```

## Usage

Each pipeline stage can be run as a standalone script. All output CSVs are written to the `data/`
directory.

### 1. Scrape Big Boards

Scrape prospect rankings from NFL Mock Draft Database:

```bash
python -m nfl_draft_scraper.mddb_bb_scraper
```

Scrape prospect rankings from Jacklich10 (requires Firefox + geckodriver):

```bash
python -m nfl_draft_scraper.jl_bb_scraper
```

### 2. Combine Big Boards

Fuzzy-match and merge MDDB and JLBB rankings into a single combined board per year:

```bash
python -m nfl_draft_scraper.big_board_combiner
```

### 3. Clean Draft Picks

Filter and normalize the raw `draft_picks.csv` into cleaned format:

```bash
python -m nfl_draft_scraper.draft_picks_cleaner
```

### 4. Scrape Approximate Value

Enrich draft picks with per-season AV data from sportsipy / Pro Football Reference. Reads
`cleaned_draft_picks.csv` and writes `cleaned_draft_picks_with_av.csv`.

```bash
# Default run — process only incomplete rows, checkpoint every 20 players
python -m nfl_draft_scraper.scrape_av

# Force a full refresh of every player (re-scrapes already-complete rows)
python -m nfl_draft_scraper.scrape_av --force

# Change checkpoint frequency (save every 50 players instead of 20)
python -m nfl_draft_scraper.scrape_av --checkpoint-every 50

# Combine both options
python -m nfl_draft_scraper.scrape_av --force --checkpoint-every 50
```

**CLI options:**

- `--force` — Re-scrape every player, even rows already marked complete. Useful for refreshing stale
  AV after a season ends.
- `--checkpoint-every N` (default: 20) — Save a checkpoint CSV after every N players processed.

**How it works:** For each player in the cleaned draft picks, the script fetches year-by-year AV via
the `sportsipy` library. If sportsipy fails, it falls back to direct HTML scraping of the player's
Pro Football Reference page. A 3-second delay is enforced between requests on both paths to avoid
rate limiting. Progress is saved to a checkpoint file (`cleaned_draft_picks_with_av_checkpoint.csv`)
so interrupted runs can resume where they left off.

**Output columns added:** One column per season (e.g. `2018` through `2025`), plus `career` (sum of
yearly AVs) and `weighted_career` (weighted sum using PFR's formula: best season at 100%,
second-best at 95%, third at 90%, etc.).

### 5. Merge Big Board Ranks to Picks

Merge combined big board rankings into the AV-enriched draft picks:

```bash
python -m nfl_draft_scraper.merge_bb_ranks_to_picks
```

## Development

### Setup

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Or install with dev extras
pip install -e ".[dev]"
```

### Linting and Formatting

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type check
pyright

# Lint Markdown
markdownlint .
```

### Testing

```bash
# Run tests with coverage
pytest

# Run tests for a specific file
pytest tests/test_big_board_combiner.py

# Run with verbose output
pytest -v
```

## Data Pipeline

The full pipeline runs in this order:

```text
1. mddb_bb_scraper    → data/mddb_big_board_{year}.csv
2. jl_bb_scraper      → data/jlbb_big_board_{year}.csv
3. big_board_combiner → data/combined_big_board_{year}.csv
4. draft_picks_cleaner → data/cleaned_draft_picks.csv
5. scrape_av          → data/cleaned_draft_picks_with_av.csv
6. merge_bb_ranks     → data/draft_picks_with_big_board_ranks_{year}.csv
```

Each stage reads from the output of previous stages. Steps 1–2 can run in parallel. Steps 4–5 can
run independently of steps 1–3.

## License

This project is licensed under the MIT License.
