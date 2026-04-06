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
    - [Pipeline Orchestrator](#pipeline-orchestrator)
    - [Individual Stages](#individual-stages)
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
├── nfl_draft_scraper/
│   ├── __init__.py
│   ├── constants.py              # Paths, year range, base URLs
│   ├── big_board_combiner.py     # Combine MDDB + JLBB boards
│   ├── draft_picks_cleaner.py    # Clean raw draft picks CSV
│   ├── jl_bb_scraper.py          # Playwright scraper for jacklich10.com
│   ├── mddb_bb_scraper.py        # Requests + JSON scraper for nflmockdraftdatabase.com
│   ├── merge_bb_ranks_to_picks.py # Merge big board ranks into draft picks
│   ├── pipeline.py               # Pipeline orchestrator (smart re-run)
│   ├── scrape_av.py              # AV enrichment via sportsipy + PFR fallback
│   └── utils/
│       ├── __init__.py
│       ├── csv_utils.py          # CSV read/write helpers
│       └── logger.py             # Colored logging configuration
├── tests/                        # Unit and integration tests (~109 tests)
├── data/                         # Generated CSV output
├── wheels/                       # Local sportsipy wheel
├── pyproject.toml                # Build config, tool settings
├── requirements.in               # Runtime dependency pins
├── requirements-dev.in           # Development dependency pins
└── AGENTS.md                     # AI agent instructions
```

## Prerequisites

- **Python 3.12+**
- **Playwright** Chromium browser (installed automatically via `playwright install chromium`)
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

## Installation

```bash
# Clone the repository
git clone https://github.com/mitch-avis/nfl-draft-scraper.git
cd nfl-draft-scraper

# Create a virtual environment and install dependencies
uv venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt
uv pip install -r requirements-dev.txt
uv pip install -e .

# Install Playwright browser
playwright install chromium
```

## Usage

### Pipeline Orchestrator

The recommended way to run the full data pipeline is via the orchestrator. It checks which output
files already exist and only runs the stages that are needed:

```bash
# Run all stages (skips stages whose output already exists)
python -m nfl_draft_scraper.pipeline

# Force re-run of all stages
python -m nfl_draft_scraper.pipeline --force

# Run specific stages only
python -m nfl_draft_scraper.pipeline scrape-mddb combine merge

# Force a specific stage
python -m nfl_draft_scraper.pipeline scrape-av --force
```

**Available stages (in dependency order):**

| Stage | Description | Output |
| --- | --- | --- |
| `scrape-mddb` | Scrape MDDB consensus big boards | `mddb_big_board_{year}.csv` |
| `scrape-jlbb` | Scrape JLBB big boards (Playwright) | `jl_big_board_{year}.csv` |
| `combine` | Fuzzy-match and merge both sources | `combined_big_board_{year}.csv` |
| `clean-picks` | Clean raw draft picks CSV | `cleaned_draft_picks.csv` |
| `scrape-av` | Enrich picks with AV data | `cleaned_draft_picks_with_av.csv` |
| `merge` | Merge big board ranks into picks | `draft_picks_with_big_board_ranks_{year}.csv` |

### Individual Stages

Each stage can also be run as a standalone script:

```bash
python -m nfl_draft_scraper.mddb_bb_scraper       # MDDB big boards
python -m nfl_draft_scraper.jl_bb_scraper          # JLBB big boards (Playwright)
python -m nfl_draft_scraper.big_board_combiner     # Combine boards
python -m nfl_draft_scraper.draft_picks_cleaner    # Clean draft picks
python -m nfl_draft_scraper.scrape_av              # Scrape AV (--force, --checkpoint-every N)
python -m nfl_draft_scraper.merge_bb_ranks_to_picks # Merge ranks into picks
```

The AV scraper supports additional options:

- `--force` — Re-scrape every player, even rows already marked complete.
- `--checkpoint-every N` (default: 20) — Save a checkpoint CSV after every N players processed.

## Development

### Setup

```bash
# Install development dependencies
uv pip install -r requirements-dev.txt
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

The pipeline orchestrator (`pipeline.py`) runs stages in this order, skipping any whose output
already exists:

```text
1. scrape-mddb     → data/mddb_big_board_{year}.csv
2. scrape-jlbb     → data/jl_big_board_{year}.csv
3. combine         → data/combined_big_board_{year}.csv
4. clean-picks     → data/cleaned_draft_picks.csv
5. scrape-av       → data/cleaned_draft_picks_with_av.csv
6. merge           → data/draft_picks_with_big_board_ranks_{year}.csv
```

Each stage reads from the output of previous stages. Steps 1–2 can run in parallel. Steps 4–5 can
run independently of steps 1–3.

**Final output columns** (per-year CSVs): `round`, `round_pick`, `overall_pick`, `team`,
`pfr_player_id`, `player`, `position`, `category`, `college`, `MDDB Rank`, `JLBB Rank`, `AvgRank`,
plus per-season AV columns, `career`, and `weighted_career`.

## License

This project is licensed under the MIT License.
