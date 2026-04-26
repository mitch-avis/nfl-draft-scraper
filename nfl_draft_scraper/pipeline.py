"""Pipeline orchestrator for the NFL Draft Scraper.

Coordinates the full data pipeline with on-demand logic: checks which data already exists and only
runs the stages that are needed (or that the user explicitly requests).

Stages, in dependency order:
  1. scrape-mddb   — scrape MDDB big boards
  2. scrape-jlbb   — scrape JLBB big boards (Playwright)
  3. combine        — combine MDDB + JLBB into unified boards
  4. clean-picks    — clean the raw draft_picks.csv
  5. scrape-av      — enrich cleaned picks with AV data
  6. merge          — merge big board ranks into AV-enriched picks

Running with no arguments performs a smart run that skips stages whose output files already exist.
Use ``--force`` to re-run everything, or pass specific stage names to run only those stages.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.logger import log

STAGES = ("scrape-mddb", "scrape-jlbb", "combine", "clean-picks", "scrape-av", "merge")


def _mddb_files_exist() -> bool:
    """Return True if all MDDB big board CSVs exist for the year range."""
    return all(
        (constants.DATA_PATH / f"mddb_big_board_{y}.csv").exists()
        for y in range(constants.START_YEAR, constants.END_YEAR + 1)
    )


def _jlbb_files_exist() -> bool:
    """Return True if all JLBB big board CSVs exist for the year range."""
    return all(
        (constants.DATA_PATH / f"jl_big_board_{y}.csv").exists()
        for y in range(constants.START_YEAR, constants.END_YEAR + 1)
    )


def _combined_files_exist() -> bool:
    """Return True if all combined big board CSVs exist for the year range."""
    return all(
        (constants.DATA_PATH / f"combined_big_board_{y}.csv").exists()
        for y in range(constants.START_YEAR, constants.END_YEAR + 1)
    )


def _cleaned_picks_exist() -> bool:
    """Return True if the cleaned draft picks file exists."""
    return (constants.DATA_PATH / "cleaned_draft_picks.csv").exists()


def _av_file_exists() -> bool:
    """Return True if the AV-enriched draft picks file exists."""
    return (constants.DATA_PATH / "cleaned_draft_picks_with_av.csv").exists()


def _merged_files_exist() -> bool:
    """Return True if all merged output CSVs exist for the year range."""
    return all(
        (constants.DATA_PATH / f"draft_picks_with_big_board_ranks_{y}.csv").exists()
        for y in range(constants.START_YEAR, constants.END_YEAR + 1)
    )


def _run_scrape_mddb() -> None:
    """Run the MDDB big board scraper."""
    from nfl_draft_scraper.mddb_bb_scraper import main as mddb_main

    log.info("=== Stage: scrape-mddb ===")
    mddb_main()


def _run_scrape_jlbb() -> None:
    """Run the JLBB big board scraper."""
    from nfl_draft_scraper.jl_bb_scraper import main as jlbb_main

    log.info("=== Stage: scrape-jlbb ===")
    jlbb_main()


def _run_combine() -> None:
    """Run the big board combiner."""
    from nfl_draft_scraper.big_board_combiner import main as combine_main

    log.info("=== Stage: combine ===")
    combine_main()


def _run_clean_picks() -> None:
    """Run the draft picks cleaner."""
    from nfl_draft_scraper.draft_picks_cleaner import main as clean_main

    log.info("=== Stage: clean-picks ===")
    clean_main()


def _run_scrape_av(force_av: bool = False) -> None:
    """Run the AV scraper."""
    from nfl_draft_scraper.scrape_av import update_av

    log.info("=== Stage: scrape-av ===")
    update_av(force=force_av)


def _run_merge() -> None:
    """Run the merge of big board ranks into picks."""
    from nfl_draft_scraper.merge_bb_ranks_to_picks import main as merge_main

    log.info("=== Stage: merge ===")
    merge_main()


_CheckFn = Callable[[], bool]
_RunFn = Callable[[], None]


def _stage_runners() -> dict[str, tuple[_CheckFn, _RunFn]]:
    """Build stage runner mapping at call time so test patches are respected."""
    return {
        "scrape-mddb": (_mddb_files_exist, _run_scrape_mddb),
        "scrape-jlbb": (_jlbb_files_exist, _run_scrape_jlbb),
        "combine": (_combined_files_exist, _run_combine),
        "clean-picks": (_cleaned_picks_exist, _run_clean_picks),
        "scrape-av": (_av_file_exists, _run_scrape_av),
        "merge": (_merged_files_exist, _run_merge),
    }


def run_pipeline(stages: list[str] | None = None, *, force: bool = False) -> None:
    """Run the specified pipeline stages, or all stages if none specified.

    Args:
        stages: List of stage names to run.  ``None`` means all stages.
        force: If True, run every requested stage regardless of existing data.

    """
    target_stages = stages if stages else list(STAGES)
    runners = _stage_runners()

    for stage_name in target_stages:
        if stage_name not in runners:
            log.error("Unknown stage: %s (valid: %s)", stage_name, ", ".join(STAGES))
            continue

        check_fn, runner_fn = runners[stage_name]

        if not force and check_fn():
            log.info(
                "Skipping %s — output files already exist (use --force to override)",
                stage_name,
            )
            continue

        runner_fn()


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the pipeline orchestrator."""
    parser = argparse.ArgumentParser(
        description="NFL Draft Scraper pipeline orchestrator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "stages (in dependency order):\n"
            "  scrape-mddb   Scrape MDDB big boards\n"
            "  scrape-jlbb   Scrape JLBB big boards (Playwright)\n"
            "  combine       Combine MDDB + JLBB boards\n"
            "  clean-picks   Clean raw draft_picks.csv\n"
            "  scrape-av     Enrich picks with AV data\n"
            "  merge         Merge big board ranks into picks\n"
        ),
    )
    parser.add_argument(
        "stages",
        nargs="*",
        choices=list(STAGES) + [[]],
        default=[],
        metavar="STAGE",
        help="Stage(s) to run. Omit to run all stages.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-run stages even if output files already exist.",
    )
    return parser


def main() -> None:
    """Entry point for ``python -m nfl_draft_scraper.pipeline``."""
    parser = _build_parser()
    args = parser.parse_args()
    requested = args.stages if args.stages else None

    log.info(
        "Pipeline: years %d–%d, stages=%s, force=%s",
        constants.START_YEAR,
        constants.END_YEAR,
        requested or "all",
        args.force,
    )

    try:
        run_pipeline(stages=requested, force=args.force)
    except KeyboardInterrupt:
        log.warning("Pipeline interrupted by user.")
        sys.exit(130)

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
