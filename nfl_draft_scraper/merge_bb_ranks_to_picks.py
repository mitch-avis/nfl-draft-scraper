"""Merge big board ranks into cleaned draft picks with AV for each draft year.

For each year, matches drafted players to their big board ranks using fuzzy matching, and outputs a
new CSV with round, round_pick, pick (overall), team, pfr_player_id, player, position, category,
college, WL Rank, MDDB Rank, JLBB Rank, Consensus, JL_Avg, JL_SD, yearly AVs, career AV, and
weighted career AV.

Output: draft_picks_with_big_board_ranks_<year>.csv for each year.
"""

from __future__ import annotations

import difflib
from pathlib import Path
from typing import Any

import polars as pl

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.logger import log

_NAME_SUFFIXES = frozenset({"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"})

# ---------------------------------------------------------------------------
# Player name aliases
# ---------------------------------------------------------------------------
# Maps lowercased draft-pick names (from NFLverse) to lowercased big-board names when the two
# sources use different names for the same player.
_PLAYER_NAME_ALIASES: dict[str, str] = {
    "boogie basham": "carlos basham jr.",  # 2021 — Carlos Basham Jr.
    "quan martin": "jartavius martin",  # 2023 — Jartavius Martin
    "kobee minor": "darrian minor",  # 2025 — Darrian Minor
    "julian ashby": "frederick julian ashby ii",  # 2025 — Frederick Julian Ashby II
}

# ---------------------------------------------------------------------------
# Position normalisation
# ---------------------------------------------------------------------------
# Each position code maps to one or more canonical groups.  Two positions are compatible when their
# group sets overlap.
_POSITION_TO_GROUPS: dict[str, frozenset[str]] = {
    # Quarterback
    "qb": frozenset({"QB"}),
    # Running back / Fullback
    "rb": frozenset({"RB"}),
    "fb": frozenset({"RB"}),
    # Wide receiver
    "wr": frozenset({"WR"}),
    # Tight end
    "te": frozenset({"TE"}),
    # Offensive line
    "c": frozenset({"OL"}),
    "g": frozenset({"OL"}),
    "og": frozenset({"OL"}),
    "iol": frozenset({"OL"}),
    "ol": frozenset({"OL"}),
    "ot": frozenset({"OL"}),
    "t": frozenset({"OL"}),
    # Defensive line (pure interior)
    "dl": frozenset({"DL"}),
    "dt": frozenset({"DL"}),
    "nt": frozenset({"DL"}),
    "idl": frozenset({"DL"}),
    # Edge rushers (straddle DL and LB)
    "edge": frozenset({"DL", "EDGE"}),
    "ed": frozenset({"DL", "EDGE"}),
    "de": frozenset({"DL", "EDGE"}),
    # Linebackers
    "lb": frozenset({"LB", "EDGE"}),
    "ilb": frozenset({"LB"}),
    "olb": frozenset({"LB", "EDGE"}),
    # Defensive backs
    "cb": frozenset({"DB"}),
    "cbn": frozenset({"DB"}),
    "db": frozenset({"DB"}),
    "s": frozenset({"DB"}),
    "fs": frozenset({"DB"}),
    "saf": frozenset({"DB"}),
    # Special teams
    "k": frozenset({"K"}),
    "p": frozenset({"P"}),
    "ls": frozenset({"LS"}),
}


def _positions_compatible(pos_a: str, pos_b: str) -> bool:
    """Return True when two position codes could refer to the same player.

    Unknown or empty codes are treated as compatible (we cannot disprove the match).
    """
    a = pos_a.strip().lower()
    b = pos_b.strip().lower()
    if not a or not b:
        return True
    groups_a = _POSITION_TO_GROUPS.get(a)
    groups_b = _POSITION_TO_GROUPS.get(b)
    if groups_a is None or groups_b is None:
        return True
    return bool(groups_a & groups_b)


# ---------------------------------------------------------------------------
# School normalisation
# ---------------------------------------------------------------------------
# Map known alternative names to a single canonical form.  Both directions must normalise to the
# same value.
_SCHOOL_ALIASES: dict[str, str] = {
    # Acronym vs full name
    "ucf": "central florida",
    "central florida": "central florida",
    "utsa": "utsa",
    "texas-san antonio": "utsa",
    # NC State uses abbreviated form in both directions
    "nc state": "north carolina state",
    # PFR double-abbreviation patterns
    "middle tenn. st.": "middle tennessee state",
    "se missouri st.": "southeast missouri state",
    "nw missouri st.": "northwest missouri state",
    "central missouri st.": "central missouri state",
    # Renamed schools
    "houston baptist": "houston christian",
    "houston christian": "houston christian",
    # UAB / Ala-Birmingham
    "uab": "uab",
    "ala-birmingham": "uab",
}


def _normalize_school(school: str) -> str:
    """Normalise a school name for comparison.

    Applies alias mapping, then expands common PFR abbreviations ("St." → "State", "Col." →
    "College") so both big-board and draft-pick school names converge to the same canonical form.
    """
    s = school.strip().lower()
    if s in _SCHOOL_ALIASES:
        return _SCHOOL_ALIASES[s]
    # Expand abbreviations that follow a space (avoids "St." meaning "Saint")
    s = s.replace(" st.", " state")
    s = s.replace(" col.", " college")
    # Remove remaining periods (e.g. in "A&M." edge cases)
    s = s.replace(".", "")
    return s


def _schools_compatible(school_a: str, school_b: str) -> bool:
    """Return True when two school names refer to the same institution.

    Empty or missing names are treated as compatible (unknown).
    """
    if not school_a.strip() or not school_b.strip():
        return True
    return _normalize_school(school_a) == _normalize_school(school_b)


def _extract_first_name(name: str) -> str:
    """Extract the first name from a full name.

    Returns the first whitespace-delimited token, lowercased. Returns empty string for empty input.
    """
    parts = name.strip().lower().split()
    return parts[0] if parts else ""


def _strip_punctuation(text: str) -> str:
    """Remove periods, apostrophes, and hyphens from text for comparison."""
    return text.replace(".", "").replace("'", "").replace("-", "")


def first_names_compatible(name_a: str, name_b: str) -> bool:
    """Check whether two player names have compatible first names.

    Strips punctuation (periods, apostrophes) then checks if one first name is a substring of the
    other.  This catches abbreviations (Pat⊂Patrick, Cam⊂Cameron), nicknames (Bisi⊂Olabisi,
    Cobie⊂Decobie), and punctuation variants (DJ=D.J., RJ=R.J.).

    Returns False only when the stripped first names share no substring relationship.  Empty names
    are not compatible.
    """
    first_a = _strip_punctuation(_extract_first_name(name_a))
    first_b = _strip_punctuation(_extract_first_name(name_b))
    if not first_a or not first_b:
        return False
    return first_a in first_b or first_b in first_a


def _extract_last_name(name: str) -> str:
    """Extract the last name from a full name, stripping common suffixes.

    Handles suffixes like Jr., Sr., II, III, IV.  Returns lowercased result.
    """
    parts = name.strip().lower().split()
    while parts and parts[-1] in _NAME_SUFFIXES:
        parts.pop()
    return parts[-1] if parts else ""


def last_names_compatible(name_a: str, name_b: str) -> bool:
    """Check whether two player names share a compatible last name.

    Handles hyphenated last names by checking whether any component of one last name appears in the
    other (e.g. "Tomlinson" matches "Hodges-Tomlinson").  Also handles cases where one last name is
    a substring of the other (e.g. "to'oto'o" contains "to'o").
    """
    last_a = _extract_last_name(name_a)
    last_b = _extract_last_name(name_b)
    if not last_a or not last_b:
        return False
    # Exact match (fast path)
    if last_a == last_b:
        return True
    # Hyphenated component match
    parts_a = set(last_a.split("-"))
    parts_b = set(last_b.split("-"))
    if parts_a & parts_b:
        return True
    # Substring match (handles to'oto'o ↔ to'o-to'o)
    return any(p in last_b for p in parts_a) or any(p in last_a for p in parts_b)


def _fuzzy_match_player(
    name: str,
    choices: list[str],
    cutoff: float = 0.6,
    *,
    pick_position: str = "",
    pick_school: str = "",
    bb_positions: dict[str, str] | None = None,
    bb_schools: dict[str, str] | None = None,
) -> str | None:
    """Return the closest matching string from *choices* for *name*.

    Return ``None`` when: * no candidate exceeds the *cutoff* similarity threshold, * the
    candidate's last name is incompatible with the input name, or * *both* the candidate's position
    and school are incompatible with the
      pick's position and school (when that data is available).
    """
    matches: list[str] = difflib.get_close_matches(str(name), choices, n=1, cutoff=cutoff)
    if not matches:
        return None
    candidate = matches[0]
    if not last_names_compatible(name, candidate):
        return None
    # Position + school cross-check (reject only when *both* disagree)
    if bb_positions is not None and bb_schools is not None:
        bb_pos = bb_positions.get(candidate, "")
        bb_school = bb_schools.get(candidate, "")
        pos_ok = _positions_compatible(pick_position, bb_pos)
        school_ok = _schools_compatible(pick_school, bb_school)
        if not pos_ok and not school_ok:
            return None
    return candidate


# Columns to extract from the combined big board CSV into the per-player lookup.
_BB_RANK_COLUMNS: list[str] = [
    "WL",
    "WL_Variance",
    "MDDB",
    "JLBB",
    "Consensus",
    "Consensus_SE",
    "JL_Avg",
    "JL_SD",
]

# Mapping from combined-CSV column name to output column name in the draft picks file.  Only columns
# that need renaming are listed here.
_BB_COL_RENAME: dict[str, str] = {
    "WL": "WL Rank",
    "MDDB": "MDDB Rank",
    "JLBB": "JLBB Rank",
}


def _get_rank_lists(
    picks_year: pl.DataFrame,
    bb_lookup: dict[str, dict[str, Any]],
    bb_names: list[str],
    bb_positions: dict[str, str] | None = None,
    bb_schools: dict[str, str] | None = None,
) -> dict[str, list[Any | None]]:
    """Fuzzy match and collect big board ranks for each drafted player.

    Returns a dict mapping output column names to parallel value lists, one entry per row of
    *picks_year*.
    """
    result: dict[str, list[Any | None]] = {
        _BB_COL_RENAME.get(col, col): [] for col in _BB_RANK_COLUMNS
    }

    for row in picks_year.iter_rows(named=True):
        player_clean: str = str(row["pfr_player_name_clean"])
        pick_position: str = str(row.get("position", "") or "")
        pick_school: str = str(row.get("college", "") or "")
        match: str | None = _fuzzy_match_player(
            player_clean,
            bb_names,
            pick_position=pick_position,
            pick_school=pick_school,
            bb_positions=bb_positions,
            bb_schools=bb_schools,
        )
        # Try alias lookup when the standard fuzzy match fails
        if match is None and player_clean in _PLAYER_NAME_ALIASES:
            alias = _PLAYER_NAME_ALIASES[player_clean]
            match = _fuzzy_match_player(
                alias,
                bb_names,
                pick_position=pick_position,
                pick_school=pick_school,
                bb_positions=bb_positions,
                bb_schools=bb_schools,
            )
            if match:
                log.debug("Matched %s via alias %s", row["pfr_player_name"], alias)
        if match:
            ranks: dict[str, Any] = bb_lookup[match]
            for col in _BB_RANK_COLUMNS:
                out_col = _BB_COL_RENAME.get(col, col)
                result[out_col].append(ranks.get(col))
            log.debug("Matched %s to %s", row["pfr_player_name"], match)
        else:
            for col in _BB_RANK_COLUMNS:
                out_col = _BB_COL_RENAME.get(col, col)
                result[out_col].append(None)
            log.info("No big board match for %s (%s)", row["pfr_player_name"], row["college"])

    return result


def _get_av_columns(df: pl.DataFrame) -> list[str]:
    """Return the list of AV columns present in the DataFrame.

    AV columns are any 4-digit year columns (per-season AV) plus the fixed set of aggregate AV
    columns. Year columns are detected from the DataFrame itself rather than the pipeline year
    range, since AV data may span earlier seasons than the current scraping window.
    """
    year_cols: list[str] = sorted(col for col in df.columns if len(col) == 4 and col.isdigit())
    aggregate_cols: list[str] = [
        "career",
        "weighted_career",
        "draft_team_career",
        "draft_team_weighted_career",
        "w_av",
        "dr_av",
    ]
    return year_cols + [col for col in aggregate_cols if col in df.columns]


def _reorder_and_save(
    picks_year: pl.DataFrame,
    output_path: Path,
    av_cols: list[str],
) -> None:
    """Reorder columns and save the DataFrame to CSV."""
    base_cols: list[str] = [
        "round",
        "round_pick",
        "pick",
        "team",
        "pfr_player_id",
        "pfr_player_name",
        "position",
        "category",
        "college",
    ]
    rank_cols: list[str] = [
        "WL Rank",
        "WL_Variance",
        "MDDB Rank",
        "JLBB Rank",
        "Consensus",
        "Consensus_SE",
        "JL_Avg",
        "JL_SD",
    ]
    ordered_cols: list[str] = base_cols + rank_cols + av_cols
    # Only include columns actually present
    ordered_cols = [c for c in ordered_cols if c in picks_year.columns]
    rename_map: dict[str, str] = {
        "pfr_player_name": "player",
        "pick": "overall_pick",
    }
    picks_out = picks_year.select(ordered_cols).rename(
        {k: v for k, v in rename_map.items() if k in ordered_cols}
    )
    picks_out.write_csv(output_path)


def _load_picks_for_year(year: int) -> pl.DataFrame | None:
    """Return the cleaned draft picks (with AV when available) filtered to ``year``.

    Prefers ``cleaned_draft_picks_with_av.csv`` so per-season AV columns are merged into the
    output. Falls back to ``cleaned_draft_picks.csv`` when the AV file is missing or has no rows
    for the requested year (e.g. a draft that has just concluded but whose first season has not
    yet been played). Returns ``None`` when neither file has rows for ``year``.
    """
    av_path = constants.DATA_PATH / "cleaned_draft_picks_with_av.csv"
    cleaned_path = constants.DATA_PATH / "cleaned_draft_picks.csv"

    if av_path.exists():
        av_df = pl.read_csv(av_path).filter(pl.col("season") == year)
        if not av_df.is_empty():
            return av_df

    if not cleaned_path.exists():
        log.error("Draft picks file not found: %s", cleaned_path)
        return None

    cleaned_df = pl.read_csv(cleaned_path).filter(pl.col("season") == year)
    if cleaned_df.is_empty():
        return None
    log.info("Using cleaned draft picks (no AV) for %s", year)
    return cleaned_df


def _merge_big_board_ranks_for_year(year: int) -> None:
    """Merge big board ranks into cleaned draft picks for a given year.

    AV columns are included when available. When AV data has not yet been scraped for ``year``
    (e.g. immediately after the draft, before the first season is played), the merge proceeds
    against ``cleaned_draft_picks.csv`` and the output simply omits the per-season AV columns.

    Output a new CSV for the year.
    """
    bb_path = constants.DATA_PATH / f"combined_big_board_{year}.csv"
    output_path = constants.DATA_PATH / f"draft_picks_with_big_board_ranks_{year}.csv"

    if not bb_path.exists():
        log.warning("Big board file not found for %s: %s", year, bb_path)
        return

    picks_year = _load_picks_for_year(year)
    if picks_year is None:
        log.info("No draft picks found for %s. Skipping.", year)
        return

    bb_df: pl.DataFrame = pl.read_csv(bb_path)
    picks_year = picks_year.with_columns(
        pl.col("pfr_player_name")
        .str.strip_chars()
        .str.to_lowercase()
        .alias("pfr_player_name_clean"),
    )
    bb_df = bb_df.with_columns(
        pl.col("Player").str.strip_chars().str.to_lowercase().alias("Player_clean"),
    )

    # Identify duplicate big board entries so we can warn before dropping them.
    dupes = bb_df.filter(pl.col("Player_clean").is_duplicated())["Player_clean"].unique().to_list()
    if dupes:
        log.warning("Duplicate big board player names for %s: %s", year, dupes)
    bb_df = bb_df.unique(subset=["Player_clean"], keep="first", maintain_order=True)

    # Build per-player lookup from combined big board columns
    bb_cols_present = [c for c in _BB_RANK_COLUMNS if c in bb_df.columns]
    bb_lookup: dict[str, dict[str, Any]] = {
        str(row["Player_clean"]): {col: row[col] for col in bb_cols_present}
        for row in bb_df.select(["Player_clean", *bb_cols_present]).iter_rows(named=True)
    }
    bb_names: list[str] = list(bb_lookup.keys())

    # Build position and school lookups for enhanced fuzzy matching
    bb_positions: dict[str, str] = dict(
        zip(
            bb_df["Player_clean"].cast(pl.String).to_list(),
            bb_df["Position"].fill_null("").cast(pl.String).to_list(),
            strict=False,
        )
    )
    bb_schools: dict[str, str] = dict(
        zip(
            bb_df["Player_clean"].cast(pl.String).to_list(),
            bb_df["School"].fill_null("").cast(pl.String).to_list(),
            strict=False,
        )
    )

    rank_data = _get_rank_lists(picks_year, bb_lookup, bb_names, bb_positions, bb_schools)
    picks_year = picks_year.with_columns(
        [pl.Series(name=col_name, values=values) for col_name, values in rank_data.items()],
    )

    av_cols: list[str] = _get_av_columns(picks_year)
    _reorder_and_save(picks_year, output_path, av_cols)
    log.info(
        "Saved merged draft picks with big board ranks for %s to %s",
        year,
        output_path,
    )


def main() -> None:
    """Merge big board ranks into draft picks for each year in the configured range."""
    log.info(
        "Starting merge of big board ranks for years %s-%s",
        constants.START_YEAR,
        constants.END_YEAR,
    )
    for year in range(constants.START_YEAR, constants.END_YEAR + 1):
        _merge_big_board_ranks_for_year(year)
    log.info("Done.")


if __name__ == "__main__":
    main()
