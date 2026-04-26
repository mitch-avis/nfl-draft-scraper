"""Jacklich10 Big Board Scraper.

Uses Playwright to scrape the consensus big board from jacklich10.com, which is an R Shiny app that
loads data dynamically via WebSocket.

The Shiny app delivers the full reactable dataset in a single SockJS WebSocket frame, so there is no
need for DOM pagination.  This module captures the WebSocket traffic, extracts the columnar JSON
payload, and converts it to row-based records.

No external browser drivers (geckodriver) are required — Playwright bundles its own Chromium.
"""

from __future__ import annotations

import json
import random
import re
import time
from typing import Any

from playwright.sync_api import Page, sync_playwright

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.csv_utils import save_csv
from nfl_draft_scraper.utils.logger import log

SLEEP_MIN = 1
SLEEP_MAX = 3
_rng = random.SystemRandom()

_TABLE_SELECTOR = ".rt-table"

# Minimum frame length to consider (the data frame is typically > 100 KB)
_MIN_FRAME_LEN = 1000

# Regex to extract text from <span> or unclosed <span> inside <div> blocks. The site uses patterns
# like:
#   <div ...><span ...>Text</span></div>   (with closing tag)
#   <div ...><span ...>Text</div>          (without closing </span>)
_SPAN_TEXT_RE = re.compile(r"<span[^>]*>([^<]+)")

# Script tag pattern for the reactable widget JSON
_WIDGET_SCRIPT_RE = re.compile(
    r'<script\s+type="application/json"\s+data-for="[^"]*">(.*?)</script>',
    re.DOTALL,
)


def _parse_name_html(html: str) -> str:
    """Extract 'First Last' from the player_name column HTML.

    The HTML contains two ``<div>`` blocks, each with a ``<span>`` holding first name and last name
    respectively.
    """
    if not html:
        return ""
    parts = _SPAN_TEXT_RE.findall(html)
    if not parts:
        return html.strip()
    return " ".join(p.strip() for p in parts).strip()


def _parse_position_html(html: str) -> str:
    """Extract the position code from the combo column HTML.

    The first ``<span>`` text in the combo HTML holds the position.
    """
    if not html:
        return ""
    match = _SPAN_TEXT_RE.search(html)
    return match.group(1).strip() if match else ""


def _extract_widget_data(shiny_html: str) -> dict[str, list[Any]]:
    """Extract the columnar data dict from a Shiny HTML output string.

    The Shiny app embeds a ``<script type="application/json">`` tag inside the rendered reactable
    widget.  The JSON lives at ``x.tag.attribs.data`` within the parsed object.

    Raises
    ------
    ValueError
        If the expected ``<script>`` tag is not found.

    """
    match = _WIDGET_SCRIPT_RE.search(shiny_html)
    if not match:
        msg = "Could not find reactable widget JSON in Shiny HTML output"
        raise ValueError(msg)
    widget: dict[str, Any] = json.loads(match.group(1))
    data: dict[str, list[Any]] = widget["x"]["tag"]["attribs"]["data"]
    col_names = list(data.keys())
    row_count = len(next(iter(data.values()), []))
    log.debug("Extracted widget data: %d columns, %d rows", len(col_names), row_count)
    log.debug("Widget columns: %s", col_names)
    return data


def _parse_shiny_message(raw_frame: str) -> dict[str, Any]:
    """Parse a SockJS WebSocket frame into a Shiny JSON message.

    The frame format is ``a["<id>#<subid>|m|<json>"]``.

    Raises
    ------
    ValueError
        If the frame cannot be parsed or ``bb_table`` is missing.

    """
    if not raw_frame.startswith("a"):
        msg = "Not a valid SockJS frame"
        raise ValueError(msg)
    try:
        messages: list[str] = json.loads(raw_frame[1:])
    except (json.JSONDecodeError, IndexError) as exc:
        msg = "Could not parse SockJS message array"
        raise ValueError(msg) from exc

    log.debug("SockJS frame contains %d message(s)", len(messages))

    for msg_str in messages:
        pipe_idx = msg_str.find("|m|")
        if pipe_idx == -1:
            continue
        payload: dict[str, Any] = json.loads(msg_str[pipe_idx + 3 :])
        if "values" in payload and "bb_table" in payload.get("values", {}):
            return payload

    msg = "bb_table not found in any Shiny message"
    raise ValueError(msg)


# Columns that are either rendered as HTML (parsed separately) or are image URLs we do not want in
# the final CSV output.
_SKIP_COLUMNS = frozenset({"team_logo_espn", "player_image", "combo", "player_name"})

# Columns with well-known names that get special handling (rename or parse).
_STRUCTURAL_COLUMNS = frozenset({"rank", "school", "conference", "sd_rk", "avg_rk"}) | _SKIP_COLUMNS


def _columnar_to_records(data: dict[str, list[Any]]) -> list[dict[str, str]]:
    """Convert columnar widget data to a list of record dicts.

    Each record contains fixed keys (``rank``, ``name``, ``pos``, ``school``, optionally
    ``conference``, ``avg``, ``sd``) followed by any source-site rank columns present in the data
    (sorted alphabetically).
    """
    ranks = data.get("rank", [])
    names = data.get("player_name", [])
    combos = data.get("combo", [])
    schools = data.get("school", [])
    conferences = data.get("conference", [])
    avgs = data.get("avg_rk", [])
    sds = data.get("sd_rk", [])

    # Source-site rank columns are everything not structural
    source_cols = sorted(col for col in data if col not in _STRUCTURAL_COLUMNS)
    log.debug("Source rank columns detected: %s", source_cols)

    records: list[dict[str, str]] = []
    for i in range(len(ranks)):
        name = _parse_name_html(names[i]) if i < len(names) else ""
        pos = _parse_position_html(combos[i]) if i < len(combos) else ""
        if not name:
            continue

        record: dict[str, str] = {"rank": str(ranks[i]), "name": name, "pos": pos}

        if i < len(schools):
            record["school"] = str(schools[i]) if schools[i] is not None else ""

        if conferences and i < len(conferences):
            record["conference"] = str(conferences[i]) if conferences[i] is not None else ""

        if avgs and i < len(avgs):
            record["avg"] = str(avgs[i]) if avgs[i] is not None else ""

        if sds and i < len(sds):
            record["sd"] = str(sds[i]) if sds[i] is not None else ""

        for col in source_cols:
            vals = data[col]
            if i < len(vals):
                record[col] = str(vals[i]) if vals[i] is not None else ""
            else:
                record[col] = ""

        records.append(record)
    return records


def _verify_year(page: Page, expected_year: int) -> None:
    """Verify the year dropdown matches the expected year after update.

    Raises
    ------
    ValueError
        If the selected year does not match ``expected_year``.

    """
    selected = page.input_value("#year")
    log.debug("Year dropdown value after update: %s (expected %d)", selected, expected_year)
    if selected != str(expected_year):
        msg = f"Year mismatch: expected {expected_year}, got {selected}"
        raise ValueError(msg)


def fetch_and_parse(page: Page, year: int) -> list[dict[str, str]]:
    """Fetch and parse the big board data for a given year.

    Navigates to the JLBB site, selects the requested year, and captures the full dataset from the
    WebSocket payload.  The initial page load delivers the default (latest) year's data; those
    frames are discarded before the year selection so only the requested year's data is kept.
    """
    log.debug("fetch_and_parse: starting for year %d", year)
    ws_frames: list[str] = []

    def _on_ws(ws: Any) -> None:
        """Register a frame listener on each WebSocket."""

        def _on_frame(data: str | bytes) -> None:
            payload = data if isinstance(data, str) else data.decode("utf-8", errors="replace")
            if len(payload) > _MIN_FRAME_LEN:
                log.debug("Captured WebSocket frame: %d chars", len(payload))
                ws_frames.append(payload)

        ws.on("framereceived", _on_frame)

    page.on("websocket", _on_ws)

    log.debug("Navigating to %s", constants.JLBB_BASE_URL)
    page.goto(constants.JLBB_BASE_URL, wait_until="networkidle", timeout=60000)
    page.wait_for_selector(_TABLE_SELECTOR, timeout=30000)
    log.debug(
        "Initial page load complete; captured %d frame(s) for default year",
        len(ws_frames),
    )

    # Discard frames from the initial load (default/latest year data)
    ws_frames.clear()
    log.debug("Cleared initial frames; selecting year %d", year)

    page.select_option("#year", str(year))
    page.click("#update")
    log.debug("Clicked update for year %d; waiting for data", year)
    time.sleep(3)
    page.wait_for_selector(_TABLE_SELECTOR, timeout=30000)

    # Allow the WebSocket frame to arrive
    time.sleep(2)
    log.debug(
        "After year selection: captured %d frame(s), sizes: %s",
        len(ws_frames),
        [len(f) for f in ws_frames],
    )

    if not ws_frames:
        msg = f"No large WebSocket frames captured for year {year}"
        raise RuntimeError(msg)

    # Pick the largest frame (should be the data payload)
    largest = ws_frames[0]
    for frame in ws_frames[1:]:
        if len(frame) > len(largest):
            largest = frame
    log.debug("Using largest frame: %d chars", len(largest))

    shiny_msg = _parse_shiny_message(largest)
    data = _extract_widget_data(shiny_msg["values"]["bb_table"]["html"])

    records = _columnar_to_records(data)
    log.info("Parsed %d players for %d", len(records), year)

    _verify_year(page, year)
    return records


def scrape_year(year: int, page: Page) -> list[dict[str, str]]:
    """Scrape a single year and save to CSV."""
    log.info("Scraping JLBB big board for year %d", year)
    recs = fetch_and_parse(page, year)
    file_name = f"jl_big_board_{year}.csv"
    save_csv(file_name, recs)
    log.info("Saved %d records to %s", len(recs), file_name)
    return recs


def main() -> None:
    """Scrape the big board data from Jacklich10's website."""
    log.info(
        "Starting JLBB scraper for years %d–%d",
        constants.START_YEAR,
        constants.END_YEAR,
    )
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()

        for year in range(constants.START_YEAR, constants.END_YEAR + 1):
            scrape_year(year, page)
            time.sleep(_rng.uniform(SLEEP_MIN, SLEEP_MAX))

        browser.close()
    log.info("JLBB scraping complete.")


if __name__ == "__main__":
    main()
