"""Tests for nfl_draft_scraper.jl_bb_scraper."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from nfl_draft_scraper.jl_bb_scraper import (
    _columnar_to_records,
    _extract_widget_data,
    _parse_name_html,
    _parse_position_html,
    _parse_shiny_message,
    _verify_year,
    fetch_and_parse,
    main,
    scrape_year,
)

# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

FIRST_NAME_HTML = (
    "<div style='line-height:12px'>"
    "<span style ='font-weight:bold;color:grey;font-size:10px'>Arvell</span></div>\n"
    "<div style='line-height:10px'>"
    "<span style='font-weight:bold;font-variant:small-caps;font-size:14px'>Reese</div>"
)

COMBO_HTML = (
    "<div style='line-height:12px'>"
    "<span style ='font-weight:bold;font-size:14px'>LB</span></div>\n"
    "<div style='line-height:10px'>"
    "<span style='font-weight:bold;color:grey;font-variant:small-caps;font-size:10px'>"
    "JR.</span></div>"
)


def _build_widget_script(data: dict) -> str:
    """Build a minimal Shiny HTML string with an embedded reactable widget JSON."""
    widget = {"x": {"tag": {"attribs": {"data": data}}}}
    return (
        '<div id="bb_table" class="reactable html-widget">'
        '<script type="application/json" data-for="bb_table">'
        f"{json.dumps(widget)}"
        "</script></div>"
    )


def _build_sockjs_frame(shiny_payload: dict) -> str:
    """Build a minimal SockJS frame wrapping a Shiny JSON message."""
    inner = json.dumps(shiny_payload)
    msg = f"5#0|m|{inner}"
    return "a" + json.dumps([msg])


# ---------------------------------------------------------------------------
# _parse_name_html
# ---------------------------------------------------------------------------


class TestParseNameHtml:
    """Tests for _parse_name_html."""

    def test_extracts_first_and_last(self) -> None:
        """Verify first and last names are joined."""
        assert _parse_name_html(FIRST_NAME_HTML) == "Arvell Reese"

    def test_suffix_in_last_name(self) -> None:
        """Verify names with suffixes (Jr.) are preserved."""
        html = (
            "<div style='line-height:12px'>"
            "<span style ='font-weight:bold;color:grey;font-size:10px'>Rueben</span></div>\n"
            "<div style='line-height:10px'>"
            "<span style='font-weight:bold;font-variant:small-caps;font-size:14px'>"
            "Bain Jr.</div>"
        )
        assert _parse_name_html(html) == "Rueben Bain Jr."

    def test_empty_string(self) -> None:
        """Verify empty input returns empty string."""
        assert _parse_name_html("") == ""

    def test_plain_text_fallback(self) -> None:
        """Verify plain text without HTML is returned as-is."""
        assert _parse_name_html("John Smith") == "John Smith"


# ---------------------------------------------------------------------------
# _parse_position_html
# ---------------------------------------------------------------------------


class TestParsePositionHtml:
    """Tests for _parse_position_html."""

    def test_extracts_position(self) -> None:
        """Verify position code is extracted from combo HTML."""
        assert _parse_position_html(COMBO_HTML) == "LB"

    def test_two_letter_position(self) -> None:
        """Verify two-character positions like OG work."""
        html = (
            "<div style='line-height:12px'>"
            "<span style ='font-weight:bold;font-size:14px'>OG</span></div>\n"
            "<div style='line-height:10px'>"
            "<span style='font-weight:bold;color:grey;font-variant:small-caps;font-size:10px'>"
            "RS SR.</span></div>"
        )
        assert _parse_position_html(html) == "OG"

    def test_empty_string(self) -> None:
        """Verify empty input returns empty string."""
        assert _parse_position_html("") == ""


# ---------------------------------------------------------------------------
# _extract_widget_data
# ---------------------------------------------------------------------------


class TestExtractWidgetData:
    """Tests for _extract_widget_data."""

    def test_extracts_columnar_data(self) -> None:
        """Verify columnar data dict is extracted from Shiny HTML."""
        data = {"rank": [1, 2], "player_name": ["A", "B"]}
        html = _build_widget_script(data)
        assert _extract_widget_data(html) == data

    def test_raises_on_missing_script(self) -> None:
        """Verify ValueError when no script tag is present."""
        with pytest.raises(ValueError, match="widget JSON"):
            _extract_widget_data("<div>no script here</div>")


# ---------------------------------------------------------------------------
# _parse_shiny_message
# ---------------------------------------------------------------------------


class TestParseShinyMessage:
    """Tests for _parse_shiny_message."""

    def test_parses_sockjs_frame(self) -> None:
        """Verify SockJS + Shiny protocol parsing extracts values dict."""
        payload = {"values": {"bb_table": {"html": "<p>test</p>"}}}
        frame = _build_sockjs_frame(payload)
        result = _parse_shiny_message(frame)
        assert result["values"]["bb_table"]["html"] == "<p>test</p>"

    def test_raises_on_bad_frame(self) -> None:
        """Verify ValueError on unparseable frame."""
        with pytest.raises(ValueError, match="SockJS"):
            _parse_shiny_message("not a valid frame")

    def test_raises_on_missing_bb_table(self) -> None:
        """Verify ValueError when bb_table is absent."""
        payload = {"values": {"other_widget": {}}}
        frame = _build_sockjs_frame(payload)
        with pytest.raises(ValueError, match="bb_table"):
            _parse_shiny_message(frame)


# ---------------------------------------------------------------------------
# _columnar_to_records
# ---------------------------------------------------------------------------


class TestColumnarToRecords:
    """Tests for _columnar_to_records."""

    def test_converts_basic_data_with_source_columns(self) -> None:
        """Verify columnar data includes source ranks, avg, sd, school, and conference."""
        data = {
            "rank": [1, 2],
            "player_name": [
                FIRST_NAME_HTML,
                FIRST_NAME_HTML.replace("Arvell", "Caleb").replace("Reese", "Downs"),
            ],
            "combo": [COMBO_HTML, COMBO_HTML.replace("LB", "S")],
            "school": ["Ohio State", "Miami"],
            "conference": ["Big 10", "ACC"],
            "team_logo_espn": ["http://img1", "http://img2"],
            "player_image": ["http://pic1", "http://pic2"],
            "BR": [6, 1],
            "ESPN": [2, 5],
            "PFF": [3, 5],
            "sd_rk": [1.76, 2.01],
            "avg_rk": [3.11, 3.44],
        }
        result = _columnar_to_records(data)
        assert len(result) == 2
        first = result[0]
        assert first["rank"] == "1"
        assert first["name"] == "Arvell Reese"
        assert first["pos"] == "LB"
        assert first["school"] == "Ohio State"
        assert first["conference"] == "Big 10"
        assert first["avg"] == "3.11"
        assert first["sd"] == "1.76"
        assert first["BR"] == "6"
        assert first["ESPN"] == "2"
        assert first["PFF"] == "3"
        # Image columns must be excluded
        assert "team_logo_espn" not in first
        assert "player_image" not in first

    def test_column_order_in_records(self) -> None:
        """Verify record keys follow expected ordering convention."""
        data = {
            "rank": [1],
            "player_name": [FIRST_NAME_HTML],
            "combo": [COMBO_HTML],
            "school": ["Ohio State"],
            "conference": ["Big 10"],
            "ESPN": [2],
            "BR": [6],
            "sd_rk": [1.76],
            "avg_rk": [3.11],
        }
        result = _columnar_to_records(data)
        keys = list(result[0].keys())
        # Fixed columns come first, then source columns alphabetically
        assert keys.index("rank") < keys.index("name")
        assert keys.index("name") < keys.index("pos")
        assert keys.index("pos") < keys.index("school")
        assert keys.index("avg") < keys.index("sd")
        # Source columns after sd
        assert keys.index("sd") < keys.index("BR")
        # Source columns sorted alphabetically
        assert keys.index("BR") < keys.index("ESPN")

    def test_empty_data(self) -> None:
        """Verify empty columnar data returns empty list."""
        data = {"rank": [], "player_name": [], "combo": [], "school": []}
        assert _columnar_to_records(data) == []

    def test_skips_rows_with_empty_name(self) -> None:
        """Verify rows with unparseable name HTML are skipped."""
        data = {
            "rank": [1],
            "player_name": [""],
            "combo": [COMBO_HTML],
            "school": ["Test U"],
        }
        assert _columnar_to_records(data) == []

    def test_handles_missing_optional_columns(self) -> None:
        """Verify records are built even without conference or source columns."""
        data = {
            "rank": [1],
            "player_name": [FIRST_NAME_HTML],
            "combo": [COMBO_HTML],
            "school": ["Ohio State"],
        }
        result = _columnar_to_records(data)
        assert len(result) == 1
        assert result[0]["name"] == "Arvell Reese"
        assert result[0]["school"] == "Ohio State"
        assert "conference" not in result[0]
        assert "avg" not in result[0]
        assert "sd" not in result[0]

    def test_dynamic_source_columns_vary_by_year(self) -> None:
        """Verify different source column sets are handled (2016 vs 2025 style)."""
        # 2016-style columns
        data_2016 = {
            "rank": [1],
            "player_name": [FIRST_NAME_HTML],
            "combo": [COMBO_HTML],
            "school": ["Ole Miss"],
            "conference": ["SEC"],
            "BR": [4],
            "ESPN": [2],
            "Yahoo": [4],
            "sd_rk": [1.49],
            "avg_rk": [2.7],
        }
        result = _columnar_to_records(data_2016)
        assert "BR" in result[0]
        assert "ESPN" in result[0]
        assert "Yahoo" in result[0]

        # 2025-style columns
        data_2025 = {
            "rank": [1],
            "player_name": [FIRST_NAME_HTML],
            "combo": [COMBO_HTML],
            "school": ["Ohio State"],
            "conference": ["Big 10"],
            "ATH": [1],
            "CBS": [5],
            "Tank": [1],
            "sd_rk": [1.76],
            "avg_rk": [3.11],
        }
        result = _columnar_to_records(data_2025)
        assert "ATH" in result[0]
        assert "CBS" in result[0]
        assert "Tank" in result[0]
        # Old columns shouldn't appear
        assert "Yahoo" not in result[0]

    def test_none_source_rank_preserved_as_empty(self) -> None:
        """Verify None values in source rank columns are preserved as empty strings."""
        data = {
            "rank": [1],
            "player_name": [FIRST_NAME_HTML],
            "combo": [COMBO_HTML],
            "school": ["Ohio State"],
            "BR": [None],
            "ESPN": [5],
            "sd_rk": [2.0],
            "avg_rk": [3.5],
        }
        result = _columnar_to_records(data)
        assert result[0]["BR"] == ""
        assert result[0]["ESPN"] == "5"

    def test_shorter_conference_list_than_ranks(self) -> None:
        """Verify records are built when conferences has fewer entries than ranks."""
        data = {
            "rank": [1, 2],
            "player_name": [
                FIRST_NAME_HTML,
                FIRST_NAME_HTML.replace("Arvell", "Caleb").replace("Reese", "Downs"),
            ],
            "combo": [COMBO_HTML, COMBO_HTML],
            "school": ["Ohio State", "Miami"],
            "conference": ["Big 10"],  # Only 1 entry for 2 players
            "sd_rk": [1.0, 2.0],
            "avg_rk": [3.0, 4.0],
        }
        result = _columnar_to_records(data)
        assert len(result) == 2
        assert result[0]["conference"] == "Big 10"
        assert "conference" not in result[1]

    def test_shorter_source_col_than_ranks(self) -> None:
        """Verify empty string when a source column has fewer values than ranks."""
        data = {
            "rank": [1, 2],
            "player_name": [
                FIRST_NAME_HTML,
                FIRST_NAME_HTML.replace("Arvell", "Caleb").replace("Reese", "Downs"),
            ],
            "combo": [COMBO_HTML, COMBO_HTML],
            "school": ["Ohio State", "Miami"],
            "ESPN": [3],  # Only 1 value for 2 players
        }
        result = _columnar_to_records(data)
        assert result[0]["ESPN"] == "3"
        assert result[1]["ESPN"] == ""

    def test_shorter_school_list_than_ranks(self) -> None:
        """Verify records are built when schools list is shorter than ranks."""
        data = {
            "rank": [1, 2],
            "player_name": [
                FIRST_NAME_HTML,
                FIRST_NAME_HTML.replace("Arvell", "Caleb").replace("Reese", "Downs"),
            ],
            "combo": [COMBO_HTML, COMBO_HTML],
            "school": ["Ohio State"],  # Only 1 for 2 players
        }
        result = _columnar_to_records(data)
        assert len(result) == 2
        assert result[0]["school"] == "Ohio State"
        assert "school" not in result[1]


# ---------------------------------------------------------------------------
# _verify_year
# ---------------------------------------------------------------------------


class TestVerifyYear:
    """Tests for _verify_year."""

    def test_passes_when_year_matches(self) -> None:
        """Verify no error when the dropdown value matches the requested year."""
        page = MagicMock()
        page.input_value.return_value = "2020"
        _verify_year(page, 2020)
        page.input_value.assert_called_once_with("#year")

    def test_raises_on_year_mismatch(self) -> None:
        """Verify ValueError when the dropdown value does not match the requested year."""
        page = MagicMock()
        page.input_value.return_value = "2026"
        with pytest.raises(ValueError, match="Year mismatch.*expected 2020.*got 2026"):
            _verify_year(page, 2020)

    def test_passes_for_default_year(self) -> None:
        """Verify no error when requesting the year that is already the default."""
        page = MagicMock()
        page.input_value.return_value = "2025"
        _verify_year(page, 2025)


# ---------------------------------------------------------------------------
# _parse_shiny_message — additional branch coverage
# ---------------------------------------------------------------------------


class TestParseShinyMessageBranches:
    """Additional branch coverage for _parse_shiny_message."""

    def test_raises_on_malformed_json(self) -> None:
        """Verify ValueError when the SockJS array contains invalid JSON."""
        frame = 'a["not valid json at all"]'
        # The inner message won't parse as valid |m| JSON but the array itself
        # parses fine — however "not valid json at all" has no |m| separator,
        # so it falls through to the "bb_table not found" error.
        with pytest.raises(ValueError, match="bb_table"):
            _parse_shiny_message(frame)

    def test_raises_on_corrupt_sockjs_array(self) -> None:
        """Verify ValueError when the outer SockJS JSON array is not valid."""
        frame = "a{corrupt"
        with pytest.raises(ValueError, match="Could not parse SockJS"):
            _parse_shiny_message(frame)

    def test_skips_messages_without_pipe_separator(self) -> None:
        """Verify messages without '|m|' are skipped gracefully."""
        # First message has no |m|, second has bb_table
        payload = {"values": {"bb_table": {"html": "<p>ok</p>"}}}
        inner_good = f"5#0|m|{json.dumps(payload)}"
        frame = "a" + json.dumps(["no-pipe-here", inner_good])
        result = _parse_shiny_message(frame)
        assert result["values"]["bb_table"]["html"] == "<p>ok</p>"


# ---------------------------------------------------------------------------
# fetch_and_parse
# ---------------------------------------------------------------------------


def _make_sockjs_frame(year_label: str, player_count: int = 2) -> str:
    """Build a realistic SockJS WebSocket frame for testing fetch_and_parse.

    Returns a frame whose payload contains player_count rows labelled with
    year_label in the school field (so tests can verify which frame was used).
    """
    names = [
        FIRST_NAME_HTML.replace("Arvell", f"Player{i}").replace("Reese", f"Y{year_label}")
        for i in range(player_count)
    ]
    combos = [COMBO_HTML] * player_count
    data = {
        "rank": list(range(1, player_count + 1)),
        "player_name": names,
        "combo": combos,
        "school": [f"School{year_label}"] * player_count,
        "ESPN": list(range(1, player_count + 1)),
        "sd_rk": [1.0] * player_count,
        "avg_rk": [2.0] * player_count,
    }
    widget = {"x": {"tag": {"attribs": {"data": data}}}}
    widget_script = (
        f'<script type="application/json" data-for="bb_table">{json.dumps(widget)}</script>'
    )
    shiny_payload = {"values": {"bb_table": {"html": widget_script}}}
    inner = json.dumps(shiny_payload)
    msg = f"5#0|m|{inner}"
    return "a" + json.dumps([msg])


class TestFetchAndParse:
    """Tests for fetch_and_parse."""

    def _setup_page(self, page: MagicMock, frames: list[str]) -> None:
        """Wire up a mock Page to deliver *frames* via the WebSocket callback."""
        ws_mock = MagicMock()
        frame_handler = None

        def _capture_on_frame(_event: str, handler):
            nonlocal frame_handler
            frame_handler = handler

        ws_mock.on = _capture_on_frame

        def _on_websocket(_event: str, handler):
            # Immediately fire the handler so it registers the frame listener
            handler(ws_mock)

        page.on = _on_websocket

        original_goto = page.goto

        def _goto_side_effect(*_args, **_kwargs):
            """Simulate the initial page load delivering frames."""
            original_goto(*_args, **_kwargs)

        page.goto = MagicMock(side_effect=_goto_side_effect)

        def _select_year_side_effect(*_args, **_kwargs):
            """Simulate year selection delivering frame(s)."""
            for f in frames:
                if frame_handler:
                    frame_handler(f)

        page.select_option = MagicMock(side_effect=_select_year_side_effect)
        page.input_value = MagicMock(return_value="")

    @patch("nfl_draft_scraper.jl_bb_scraper.time")
    def test_returns_records_for_selected_year(self, mock_time: MagicMock) -> None:
        """Verify records come from the year-specific WebSocket frame."""
        mock_time.sleep = MagicMock()
        page = MagicMock()
        year_frame = _make_sockjs_frame("2020")
        self._setup_page(page, [year_frame])
        page.input_value.return_value = "2020"

        records = fetch_and_parse(page, 2020)

        assert len(records) == 2
        assert records[0]["school"] == "School2020"
        page.select_option.assert_called_once_with("#year", "2020")

    @patch("nfl_draft_scraper.jl_bb_scraper.time")
    def test_clears_initial_frames_uses_year_specific(self, mock_time: MagicMock) -> None:
        """Verify initial load frames are discarded; only post-selection frames used."""
        mock_time.sleep = MagicMock()
        page = MagicMock()
        # The year frame will be delivered on select_option
        year_frame = _make_sockjs_frame("2020")
        self._setup_page(page, [year_frame])
        page.input_value.return_value = "2020"

        # Simulate an initial-load frame that was captured before year selection.
        # In the real code, ws_frames.clear() after initial load should discard this.
        records = fetch_and_parse(page, 2020)

        # Should get 2020 data, not default data
        assert all(r["school"] == "School2020" for r in records)

    @patch("nfl_draft_scraper.jl_bb_scraper.time")
    def test_raises_on_no_frames(self, mock_time: MagicMock) -> None:
        """Verify RuntimeError when no WebSocket frames arrive after year selection."""
        mock_time.sleep = MagicMock()
        page = MagicMock()
        self._setup_page(page, [])  # No frames delivered
        with pytest.raises(RuntimeError, match="No large WebSocket frames"):
            fetch_and_parse(page, 2020)

    @patch("nfl_draft_scraper.jl_bb_scraper.time")
    def test_raises_on_year_mismatch(self, mock_time: MagicMock) -> None:
        """Verify ValueError when dropdown still shows wrong year after update."""
        mock_time.sleep = MagicMock()
        page = MagicMock()
        year_frame = _make_sockjs_frame("2020")
        self._setup_page(page, [year_frame])
        page.input_value.return_value = "2026"  # Wrong year in dropdown

        with pytest.raises(ValueError, match="Year mismatch"):
            fetch_and_parse(page, 2020)

    @patch("nfl_draft_scraper.jl_bb_scraper.time")
    def test_picks_largest_frame_after_clear(self, mock_time: MagicMock) -> None:
        """Verify the largest post-selection frame is used when multiple arrive."""
        mock_time.sleep = MagicMock()
        page = MagicMock()
        # Both frames must exceed _MIN_FRAME_LEN (1000) to be captured.
        small_frame = _make_sockjs_frame("2020", player_count=5)
        big_frame = _make_sockjs_frame("2020", player_count=10)
        assert len(small_frame) > 1000, "small frame must exceed _MIN_FRAME_LEN"
        assert len(big_frame) > len(small_frame), "big frame must be larger"
        # Send small → big → small to cover both "larger" and "not larger" branches
        self._setup_page(page, [small_frame, big_frame, small_frame])
        page.input_value.return_value = "2020"

        records = fetch_and_parse(page, 2020)

        # Should still pick the bigger frame (10 players)
        assert len(records) == 10


# ---------------------------------------------------------------------------
# scrape_year
# ---------------------------------------------------------------------------


class TestScrapeYear:
    """Tests for scrape_year."""

    @patch("nfl_draft_scraper.jl_bb_scraper.save_csv")
    @patch("nfl_draft_scraper.jl_bb_scraper.fetch_and_parse")
    def test_saves_to_correct_filename(self, mock_fetch: MagicMock, mock_save: MagicMock) -> None:
        """Verify the CSV is saved with the jl_big_board naming convention."""
        mock_fetch.return_value = [{"rank": "1", "name": "Test Player"}]
        page = MagicMock()

        scrape_year(2020, page)

        mock_save.assert_called_once_with(
            "jl_big_board_2020.csv",
            [{"rank": "1", "name": "Test Player"}],
        )

    @patch("nfl_draft_scraper.jl_bb_scraper.save_csv")
    @patch("nfl_draft_scraper.jl_bb_scraper.fetch_and_parse")
    def test_returns_records(self, mock_fetch: MagicMock, mock_save: MagicMock) -> None:
        """Verify scrape_year returns the parsed records."""
        expected = [{"rank": "1", "name": "Player A"}]
        mock_fetch.return_value = expected
        page = MagicMock()

        result = scrape_year(2020, page)

        assert result == expected

    @patch("nfl_draft_scraper.jl_bb_scraper.save_csv")
    @patch("nfl_draft_scraper.jl_bb_scraper.fetch_and_parse")
    def test_passes_page_and_year(self, mock_fetch: MagicMock, mock_save: MagicMock) -> None:
        """Verify fetch_and_parse receives the page and year arguments."""
        mock_fetch.return_value = [{"rank": "1", "name": "Test"}]
        page = MagicMock()

        scrape_year(2023, page)

        mock_fetch.assert_called_once_with(page, 2023)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMain:
    """Tests for the main entry point."""

    @patch("nfl_draft_scraper.jl_bb_scraper._rng")
    @patch("nfl_draft_scraper.jl_bb_scraper.scrape_year")
    @patch("nfl_draft_scraper.jl_bb_scraper.sync_playwright")
    def test_iterates_all_years(
        self,
        mock_pw: MagicMock,
        mock_scrape: MagicMock,
        mock_rng: MagicMock,
        monkeypatch,
    ) -> None:
        """Verify main scrapes every year in the configured range."""
        monkeypatch.setattr("nfl_draft_scraper.jl_bb_scraper.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.jl_bb_scraper.constants.END_YEAR", 2021)
        mock_rng.uniform.return_value = 0

        # Set up the Playwright context manager chain
        mock_browser = MagicMock()
        mock_page = MagicMock()
        mock_browser.new_page.return_value = mock_page
        mock_pw.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_pw.return_value.__enter__.return_value.chromium.launch.return_value = mock_browser
        mock_scrape.return_value = []

        main()

        assert mock_scrape.call_count == 2
        mock_scrape.assert_any_call(2020, mock_page)
        mock_scrape.assert_any_call(2021, mock_page)

    @patch("nfl_draft_scraper.jl_bb_scraper._rng")
    @patch("nfl_draft_scraper.jl_bb_scraper.scrape_year")
    @patch("nfl_draft_scraper.jl_bb_scraper.sync_playwright")
    def test_launches_headless_browser(
        self,
        mock_pw: MagicMock,
        mock_scrape: MagicMock,
        mock_rng: MagicMock,
        monkeypatch,
    ) -> None:
        """Verify main launches a headless Chromium browser."""
        monkeypatch.setattr("nfl_draft_scraper.jl_bb_scraper.constants.START_YEAR", 2020)
        monkeypatch.setattr("nfl_draft_scraper.jl_bb_scraper.constants.END_YEAR", 2020)
        mock_rng.uniform.return_value = 0

        mock_context = MagicMock()
        mock_browser = MagicMock()
        mock_context.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = MagicMock()
        mock_pw.return_value.__enter__ = MagicMock(return_value=mock_context)
        mock_scrape.return_value = []

        main()

        mock_context.chromium.launch.assert_called_once_with(headless=True)
        mock_browser.close.assert_called_once()
