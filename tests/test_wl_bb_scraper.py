"""Tests for nfl_draft_scraper.wl_bb_scraper."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nfl_draft_scraper.wl_bb_scraper import (
    UnknownYearError,
    fetch_csv,
    parse_big_board,
    scrape_year,
)

_SAMPLE_CSV = (
    "Wide Left Consensus Big Board,,,,,,,,,\n"
    "Compiled by Arif Hasan (@ArifHasanNFL),,,,,,,,,\n"
    "Find out more at wideleft.football,,,,,,,,,\n"
    "Ovr,Fore,Eval,Player,School,Position,Pos Rk,Variance,Pick,Team\n"
    "1,1,1,Fernando Mendoza,Indiana,QB,1,99.1,1,Las Vegas Raiders\n"
    "2,2,2,Jeremiyah Love,Notre Dame,RB,1,80.0,5,New York Giants\n"
    ",,,Notes go here,,,,,,\n"
    "4,4,4,T.J. Parker,Clemson,EDGE,1,70.5,8,Cleveland Browns\n"
)

_SAMPLE_CSV_CAPS_PLAYER_HEADER = (
    "Wide Left Consensus Big Board,,,,,,,,,\n"
    "Compiled by Arif Hasan (@ArifHasanNFL),,,,,,,,,\n"
    "Find out more at wideleft.football,,,,,,,,,\n"
    "Ovr,Fore,Eval,PLAYER,School,Position,Pos Rk,Variance,Pick,Team\n"
    "1,2,1,Marvin Harrison Jr.,Ohio State,WR,1,79.2,4,Arizona Cardinals\n"
)


class TestParseBigBoard:
    """Tests for parse_big_board."""

    def test_parses_valid_rows(self):
        """Verify numeric Ovr rows are turned into rank/name/pos/school records."""
        rows = parse_big_board(_SAMPLE_CSV)
        assert rows == [
            {"rank": "1", "name": "Fernando Mendoza", "pos": "QB", "school": "Indiana"},
            {"rank": "2", "name": "Jeremiyah Love", "pos": "RB", "school": "Notre Dame"},
            {"rank": "4", "name": "T.J. Parker", "pos": "EDGE", "school": "Clemson"},
        ]

    def test_handles_uppercase_player_header(self):
        """Verify the legacy 'PLAYER' header (2024) is matched case-insensitively."""
        rows = parse_big_board(_SAMPLE_CSV_CAPS_PLAYER_HEADER)
        assert len(rows) == 1
        assert rows[0]["name"] == "Marvin Harrison Jr."

    def test_skips_blank_ovr_rows(self):
        """Verify rows whose Ovr value is blank or non-numeric are skipped."""
        rows = parse_big_board(_SAMPLE_CSV)
        assert "Notes go here" not in {r["name"] for r in rows}

    def test_returns_empty_list_when_no_header(self):
        """Verify a CSV missing the canonical header row yields an empty list."""
        rows = parse_big_board("just,some,unrelated,text\n1,2,3,4\n")
        assert rows == []

    def test_returns_empty_list_when_csv_is_empty(self):
        """Verify an empty CSV string yields an empty list."""
        assert parse_big_board("") == []


class TestFetchCsv:
    """Tests for fetch_csv."""

    def test_unknown_year_raises(self, monkeypatch):
        """Verify a year missing from WL_SHEET_IDS raises UnknownYearError."""
        from nfl_draft_scraper import constants

        monkeypatch.setattr(constants, "WL_SHEET_IDS", {2026: "abc"})
        with pytest.raises(UnknownYearError):
            fetch_csv(2019)

    def test_fetches_csv_export_url(self, monkeypatch):
        """Verify fetch_csv requests the gviz csv export URL for the year's sheet."""
        from nfl_draft_scraper import constants, wl_bb_scraper

        monkeypatch.setattr(constants, "WL_SHEET_IDS", {2026: "sheet-id-xyz"})
        mock_response = MagicMock()
        mock_response.text = _SAMPLE_CSV
        mock_response.raise_for_status = MagicMock()
        mock_get = MagicMock(return_value=mock_response)
        monkeypatch.setattr(wl_bb_scraper.requests, "get", mock_get)

        result = fetch_csv(2026)

        assert result == _SAMPLE_CSV
        called_url = mock_get.call_args.args[0]
        assert "sheet-id-xyz" in called_url
        assert "format=csv" in called_url


class TestScrapeYear:
    """Tests for scrape_year."""

    def test_fetches_parses_and_saves(self, monkeypatch):
        """Verify scrape_year wires fetch_csv → parse_big_board → save_csv."""
        from nfl_draft_scraper import wl_bb_scraper

        mock_fetch = MagicMock(return_value=_SAMPLE_CSV)
        mock_save = MagicMock()
        monkeypatch.setattr(wl_bb_scraper, "fetch_csv", mock_fetch)
        monkeypatch.setattr(wl_bb_scraper, "save_csv", mock_save)

        result = scrape_year(2026)

        mock_fetch.assert_called_once_with(2026)
        mock_save.assert_called_once()
        assert mock_save.call_args.args[0] == "wl_big_board_2026.csv"
        assert len(result) == 3


class TestMain:
    """Tests for main."""

    def test_iterates_known_years_in_range_and_sleeps(self, monkeypatch):
        """Verify main scrapes only years that are both in WL_SHEET_IDS and in range."""
        from nfl_draft_scraper import constants, wl_bb_scraper

        monkeypatch.setattr(constants, "START_YEAR", 2024)
        monkeypatch.setattr(constants, "END_YEAR", 2026)
        monkeypatch.setattr(constants, "WL_SHEET_IDS", {2024: "a", 2025: "b", 2026: "c"})
        mock_scrape = MagicMock()
        mock_sleep = MagicMock()
        monkeypatch.setattr(wl_bb_scraper, "scrape_year", mock_scrape)
        monkeypatch.setattr(wl_bb_scraper.time, "sleep", mock_sleep)

        wl_bb_scraper.main()

        assert mock_scrape.call_args_list == [((2024,),), ((2025,),), ((2026,),)]
        assert mock_sleep.call_count == 3

    def test_skips_years_without_sheet_id(self, monkeypatch):
        """Verify main skips years that have no sheet id and logs a warning."""
        from nfl_draft_scraper import constants, wl_bb_scraper

        monkeypatch.setattr(constants, "START_YEAR", 2018)
        monkeypatch.setattr(constants, "END_YEAR", 2025)
        monkeypatch.setattr(constants, "WL_SHEET_IDS", {2024: "a", 2025: "b"})
        mock_scrape = MagicMock()
        monkeypatch.setattr(wl_bb_scraper, "scrape_year", mock_scrape)
        monkeypatch.setattr(wl_bb_scraper.time, "sleep", MagicMock())

        wl_bb_scraper.main()

        assert mock_scrape.call_args_list == [((2024,),), ((2025,),)]


class TestParseBigBoardEdgeCases:
    """Cover residual branches in parse_big_board."""

    def test_header_missing_required_column_returns_empty(self):
        """Verify a header that lacks one of the required columns yields an empty list."""
        csv_text = (
            "junk1,,,,\n"
            "junk2,,,,\n"
            "junk3,,,,\n"
            "Ovr,Fore,Eval,Player,School\n"  # missing Position
            "1,1,1,X,Y\n"
        )
        assert parse_big_board(csv_text) == []

    def test_short_row_is_skipped(self):
        """Verify a data row with fewer columns than the header is skipped."""
        csv_text = (
            "junk,,,,,,,,,\n"
            "junk,,,,,,,,,\n"
            "junk,,,,,,,,,\n"
            "Ovr,Fore,Eval,Player,School,Position,Pos Rk,Variance,Pick,Team\n"
            "1,1,1,X,Y\n"  # too short
            "2,1,1,Full Row,School,QB,1,80,1,Team\n"
        )
        result = parse_big_board(csv_text)
        assert [r["name"] for r in result] == ["Full Row"]

    def test_blank_name_row_is_skipped(self):
        """Verify a numeric row with a blank player name is skipped."""
        csv_text = (
            "junk,,,,,,,,,\n"
            "junk,,,,,,,,,\n"
            "junk,,,,,,,,,\n"
            "Ovr,Fore,Eval,Player,School,Position,Pos Rk,Variance,Pick,Team\n"
            "1,1,1,,,QB,1,80,1,Team\n"
            "2,1,1,Real Player,Sch,QB,1,80,1,Team\n"
        )
        result = parse_big_board(csv_text)
        assert [r["name"] for r in result] == ["Real Player"]
