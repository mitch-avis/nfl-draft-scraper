"""Tests for nfl_draft_scraper.utils.csv_utils."""

import os

import pandas as pd

from nfl_draft_scraper.utils.csv_utils import read_df_from_csv, save_csv, write_df_to_csv


class TestSaveCsv:
    """Tests for save_csv."""

    def test_writes_csv_file(self, tmp_path, monkeypatch):
        """Verify writes csv file."""
        monkeypatch.setattr("nfl_draft_scraper.utils.csv_utils.constants.DATA_PATH", tmp_path)
        records = [{"name": "Alice", "rank": 1}, {"name": "Bob", "rank": 2}]
        save_csv("test.csv", records)
        path = os.path.join(str(tmp_path), "test.csv")
        assert os.path.exists(path)
        df = pd.read_csv(path)
        assert len(df) == 2
        assert list(df.columns) == ["name", "rank"]


class TestWriteDfToCsv:
    """Tests for write_df_to_csv."""

    def test_writes_and_reads_back(self, tmp_path):
        """Verify writes and reads back."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        path = str(tmp_path / "output.csv")
        write_df_to_csv(df, path, index=False)
        result = pd.read_csv(path)
        assert list(result.columns) == ["a", "b"]
        assert len(result) == 2

    def test_creates_directory(self, tmp_path):
        """Verify creates directory."""
        path = str(tmp_path / "subdir" / "output.csv")
        df = pd.DataFrame({"a": [1]})
        write_df_to_csv(df, path)
        assert os.path.exists(path)


class TestReadDfFromCsv:
    """Tests for read_df_from_csv."""

    def test_reads_csv(self, tmp_path):
        """Verify reads csv."""
        path = str(tmp_path / "input.csv")
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_csv(path, index=True)
        result = read_df_from_csv(path, check_exists=True)
        assert len(result) == 2

    def test_exits_on_missing_file(self, tmp_path):
        """Verify exits on missing file."""
        import pytest

        with pytest.raises(SystemExit):
            read_df_from_csv(str(tmp_path / "nonexistent.csv"), check_exists=True)
