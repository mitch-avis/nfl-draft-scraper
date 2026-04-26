"""Tests for nfl_draft_scraper.utils.csv_utils."""

from __future__ import annotations

import os

import polars as pl
import pytest

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
        df = pl.read_csv(path)
        assert df.height == 2
        assert df.columns == ["name", "rank"]


class TestWriteDfToCsv:
    """Tests for write_df_to_csv."""

    def test_writes_and_reads_back(self, tmp_path):
        """Verify writes and reads back."""
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        path = str(tmp_path / "output.csv")
        write_df_to_csv(df, path, index=False)
        result = pl.read_csv(path)
        assert result.columns == ["a", "b"]
        assert result.height == 2

    def test_writes_index_column_by_default(self, tmp_path):
        """Verify writes index column by default."""
        df = pl.DataFrame({"a": [1, 2]})
        path = str(tmp_path / "indexed.csv")
        write_df_to_csv(df, path)
        result = pl.read_csv(path)
        assert result.columns == ["", "a"]
        assert result[""].to_list() == [0, 1]

    def test_creates_directory(self, tmp_path):
        """Verify creates directory."""
        path = str(tmp_path / "subdir" / "output.csv")
        df = pl.DataFrame({"a": [1]})
        write_df_to_csv(df, path)
        assert os.path.exists(path)

    def test_handles_bare_filename_without_parent(self, tmp_path, monkeypatch):
        """Verify writing to a bare filename (no parent directory) does not error."""
        monkeypatch.chdir(tmp_path)
        df = pl.DataFrame({"a": [1]})
        write_df_to_csv(df, "bare.csv", index=False)
        assert (tmp_path / "bare.csv").exists()


class TestReadDfFromCsv:
    """Tests for read_df_from_csv."""

    def test_reads_csv_with_index_column(self, tmp_path):
        """Verify reads csv and strips the leading empty-header index column."""
        path = str(tmp_path / "input.csv")
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        write_df_to_csv(df, path, index=True)
        result = read_df_from_csv(path, check_exists=True)
        assert result.columns == ["a", "b"]
        assert result.height == 2

    def test_reads_csv_without_index_column(self, tmp_path):
        """Verify reads csv that has no leading index column."""
        path = str(tmp_path / "noindex.csv")
        pl.DataFrame({"a": [1, 2], "b": [3, 4]}).write_csv(path)
        result = read_df_from_csv(path, check_exists=True)
        assert result.columns == ["a", "b"]
        assert result.height == 2

    def test_exits_on_missing_file(self, tmp_path):
        """Verify exits on missing file."""
        with pytest.raises(SystemExit):
            read_df_from_csv(str(tmp_path / "nonexistent.csv"), check_exists=True)
