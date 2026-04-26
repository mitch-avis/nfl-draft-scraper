"""CSV utility functions."""

from __future__ import annotations

import csv
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl

from nfl_draft_scraper import constants
from nfl_draft_scraper.utils.logger import log


def save_csv(file_name: str, records: list[dict[str, Any]]) -> None:
    """Save the records to a CSV file."""
    constants.DATA_PATH.mkdir(parents=True, exist_ok=True)
    file_path = constants.DATA_PATH / file_name
    with open(file_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)


def read_write_data(
    data_name: str,
    func: Callable[..., Any],
    *args: Any,
    force_refresh: bool = False,
    **kwargs: Any,
) -> pl.DataFrame:
    """Read data from CSV or generate it using a specified function, then write it back.

    This function checks if a CSV file with the given data name exists. If it does and force_refresh
    is False, it reads the data from the file. Otherwise, it generates the data by calling the
    provided function and writes the new data to a CSV file.

    Args:
        data_name (str): The base name of the data file (without extension).
        func (Callable): The function to generate data if needed.
        *args: Positional arguments to pass to the data generation function.
        force_refresh (bool, optional): If True, forces data regeneration. Defaults to False.
        **kwargs: Keyword arguments to pass to the data generation function.

    Returns:
        pl.DataFrame: The data as a polars DataFrame.

    """
    dataframe: pl.DataFrame = pl.DataFrame()
    file_path = str(constants.DATA_PATH / f"{data_name}.csv")

    # Check if the CSV file exists and read it if force_refresh is not True
    if os.path.isfile(file_path) and not force_refresh:
        dataframe = read_df_from_csv(file_path, check_exists=False)

    # If the DataFrame is empty (file doesn't exist) or force_refresh is True, generate the data
    if dataframe.is_empty() or force_refresh:
        log.debug("* Calling %s()", getattr(func, "__name__", repr(func)))
        dataframe = pl.DataFrame(func(*args, **kwargs))
        # Write the generated DataFrame to a CSV file
        write_df_to_csv(dataframe, file_path)

    return dataframe


def read_df_from_csv(file_path: str | Path, check_exists: bool = True) -> pl.DataFrame:
    """Read a DataFrame from a CSV file.

    If the CSV was written with a leading unnamed index column (the polars convention used by
    ``write_df_to_csv`` when ``index=True``), that column is dropped on read so callers see only the
    data columns.

    Args:
        file_path (str | Path): The path to the CSV file.
        check_exists (bool, optional): Whether to check if the file exists before reading.
            Defaults to True.

    Returns:
        pl.DataFrame: The data read from the CSV file.

    """
    if check_exists and not os.path.isfile(file_path):
        log.error("%s not found!", os.path.basename(file_path))
        sys.exit(1)
    dataframe = pl.read_csv(file_path)
    # Drop a leading unnamed index column ("" header), if present
    if dataframe.width > 0 and dataframe.columns[0] == "":
        dataframe = dataframe.drop("")
    return dataframe


def write_df_to_csv(dataframe: pl.DataFrame, file_path: str | Path, index: bool = True) -> None:
    """Write a DataFrame to a CSV file.

    If the directory for the file does not exist, it is created. When *index* is True (the default)
    a synthetic 0-based row-index column is written as the first column with an empty header,
    matching the legacy pandas ``to_csv(index=True)`` layout used by other tools in this repo.

    Args:
        dataframe (pl.DataFrame): The DataFrame to write.
        file_path (str | Path): The path to the CSV file where the data should be written.
        index (bool, optional): Whether to include the row index in the CSV file. Defaults to True.

    """
    parent = os.path.dirname(file_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    if index:
        out = dataframe.with_columns(
            pl.int_range(0, dataframe.height, dtype=pl.Int64).alias("")
        ).select(["", *dataframe.columns])
    else:
        out = dataframe
    out.write_csv(file_path)
    log.debug("Data written to %s", os.path.basename(file_path))
