"""CSV utility functions."""

from __future__ import annotations

import csv
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd

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
    data_name: str, func: Callable[..., Any], *args, force_refresh: bool = False, **kwargs
) -> pd.DataFrame:
    """Read data from CSV or generate it using a specified function, then write it back.

    This function checks if a CSV file with the given data name exists. If it does and
    force_refresh is False, it reads the data from the file. Otherwise, it generates the data by
    calling the provided function and writes the new data to a CSV file.

    Args:
        data_name (str): The base name of the data file (without extension).
        func (Callable): The function to generate data if needed.
        *args: Positional arguments to pass to the data generation function.
        force_refresh (bool, optional): If True, forces data regeneration. Defaults to False.
        **kwargs: Keyword arguments to pass to the data generation function.

    Returns:
        pd.DataFrame: The data as a pandas DataFrame.

    """
    # Initialize an empty DataFrame
    dataframe = pd.DataFrame()
    file_path = str(constants.DATA_PATH / f"{data_name}.csv")

    # Check if the CSV file exists and read it if force_refresh is not True
    if os.path.isfile(file_path) and not force_refresh:
        dataframe = read_df_from_csv(file_path, check_exists=False)

    # If the DataFrame is empty (file doesn't exist) or force_refresh is True, generate the data
    if dataframe.empty or force_refresh:
        log.debug("* Calling %s()", getattr(func, "__name__", repr(func)))
        dataframe = pd.DataFrame(func(*args, **kwargs))
        # Write the generated DataFrame to a CSV file
        write_df_to_csv(dataframe, file_path)

    return dataframe


def read_df_from_csv(file_path: str | Path, check_exists: bool = True) -> pd.DataFrame:
    """Read a DataFrame from a CSV file.

    If check_exists is True, the function first checks if the file exists. If it does not, logs an
    error message and exits the program.

    Args:
        file_path (str): The path to the CSV file.
        check_exists (bool, optional):  Whether to check if the file exists before reading. Defaults
                                        to True.

    Returns:
        pd.DataFrame: The data read from the CSV file.

    """
    # Check if the file exists, if required
    if check_exists and not os.path.isfile(file_path):
        # Log an error message and exit if the file does not exist
        log.error("%s not found!", os.path.basename(file_path))
        sys.exit(1)
    # Read the CSV file into a DataFrame, using the first column as the index
    dataframe = pd.read_csv(file_path, index_col=0)
    return dataframe


def write_df_to_csv(dataframe: pd.DataFrame, file_path: str | Path, index: bool = True) -> None:
    """Write a DataFrame to a CSV file.

    If the directory for the file does not exist, it is created. The DataFrame is then written to
    the file, including the index.

    Args:
        dataframe (pd.DataFrame): The DataFrame to write.
        file_path (str): The path to the CSV file where the data should be written.
        index (bool, optional): Whether to include the index in the CSV file. Defaults to True.

    """
    # Create the directory if it does not exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # Write the DataFrame to the CSV file, including the index
    dataframe.to_csv(file_path, index=index)
    # Log a message indicating the successful write
    log.debug("Data written to %s", os.path.basename(file_path))
