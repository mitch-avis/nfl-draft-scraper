"""Minimal stub for sportsipy.nfl.roster — declares only the Player surface we use."""

from typing import Any

import polars as pl

class Player:
    """A NFL player as scraped by sportsipy."""

    dataframe: pl.DataFrame | None
    def __init__(self, player_id: str) -> None: ...
    def __getattr__(self, name: str) -> Any: ...
