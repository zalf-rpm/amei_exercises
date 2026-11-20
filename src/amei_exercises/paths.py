"""Where the data of the exercises are: the repository's top-level folders (the scripts are in src/)."""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ET_DIR = REPO / "evapotranspiration"  # the AgMIP Wheat ET2 study
ST_DIR = REPO / "soil_temperature"  # the AMEI soil temperature model intercomparison
MARICOPA_DIR = REPO / "maricopa_wheat_face"  # the Maricopa flows and their workbook

__all__ = ["ET_DIR", "MARICOPA_DIR", "REPO", "ST_DIR"]
