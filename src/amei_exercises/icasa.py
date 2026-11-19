"""Readers for workbooks in the ICASA Excel format v4.1 (Bushland, Maricopa): three header rows
(names, units, codes), sheets keyed by TREAT_ID.
"""

from __future__ import annotations

import re
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from amei_exercises.et_monica import WEATHER_COLUMNS, Horizon, nan_to_none, sat_vapor_pressure


def read_sheets(xlsx: Path) -> dict[str, pd.DataFrame]:
    """All sheets, with the code row (row 3) as header and the empty rows/columns dropped."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "Data Validation extension", UserWarning)  # openpyxl, harmless
        sheets = pd.read_excel(xlsx, sheet_name=None, header=2)
    return {
        name: df.dropna(how="all")
        .dropna(axis=1, how="all")
        .loc[:, lambda d: ~d.columns.astype(str).str.startswith("Unnamed")]
        for name, df in sheets.items()
    }


def treatment_id_resolver(treatments: dict[str, dict[str, Any]]):
    """Maps the treatment ids used in the other sheets to the ones of the Treatments sheet.

    Treatments/Plots/Planting/Fertilizer use TXBL1990F (harvest year), while Irrigation, the
    observations and ET_Daily use TXBL1989F (sowing year) for the same season. An id that isn't in
    the Treatments sheet is matched on the same prefix and suffix with either the sowing or the
    harvest year of a treatment.
    """
    pattern = re.compile(r"^(.*?)(\d{4})(.*)$")

    def resolve(tid: str) -> str | None:
        if tid in treatments:
            return tid
        m = pattern.match(tid)
        if not m:
            return None
        prefix, year, suffix = m.group(1), int(m.group(2)), m.group(3)
        candidates = [
            t
            for t, r in treatments.items()
            if (p := pattern.match(t))
            and p.group(1) == prefix
            and p.group(3) == suffix
            and year in (pd.Timestamp(r["SDAT"]).year, pd.Timestamp(r["ENDAT"]).year)
        ]
        return candidates[0] if len(candidates) == 1 else None

    return resolve


def weather(sheets: dict[str, pd.DataFrame], dataset: str) -> pd.DataFrame:
    """MONICA's weather from the Weather_daily sheet.

    - VPRSD is the vapor pressure [kPa] (Bushland: it equals ea(TDEW) exactly, see checks.py).
    - The RH columns are not used: Bushland's RHUMD is labelled "relative_humidity_mean", but it
      pairs with RHMXD (the maximum) and fits the minimum slightly better (checks.py); in Avignon
      the same code is defined as the minimum. The mean RH MONICA wants is derived as ea / es_mean,
      as for Avignon (and as Maricopa's RHAVD was computed).
    """
    w = sheets["Weather_daily"]
    w = w[w["WST_DATASET"] == dataset]
    es = (sat_vapor_pressure(w["TMAX"]) + sat_vapor_pressure(w["TMIN"])) / 2
    return pd.DataFrame(
        {
            "tmin": w["TMIN"].to_numpy(),
            "tavg": w["TAVD"].to_numpy(),
            "tmax": w["TMAX"].to_numpy(),
            "precip": w["RAIN"].to_numpy(),
            "globrad": w["SRAD"].to_numpy(),  # MJ m-2 d-1
            "wind": (w["WIND"] / 86.4).to_numpy(),  # km d-1 -> m s-1
            "relhumid": (100 * w["VPRSD"] / es).clip(upper=100).to_numpy(),
            "vaporpress": w["VPRSD"].to_numpy(),  # kPa
        },
        index=pd.to_datetime(w["W_DATE"]).dt.date,
    )[WEATHER_COLUMNS]


def horizons(sheets: dict[str, pd.DataFrame], soil_id: str, notes: list[str]) -> list[Horizon]:
    hs = sheets["Soil_profile_layers"]
    hs = hs[hs["SOIL_ID"] == soil_id].sort_values("SLLT").reset_index(drop=True)
    # a value missing in a deeper horizon (Bushland: pH below 160 cm) is taken from the one above
    for col in ("SLSND", "SLCLY", "SLOC", "SLBDM", "SLDUL", "SLLL", "SLSAT", "SLPHW", "C_N"):
        gaps = hs.index[hs[col].isna()].tolist()
        if gaps:
            notes.append(
                f"{col} missing for horizon(s) starting at {hs.loc[gaps, 'SLLT'].tolist()} cm, taken from above"
            )
            hs[col] = hs[col].ffill()
    return [
        Horizon(
            top_cm=float(h["SLLT"]),
            bottom_cm=float(h["SLLB"]),
            sand=float(h["SLSND"]),
            clay=float(h["SLCLY"]),
            soc=float(h["SLOC"]),
            bulk_density=float(h["SLBDM"]),
            fc=float(h["SLDUL"]),
            pwp=float(h["SLLL"]),
            sat=float(h["SLSAT"]),
            ph=nan_to_none(h.get("SLPHW")),
            cn=nan_to_none(h.get("C_N")),
            stones=nan_to_none(h.get("SLCF")),
        )
        for h in hs.to_dict("records")
    ]
