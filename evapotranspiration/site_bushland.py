"""Bushland (Texas, USA): AMEI_Bushland_v4_2026-01-24(no_soil_T).xlsx -> Setups.

ICASA layout: three header rows (names, units, codes), sheets keyed by TREAT_ID.
"""

from __future__ import annotations

import logging
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd
from et_monica import (
    HERE,
    WEATHER_COLUMNS,
    Fertilization,
    Horizon,
    Observations,
    Setup,
    nan_to_none,
    to_date,
)

logger = logging.getLogger("site_bushland")

NAME = "Bushland"
XLSX = HERE / "AMEI_Bushland_v4_2026-01-24(no_soil_T).xlsx"
NO_OF_LAYERS = 42  # 2.1 m of the 2.4 m profile
CULTIVAR = "crops/cultivar_winter-wheat.json"


def read_sheets(xlsx: Path) -> dict[str, pd.DataFrame]:
    """All sheets, with the code row (row 3) as header and the empty rows/columns dropped."""
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
    w = sheets["Weather_daily"]
    w = w[w["WST_DATASET"] == dataset]
    return pd.DataFrame(
        {
            "tmin": w["TMIN"].to_numpy(),
            "tavg": w["TAVD"].to_numpy(),
            "tmax": w["TMAX"].to_numpy(),
            "precip": w["RAIN"].to_numpy(),
            "globrad": w["SRAD"].to_numpy(),  # MJ m-2 d-1
            "wind": (w["WIND"] / 86.4).to_numpy(),  # km d-1 -> m s-1
            "relhumid": w["RHUMD"].to_numpy(),  # mean daily RH here (in Avignon it's the minimum)
            "vaporpress": w["VPRSD"].to_numpy(),  # kPa; checked: equals ea(TDEW)
        },
        index=pd.to_datetime(w["W_DATE"]).dt.date,
    )[WEATHER_COLUMNS]


def horizons(sheets: dict[str, pd.DataFrame], soil_id: str, notes: list[str]) -> list[Horizon]:
    hs = sheets["Soil_profile_layers"]
    hs = hs[hs["SOIL_ID"] == soil_id].sort_values("SLLT").reset_index(drop=True)
    # a value missing in a deeper horizon (pH below 160 cm) is taken from the one above
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


def load(xlsx: Path = XLSX) -> list[Setup]:
    sheets = read_sheets(xlsx)
    treatments = {str(r["TREAT_ID"]): r for r in sheets["Treatments"].to_dict("records")}
    resolve = treatment_id_resolver(treatments)

    rows: dict[str, dict[str, list[dict[str, Any]]]] = {t: {} for t in treatments}
    unmatched: dict[str, set[str]] = {}
    for name, df in sheets.items():
        if name == "Treatments" or "TREAT_ID" not in df.columns:
            continue
        for r in df.to_dict("records"):
            tid = resolve(str(r["TREAT_ID"]))
            if tid is None:
                unmatched.setdefault(name, set()).add(str(r["TREAT_ID"]))
            else:
                rows[tid].setdefault(name, []).append(r)
    for name, ids in unmatched.items():
        logger.warning("%s: rows of unknown treatments %s skipped", name, sorted(ids))

    def one(tid: str, sheet: str) -> dict[str, Any]:
        found = rows[tid].get(sheet, [])
        if len(found) != 1:
            msg = f"{tid}: expected one row in {sheet}, found {len(found)}"
            raise ValueError(msg)
        return found[0]

    field = sheets["Fields"].set_index("FIELD_ID")
    station = sheets["Weather_stations"].set_index("WST_ID")
    soil_meta = sheets["Soil_metadata"].set_index("SOIL_ID")

    setups = []
    for tid, t in treatments.items():
        notes: list[str] = []
        soil_id = one(tid, "Plots")["SOIL_ID"]
        fl = field.loc[t["FIELD_ID"]]
        st = station.loc[t["wst_id"]]

        fertilizations = []
        for f in rows[tid].get("Fertilizer_events", []):
            kg_n = float(f["FEAMN"])
            if kg_n <= 0:
                continue
            nh4, no3 = float(f.get("FENH4") or 0), float(f.get("FENO3") or 0)
            known = nh4 + no3 > 0
            fertilizations.append(
                Fertilization(
                    to_date(f["FEDATE"]),
                    kg_n,
                    nh4 / kg_n if known else 1.0,
                    no3 / kg_n if known else 0.0,
                    str(f["FECD"]),
                )
            )

        et = pd.DataFrame(rows[tid].get("ET_Daily", []))
        summary = rows[tid].get("Obs_crop_summary_means", [{}])[0]
        icin = nan_to_none(one(tid, "initial_conditions").get("ICIN"))

        setups.append(
            Setup(
                id=tid,
                start=to_date(t["SDAT"]),
                end=to_date(t["ENDAT"]),
                latitude=float(fl["FL_LAT"]),
                elevation=float(fl["FLELE"]),
                slope=math.tan(math.radians(float(fl["FLSL"]))),  # degree -> m m-1
                weather=weather(sheets, t["WST_DATASET"]),
                wind_height=float(st["WNDHT"]),
                co2=nan_to_none(st.get("CO2Y")),
                albedo=nan_to_none(soil_meta.loc[soil_id].get("SALB")),  # as in the Maricopa flow
                horizons=horizons(sheets, soil_id, notes),
                initial_moisture=[
                    (float(r["ICTL"]), float(r["ICBL"]), float(r["ICH2O"]))
                    for r in sorted(rows[tid].get("initial_condition_layers", []), key=lambda r: r["ICTL"])
                ],
                initial_n_total_kg_ha=None if icin is None else float(icin),
                sowing=to_date(one(tid, "Planting_events")["PDATE"]),
                harvest=to_date(one(tid, "Harvest_events")["HADAT"]),
                fertilizations=fertilizations,
                # not int(): the flow component's int() cut 10.2 mm down to 10 mm
                irrigations=[
                    (to_date(i["IDATE"]), float(i["IRVAL"]))
                    for i in rows[tid].get("Irrigation_events", [])
                    if float(i["IRVAL"]) > 0
                ],
                cultivar=CULTIVAR,
                no_of_layers=NO_OF_LAYERS,
                obs=Observations(
                    et=pd.Series(et["ETAD"].astype(float).to_numpy(), index=pd.to_datetime(et["DATE"]).dt.date)
                    if len(et)
                    else pd.Series(dtype=float),
                    emergence=to_date(summary.get("PLDAE")),
                    anthesis=to_date(summary.get("ADAT")),
                    maturity=to_date(summary.get("MDAT")),
                    lai_max=nan_to_none(summary.get("LAIX")),
                    grain_yield=nan_to_none(summary.get("HWAM")),
                ),
                notes=notes,
            )
        )
    return setups
