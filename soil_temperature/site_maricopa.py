"""Maricopa (Arizona, USA), wheat FACE: the ICASA workbook in "Maricopa wheat FACE/" -> Setups.

16 treatments (4 seasons x CO2 x water or N). See FINDINGS.md for what MONICA can't represent.
"""

from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from st_monica import (
    HERE,
    Fertilization,
    Observations,
    Setup,
    horizons,
    nan_to_none,
    read_sheets,
    to_date,
    weather,
)

logger = logging.getLogger("site_maricopa")

NAME = "Maricopa"
TITLE = "Maricopa Wheat FACE"
DATA_DIR = HERE / "Maricopa wheat FACE"
XLSX = DATA_DIR / "MARICOPA Wheat FACE data_2026-10-15 (ICASA data format v4.1)(PM9)(BAK1)(no SoilT).xlsx"
TEMPLATES = {
    "summary": DATA_DIR / "AMIE_template_MarcicopaWheatFACE_summary_output.txt",
    "daily": DATA_DIR / "AMIE_template_MarcicopaWheatFACE_daily_output.txt",
    "layers": DATA_DIR / "AMIE_template_MarcicopaWheatFACE_layers_output.txt",
}
# the parameters of the Maricopa calibration flows; the phenology is calibrated (1st step), LAI not
PARAMS = HERE.parent / "maricopa_wheat_face/params"
CULTIVAR = "crops/cultivar_spring-wheat_with_1_calibrated_phenology.json"
NO_OF_LAYERS = 42  # 2.1 m in 5 cm layers, as the protocol asks
NITROGEN_STRESS = True  # low and high N treatments


def co2_series(start: date, end: date, base: float, mods: list[dict[str, Any]]) -> pd.Series:
    """Daily CO2 [ppm]: the station's CO2Y, changed from each Env_modifications EMDATE on.

    "Replace" sets the concentration, "Add" adds to the current one (1996/97: + 185 ppm in the FACE
    plots).
    """
    mods = sorted(mods, key=lambda m: pd.Timestamp(m["EMDATE"]))
    days, values = [], []
    co2, k, day = base, 0, start
    while day <= end:
        while k < len(mods) and to_date(mods[k]["EMDATE"]) <= day:
            m = mods[k]
            if m["ECCO2"] == "Replace":
                co2 = float(m["EMCO2"])
            elif m["ECCO2"] == "Add":
                co2 += float(m["EMCO2"])
            else:
                msg = f"unknown CO2 modification {m['ECCO2']!r}"
                raise ValueError(msg)
            k += 1
        days.append(day)
        values.append(co2)
        day += timedelta(days=1)
    return pd.Series(values, index=days)


def fertilization(f: dict[str, Any]) -> Fertilization | None:
    """None for applications without N (phosphate only)."""
    kg_n = float(f["FEAMN"])
    if kg_n <= 0:
        return None
    no3, nh4, urea = (float(nan_to_none(f.get(c)) or 0) for c in ("FENO3", "FENH4", "FEUREA"))
    forms = no3 + nh4 + urea
    if forms <= 0:
        msg = f"{f['TREAT_ID']} {f['FEDATE']}: {f['FECD']} with {kg_n} kg N but no NO3/NH4/urea split"
        raise ValueError(msg)
    if abs(forms - kg_n) > 0.01:
        logger.warning(
            "%s %s %s: NO3 + NH4 + urea = %s kg N, FEAMN = %s kg N; FEAMN applied in the ratio of the forms",
            f["TREAT_ID"],
            to_date(f["FEDATE"]),
            f["FECD"],
            forms,
            kg_n,
        )
    return Fertilization(to_date(f["FEDATE"]), kg_n, nh4 / forms, no3 / forms, str(f["FECD"]), urea / forms)


def load(xlsx: Path = XLSX) -> list[Setup]:
    sheets = read_sheets(xlsx)
    treatments = {str(r["TREAT_ID"]): r for r in sheets["Treatments"].to_dict("records")}
    rows: dict[str, dict[str, list[dict[str, Any]]]] = {t: {} for t in treatments}
    for name, df in sheets.items():
        if name == "Treatments" or "TREAT_ID" not in df.columns:
            continue
        for r in df.to_dict("records"):
            tid = str(r["TREAT_ID"])
            if tid in rows:
                rows[tid].setdefault(name, []).append(r)

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
        plots = rows[tid]["Plots"]
        soil_id = plots[0]["SOIL_ID"]
        if any(p["SOIL_ID"] != soil_id for p in plots):
            msg = f"{tid}: plots on different soils"
            raise ValueError(msg)
        fl = field.loc[t["FIELD_ID"]]
        st = station.loc[t["wst_id"]]
        start, end = to_date(t["SDAT"]), to_date(t["ENDAT"])

        residue = one(tid, "Residue")
        notes.append(
            f"residue not simulated: {residue['ICRAG']:g} kg/ha {residue['ICPCR']} ({residue['ICRN']:g} % N) "
            f"+ {residue['ICRT']:g} kg/ha roots, incorporated to {residue['ICRDP']:g} cm"
        )
        irrigations = [r for r in rows[tid].get("Irrigation_events", []) if float(r["IRVAL"]) > 0]

        summary = rows[tid].get("Obs_crop_summary_means", [{}])[0]
        setups.append(
            Setup(
                id=tid,
                start=start,
                end=end,
                latitude=float(fl["FL_LAT"]),
                elevation=float(fl["FLELE"]),
                slope=math.tan(math.radians(float(fl["FLSL"]))),  # degree -> m m-1
                weather=weather(sheets, t["WST_DATASET"]),
                wind_height=float(st["WNDHT"]),
                tav=float(st["TAV"]),
                tamp=float(st["TAMP"]),
                # MONICA's own soil temperature: the lower boundary, its TAV (default 9.5 °C, Germany)
                site_params={"SoilTemperatureParameters": {"BaseTemperature": float(st["TAV"])}},
                co2_daily=co2_series(start, end, float(st["CO2Y"]), rows[tid].get("Env_modifications", [])),
                albedo=nan_to_none(soil_meta.loc[soil_id].get("SALB")),  # as in the Maricopa flow
                horizons=horizons(sheets, soil_id, notes),
                initial_moisture=[
                    (float(r["ICTL"]), float(r["ICBL"]), float(r["ICH2O"]))
                    for r in sorted(rows[tid]["initial_condition_layers"], key=lambda r: r["ICTL"])
                ],
                initial_n=[
                    (float(r["ICTL"]), float(r["ICBL"]), nan_to_none(r.get("ICNO3M")), nan_to_none(r.get("ICNH4M")))
                    for r in sorted(rows[tid]["initial_condition_layers"], key=lambda r: r["ICTL"])
                ],
                sowing=to_date(one(tid, "Planting_events")["PDATE"]),
                harvest=to_date(one(tid, "Harvest_events")["HADAT"]),
                fertilizations=[
                    f for r in rows[tid].get("Fertilizer_events", []) if (f := fertilization(r)) is not None
                ],
                # IRADP > 0: buried drip lines (23 cm), the water goes into the layer at that depth
                irrigations=[(to_date(r["IDATE"]), float(r["IRVAL"]), float(r["IRADP"]) / 100) for r in irrigations],
                cultivar=CULTIVAR,
                no_of_layers=NO_OF_LAYERS,
                obs=Observations(
                    et=pd.Series(dtype=float),
                    emergence=to_date(summary.get("PLDAE")),
                    anthesis=to_date(summary.get("ADAT")),
                    maturity=to_date(summary.get("MDAT")),
                    lai_max=nan_to_none(summary.get("LAIX")),
                    grain_yield=nan_to_none(summary.get("GWAM")),
                ),
                notes=notes,
            )
        )
    return setups
