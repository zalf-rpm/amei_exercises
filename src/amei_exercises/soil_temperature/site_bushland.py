"""Bushland (Texas, USA), soil temperature study: the ICASA workbook in "Bushland/" -> Setups.

Same experiment as the ET study's Bushland (amei_exercises.evapotranspiration.site_bushland), but its own,
newer workbook: earlier SDATs (the fertilization), initial N per layer, a 240 cm profile. See
FINDINGS.md for what had to be decided.
"""

from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Any

import pandas as pd

from amei_exercises.soil_temperature.st_monica import (
    ET_DIR,
    ST_DIR,
    Fertilization,
    Observations,
    Setup,
    horizons,
    nan_to_none,
    read_sheets,
    soil_temperature_params,
    to_date,
    treatment_id_resolver,
    weather,
)

logger = logging.getLogger("site_bushland")

NAME = "Bushland"
TITLE = "Bushland, TX"  # the summary template's first line says "Maricopa Wheat FACE"
DATA_DIR = ST_DIR / "Bushland"
XLSX = DATA_DIR / "BUSHLAND_data_2026-10-03(ICASA data format v4.1)(PM1)(TBF1)(noSoilT).xlsx"
TEMPLATES = {
    "summary": DATA_DIR / "AMEI_template_Bushland_summary_output.txt",
    "daily": DATA_DIR / "AMEI_template_Bushland_daily_output.txt",
    "layers": DATA_DIR / "AMEI_template_Bushland_layers_output.txt",
}
# the ET study's parameters and its calibrated cultivar (evapotranspiration/FINDINGS.md, "Calibration")
PARAMS = ET_DIR / "params"
CULTIVAR = "crops/cultivar_winter-wheat_bushland_2_growth.json"
NO_OF_LAYERS = 48  # 2.4 m in 5 cm layers: the protocol's "48 layers ... 0 to 2.40 m", the whole profile
NITROGEN_STRESS = True  # as in the ET study's runs with the calibrated cultivar
# Odin's fixes for standing water (evapotranspiration/FINDINGS.md, model issues 4 and 5), as in the ET study
SOIL_MOISTURE = {"PondEvaporationByCoverage": True, "InfiltrationByDepth": True}
SETUP_SHEETS = (
    "Plots",
    "Residue",
    "initial_condition_layers",
    "Planting_events",
    "Harvest_events",
    "Fertilizer_events",
    "Irrigation_events",
    "Obs_crop_summary_means",
)


def fertilization(f: dict[str, Any]) -> Fertilization | None:
    kg_n = float(f["FEAMN"])
    if kg_n <= 0:
        return None
    no3, nh4 = (float(nan_to_none(f.get(c)) or 0) for c in ("FENO3", "FENH4"))
    forms = no3 + nh4
    if forms <= 0:
        msg = f"{f['TREAT_ID']} {f['FEDATE']}: {f['FECD']} with {kg_n} kg N but no NO3/NH4 split"
        raise ValueError(msg)
    # anhydrous ammonia, injected at FEDEP (17.25 cm)
    depth_m = float(nan_to_none(f.get("FEDEP")) or 0) / 100
    return Fertilization(to_date(f["FEDATE"]), kg_n, nh4 / forms, no3 / forms, str(f["FECD"]), depth_m=depth_m)


def load(xlsx: Path = XLSX) -> list[Setup]:
    sheets = read_sheets(xlsx)
    treatments = {str(r["TREAT_ID"]): r for r in sheets["Treatments"].to_dict("records")}
    resolve = treatment_id_resolver(treatments)

    rows: dict[str, dict[str, list[dict[str, Any]]]] = {t: {} for t in treatments}
    for name in SETUP_SHEETS:
        for r in sheets[name].to_dict("records"):
            tid = None if pd.isna(r["TREAT_ID"]) else resolve(str(r["TREAT_ID"]).strip())
            if tid is None:
                logger.warning("%s: a row of no known treatment (%r) skipped: %s", name, r["TREAT_ID"], r)
            else:
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
        soil_id = one(tid, "Plots")["SOIL_ID"]
        fl = field.loc[t["FIELD_ID"]]
        st = station.loc[t["wst_id"]]
        start = to_date(t["SDAT"])

        ic_layers = sorted(rows[tid].get("initial_condition_layers", []), key=lambda r: float(r["ICTL"]))
        ic_dates = {to_date(r["ICDAT"]) for r in ic_layers}
        if any(d != start for d in ic_dates):
            notes.append(f"initial conditions of {sorted(ic_dates)} used at SDAT {start}")
        if any(nan_to_none(r.get("ICLST")) is not None for r in ic_layers):
            notes.append("initial soil temperatures (ICLST) not used: MONICA starts its soil temperature itself")
        residue = one(tid, "Residue")
        if float(residue["ICRAG"]) > 0 or float(residue["ICRT"]) > 0:
            notes.append(f"residue not simulated: {residue['ICRAG']} kg/ha, roots {residue['ICRT']} kg/ha")

        summary = rows[tid].get("Obs_crop_summary_means", [{}])[0]
        setups.append(
            Setup(
                id=tid,
                start=start,
                end=to_date(t["ENDAT"]),
                latitude=float(fl["FL_LAT"]),
                elevation=float(fl["FLELE"]),
                slope=math.tan(math.radians(float(fl["FLSL"]))),  # degree -> m m-1
                weather=weather(sheets, t["WST_DATASET"]),
                wind_height=float(st["WNDHT"]),
                co2=nan_to_none(st.get("CO2Y")),
                tav=float(st["TAV"]),
                tamp=float(st["TAMP"]),
                # MONICA's own soil temperature: its lower boundary 9.5 °C or TAV (st_monica.BASE_TEMPERATURE_FROM_TAV)
                site_params={
                    **soil_temperature_params(float(st["TAV"])),
                    "SoilMoistureParameters": dict(SOIL_MOISTURE),
                },
                albedo=nan_to_none(soil_meta.loc[soil_id].get("SALB")),
                horizons=horizons(sheets, soil_id, notes),
                initial_moisture=[(float(r["ICTL"]), float(r["ICBL"]), float(r["ICH2O"])) for r in ic_layers],
                initial_n=[
                    (float(r["ICTL"]), float(r["ICBL"]), nan_to_none(r.get("ICNO3M")), nan_to_none(r.get("ICNH4M")))
                    for r in ic_layers
                ],
                sowing=to_date(one(tid, "Planting_events")["PDATE"]),
                harvest=to_date(one(tid, "Harvest_events")["HADAT"]),
                fertilizations=[
                    f for r in rows[tid].get("Fertilizer_events", []) if (f := fertilization(r)) is not None
                ],
                # sprinkler, at the surface
                irrigations=[
                    (to_date(i["IDATE"]), float(i["IRVAL"]))
                    for i in rows[tid].get("Irrigation_events", [])
                    if float(i["IRVAL"]) > 0
                ],
                cultivar=CULTIVAR,
                no_of_layers=NO_OF_LAYERS,
                obs=Observations(
                    et=pd.Series(dtype=float),
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
