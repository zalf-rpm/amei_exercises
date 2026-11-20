"""Maricopa (Arizona, USA), wheat FACE: the ICASA workbook in "Maricopa wheat FACE/" -> Setups.

16 treatments (4 seasons x CO2 x water or N). See FINDINGS.md for what MONICA can't represent.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from amei_exercises.soil_temperature.st_monica import (
    SOIL_WATER_DEPTHS_CM,
    ST_DIR,
    Fertilization,
    Observations,
    OrganicAddition,
    Setup,
    daily_obs,
    horizons,
    nan_to_none,
    read_sheets,
    to_date,
    weather,
)

logger = logging.getLogger("site_maricopa")

NAME = "Maricopa"
TITLE = "Maricopa Wheat FACE"
DATA_DIR = ST_DIR / "Maricopa wheat FACE"
XLSX = DATA_DIR / "MARICOPA Wheat FACE data_2026-10-15 (ICASA data format v4.1)(PM9)(BAK1)(no SoilT).xlsx"
TEMPLATES = {
    "summary": DATA_DIR / "AMIE_template_MarcicopaWheatFACE_summary_output.txt",
    "daily": DATA_DIR / "AMIE_template_MarcicopaWheatFACE_daily_output.txt",
    "layers": DATA_DIR / "AMIE_template_MarcicopaWheatFACE_layers_output.txt",
}
# the parameters of calibrate.py --site maricopa (soil_temperature/calibration/calibrate.sh, FINDINGS.md): phenology,
# water (StageKcFactor against the measured ET and soil water), growth and yield on top of it (steps 1, 4-6);
# the calibration flow's cultivar was crops/cultivar_spring-wheat_with_1_calibrated_phenology.json
PARAMS = ST_DIR / "params"
# calibrate.py --site maricopa writes to <CALIBRATION_DIR>/maricopa_<step>
CALIBRATION_DIR = ST_DIR / "calibration"
CULTIVAR = "crops/cultivar_spring-wheat_maricopa_6_yield.json"
SPECIES = "crops/species_wheat_maricopa_5_growth.json"
NO_OF_LAYERS = 42  # 2.1 m in 5 cm layers, as the protocol asks
NITROGEN_STRESS = True  # low and high N treatments
# Odin's fixes for standing water (evapotranspiration/FINDINGS.md, model issues 4 and 5): with 5 cm layers
# and flood irrigation the water would pond and evaporate in place of the crop's transpiration
SOIL_MOISTURE = {"PondEvaporationByCoverage": True, "InfiltrationByDepth": True}


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


# the previous crop's residue (sheet Residue) as MONICA's crop residue of wheat, its N content the workbook's
RESIDUE_PARAMS = "crops/residue_wheat.json"


def residue_additions(r: dict[str, Any], notes: list[str]) -> list[OrganicAddition]:
    """Above ground residue (ICRAG) and roots (ICRT) of the previous crop, kg DM/ha, into the soil on ICRDAT.

    Both evenly from the surface to ICRDP (ICRIP = 100 %: all incorporated), as MONICA's wheat residue
    with the workbook's N concentration (ICRN, %; given for the residue, used for the roots as well).
    """
    params = json.loads((PARAMS / RESIDUE_PARAMS).read_text())
    if (n := nan_to_none(r.get("ICRN"))) is not None:
        params["NConcentration"] = [n / 100, "kg N kg DM-1"]
    incorporated = nan_to_none(r.get("ICRIP")) or 0.0
    depth = float(nan_to_none(r.get("ICRDP")) or 0.0) if incorporated > 0 else 0.0
    if 0 < incorporated < 100:
        notes.append(f"residue {incorporated:g} % incorporated: all of it put to 0-{depth:g} cm")
    day = to_date(r["ICRDAT"])
    adds = [
        OrganicAddition(day, float(kg), 0.0, depth, params, name)
        for name, kg in (("residue", nan_to_none(r.get("ICRAG"))), ("roots", nan_to_none(r.get("ICRT"))))
        if kg
    ]
    if adds:
        notes.append(
            f"residue {r.get('ICRAG', 0):g} kg DM/ha ({r.get('ICPCR')}, {params['NConcentration'][0] * 100:g} % N) "
            f"+ roots {r.get('ICRT', 0):g} kg DM/ha into 0-{depth:g} cm on {day}"
        )
    return adds


# Cum_evapotranspiration_means' ETAP: 1993/94 the mean ET per period [mm/d], 1996/97 (10-day periods)
# the sum over the period [mm] (values up to 88; the definition says "averaged over a given period", mm)
ETAP_SUMS = {"AZMA1996", "AZMA1997"}


def et_periods(rows: list[dict[str, Any]]) -> list[tuple[date, date, float]]:
    """The measured ET per period as (first day, last day, mm/d)."""
    out = []
    for r in rows:
        a, b, v = to_date(r["SMDATE"]), to_date(r["EMDATE"]), nan_to_none(r.get("ETAP"))
        if a is None or b is None or v is None:
            continue
        out.append((a, b, float(v) / ((b - a).days + 1) if r["EID"] in ETAP_SUMS else float(v)))
    return out


def soil_water(rows: list[dict[str, Any]]) -> pd.Series:
    """Measured soil water [mm] in SOIL_WATER_DEPTHS_CM: neutron probe readings (SWLD at SLSD every 20 cm,
    30-210 cm), each for its depth +-10 cm; only dates with all depths of the layer."""
    top, bottom = SOIL_WATER_DEPTHS_CM
    wanted = set(range(top + 10, bottom, 20))
    by_date: dict[date, dict[int, float]] = {}
    for r in rows:
        if (v := nan_to_none(r.get("SWLD"))) is not None and int(r["SLSD"]) in wanted:
            by_date.setdefault(to_date(r["DATE"]), {})[int(r["SLSD"])] = float(v)
    mm = {d: sum(v.values()) * 200 for d, v in by_date.items() if set(v) == wanted}
    return pd.Series(mm, dtype=float).sort_index()


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

        organic_additions = residue_additions(one(tid, "Residue"), notes)
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
                site_params={
                    "SoilTemperatureParameters": {"BaseTemperature": float(st["TAV"])},
                    "SoilMoistureParameters": dict(SOIL_MOISTURE),
                },
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
                organic_additions=organic_additions,
                # IRADP > 0: buried drip lines (23 cm), the water goes into the layer at that depth
                irrigations=[(to_date(r["IDATE"]), float(r["IRVAL"]), float(r["IRADP"]) / 100) for r in irrigations],
                cultivar=CULTIVAR,
                species=SPECIES,
                no_of_layers=NO_OF_LAYERS,
                obs=Observations(
                    et=pd.Series(dtype=float),
                    emergence=to_date(summary.get("PLDAE")),
                    ear_emergence=to_date(summary.get("EEMD")),
                    anthesis=to_date(summary.get("ADAT")),
                    maturity=to_date(summary.get("MDAT")),
                    lai_max=nan_to_none(summary.get("LAIX")),
                    grain_yield=nan_to_none(summary.get("GWAM")),
                    lai=daily_obs(rows[tid].get("Obs_crop_daily_means", []), "LAID"),
                    biomass=daily_obs(rows[tid].get("Obs_crop_daily_means", []), "CWAD"),
                    green_leaf=daily_obs(rows[tid].get("Obs_crop_daily_means", []), "WLVG"),
                    et_periods=et_periods(rows[tid].get("Cum_evapotranspiration_means", [])),
                    soil_water=soil_water(rows[tid].get("Soil_moisture_means", [])),
                ),
                notes=notes,
            )
        )
    return setups
