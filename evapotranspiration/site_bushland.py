"""Bushland (Texas, USA): AMEI_Bushland_v4_2026-01-24(no_soil_T).xlsx -> Setups.

ICASA layout (icasa.py): three header rows (names, units, codes), sheets keyed by TREAT_ID.
"""

from __future__ import annotations

import itertools
import logging
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from et_monica import HERE, Fertilization, Observations, Setup, nan_to_none, to_date
from icasa import horizons, read_sheets, treatment_id_resolver, weather

logger = logging.getLogger("site_bushland")

NAME = "Bushland"
XLSX = HERE / "AMEI_Bushland_v4_2026-01-24(no_soil_T).xlsx"
# 10 cm layers: with 5 cm the infiltration limit (it scales with the layer thickness) let the
# irrigation water pond, which then evaporated at 1.1 x ET0 (FINDINGS.md, model issues 4 and 5)
LAYER_THICKNESS_M = 0.1
NO_OF_LAYERS = 24  # the whole 2.4 m profile
CULTIVAR = "crops/cultivar_winter-wheat.json"
# the treatments differ in their fertilization (1990, 1993: F about twice P), so with N response
NITROGEN_STRESS = True
# Odin's fixes for standing water (FINDINGS.md, model issues 4 and 5), in "Orig" and "Imp" alike: they
# are not about ET0/ETp
SOIL_MOISTURE = {"PondEvaporationByCoverage": True, "InfiltrationByDepth": True}


def zadoks_date(daily: list[dict[str, Any]], stage: float) -> date | None:
    """The date the observed Zadoks stage (GSTZD) reached `stage`, linearly interpolated."""
    obs = sorted((to_date(r["DATE"]), float(r["GSTZD"])) for r in daily if nan_to_none(r.get("GSTZD")) is not None)
    for (d0, z0), (d1, z1) in itertools.pairwise(obs):
        if z0 < stage <= z1:
            return d0 + timedelta(days=round((d1 - d0).days * (stage - z0) / (z1 - z0)))
    return None


def daily_obs(daily: list[dict[str, Any]], code: str) -> pd.Series:
    """The observed daily values of `code`, date index, without the missing ones."""
    obs = {to_date(r["DATE"]): float(r[code]) for r in daily if nan_to_none(r.get(code)) is not None}
    return pd.Series(obs, dtype=float).sort_index()


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
                    # anhydrous ammonia, injected at 17 cm
                    depth_m=float(nan_to_none(f.get("FEDEP")) or 0) / 100,
                )
            )

        et = pd.DataFrame(rows[tid].get("ET_Daily", []))
        summary = rows[tid].get("Obs_crop_summary_means", [{}])[0]
        icin = nan_to_none(one(tid, "initial_conditions").get("ICIN"))
        anthesis = to_date(summary.get("ADAT"))
        if anthesis is None and (anthesis := zadoks_date(rows[tid].get("Obs_crop_daily_means", []), 61)):
            notes.append(f"anthesis {anthesis} interpolated from the daily Zadoks stages (GSTZD 61)")

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
                # only the profile's total is given: put it where the organic matter is
                initial_n_by_soc=True,
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
                layer_thickness_m=LAYER_THICKNESS_M,
                site_params={"SoilMoistureParameters": dict(SOIL_MOISTURE)},
                obs=Observations(
                    et=pd.Series(et["ETAD"].astype(float).to_numpy(), index=pd.to_datetime(et["DATE"]).dt.date)
                    if len(et)
                    else pd.Series(dtype=float),
                    emergence=to_date(summary.get("PLDAE")),
                    ear_emergence=to_date(summary.get("EEMD")),
                    anthesis=anthesis,
                    maturity=to_date(summary.get("MDAT")),
                    lai_max=nan_to_none(summary.get("LAIX")),
                    grain_yield=nan_to_none(summary.get("HWAM")),
                    lai=daily_obs(rows[tid].get("Obs_crop_daily_means", []), "LAID"),
                    biomass=daily_obs(rows[tid].get("Obs_crop_daily_means", []), "CWAD"),
                ),
                notes=notes,
            )
        )
    return setups
