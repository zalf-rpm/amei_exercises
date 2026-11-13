"""Avignon (France): AgMIP_Wheat_Avignon_full_vPublish.xlsx -> Setups.

Older AgMIP layout: one header row (codes), sheets keyed by EXPTRNO, no Treatments sheet
(Metadata instead), no simulation period and no harvest dates. See FINDINGS.md for what had to be
assumed.
"""

from __future__ import annotations

import logging
from datetime import date
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
    sat_vapor_pressure,
    to_date,
    wind_at_2m,
)

logger = logging.getLogger("site_avignon")

NAME = "Avignon"
XLSX = HERE / "AgMIP_Wheat_Avignon_full_vPublish.xlsx"
NO_OF_LAYERS = 38  # 1.9 m, the depth of the soil profile
CULTIVAR = "crops/cultivar_winter-wheat.json"  # durum wheat in reality; MONICA has no durum cultivar here
LATEST_HARVEST = (7, 15)  # month, day in the harvest year: no harvest dates in the data
FERTILIZERS = {"FE001": ("Ammonium nitrate", 0.5, 0.5)}  # code -> (name, NH4 share, NO3 share)


def read_sheets(xlsx: Path) -> dict[str, pd.DataFrame]:
    sheets = pd.read_excel(xlsx, sheet_name=None, header=0)
    return {name: df.dropna(how="all").dropna(axis=1, how="all") for name, df in sheets.items()}


def weather(sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """MONICA's weather from the Avignon sheet.

    - TAVD isn't given: (TMAX + TMIN) / 2.
    - VPRSD is, despite its name and the Definitions sheet, the vapor pressure *deficit*
      (correlation 0.98 with es - ea(TDEW), ~0 on days with 90-100 % RH). So ea comes from the
      dew point (FAO56's first choice), and the mean RH MONICA wants from ea / es.
    - RHUMD is the *minimum* RH here, not the mean as in Bushland, so it isn't used.
    - The wind was measured at heights between 2 and 4 m that change over the season (WINDHT);
      it's brought to 2 m per day (FAO56 eq. 47).
    """
    w = sheets["Weather_daily"]
    ea = sat_vapor_pressure(w["TDEW"])
    es = (sat_vapor_pressure(w["TMAX"]) + sat_vapor_pressure(w["TMIN"])) / 2
    return pd.DataFrame(
        {
            "tmin": w["TMIN"].to_numpy(),
            "tavg": ((w["TMAX"] + w["TMIN"]) / 2).to_numpy(),
            "tmax": w["TMAX"].to_numpy(),
            "precip": w["RAIN"].to_numpy(),
            "globrad": w["SRAD"].to_numpy(),  # MJ m-2 d-1
            "wind": wind_at_2m(w["WIND"] / 86.4, w["WINDHT"]).to_numpy(),  # km d-1 at WINDHT -> m s-1 at 2 m
            "relhumid": (100 * ea / es).clip(upper=100).to_numpy(),
            "vaporpress": ea.to_numpy(),  # kPa
        },
        index=pd.to_datetime(w["W_DATE"]).dt.date,
    )[WEATHER_COLUMNS]


def horizons(sheets: dict[str, pd.DataFrame]) -> list[Horizon]:
    """There's only one profile; its SOIL_ID ('calcaric fluvisol') isn't the one Metadata refers to (FRAV)."""
    hs = sheets["Soil_layers"].sort_values("SLLB").to_dict("records")
    result, top = [], 0.0
    for h in hs:
        result.append(
            Horizon(
                top_cm=top,
                bottom_cm=float(h["SLLB"]),
                sand=float(h["SLSND"]),
                clay=float(h["SLCLY"]),
                soc=float(h["SLOC"]),
                bulk_density=float(h["SLBDM"]),
                fc=float(h["SLDUL"]),
                pwp=float(h["SLLL"]),
                sat=float(h["SLSAT"]),
                ph=nan_to_none(h.get("SLHW")),
                stones=nan_to_none(h.get("SLCF")),
                nitrate_kg_ha=nan_to_none(h.get("NOSLD_kgperha")),
                ammonium_kg_ha=nan_to_none(h.get("NHSLD_kgperha")),
            )
        )
        top = float(h["SLLB"])
    return result


def by_trno(df: pd.DataFrame, trno: int, col: str = "EXPTRNO") -> list[dict[str, Any]]:
    return df[df[col] == trno].to_dict("records")


def series(rows: list[dict[str, Any]], *cols: str) -> pd.Series:
    """The first non-missing of cols per day, e.g. the 100 % quality value, else the 90 % one."""
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    values = df[list(cols)].bfill(axis=1).iloc[:, 0].astype(float)
    return pd.Series(values.to_numpy(), index=pd.to_datetime(df["DATE"]).dt.date).dropna()


def phenology_from_bbch(
    bbch: list[dict[str, Any]], wth: pd.DataFrame, notes: list[str]
) -> tuple[date | None, date | None]:
    """Anthesis and maturity estimated from the BBCH observations, as the Comments sheet suggests.

    Anthesis: 4 days before BBCH 69, or 150 °C d (base 0, daily mean) after BBCH 55/58.
    Maturity: BBCH 89 (fully ripe), else 87 (hard dough).
    """
    stages = {int(r["BBCH"]): to_date(r["DATE"]) for r in bbch}
    anthesis = maturity = None
    if 69 in stages:
        anthesis = stages[69] - pd.Timedelta(days=4)
        notes.append(f"anthesis {anthesis} estimated: 4 days before BBCH 69")
    elif (start := stages.get(58) or stages.get(55)) is not None:
        degree_days = wth.loc[start:, "tavg"].clip(lower=0).cumsum()
        anthesis = degree_days[degree_days >= 150].index[0]
        notes.append(f"anthesis {anthesis} estimated: 150 °C d after BBCH {58 if 58 in stages else 55} ({start})")
    if (m := stages.get(89) or stages.get(87)) is not None:
        maturity = m
        notes.append(f"maturity {maturity} from BBCH {89 if 89 in stages else 87}")
    return anthesis, maturity


def load(xlsx: Path = XLSX) -> list[Setup]:
    sheets = read_sheets(xlsx)
    wth = weather(sheets)
    profile = horizons(sheets)
    setups = []
    for m in sheets["Metadata"].to_dict("records"):
        trno = int(m["EXPTRNO"])
        tid = str(m["EXP_TRT_NAME"])
        notes = [f"EXPTRNO {trno}"]
        planting = by_trno(sheets["Plantings"], trno)[0]
        sowing = to_date(planting["PDATE"])

        ic = sorted(by_trno(sheets["Init_conditions_soil_layers"], trno), key=lambda r: r["ICBL"])
        initial_moisture, top = [], 0.0
        for r in ic:
            initial_moisture.append((top, float(r["ICBL"]), float(r["ICH2O"])))
            top = float(r["ICBL"])
        ic_dates = {to_date(r["ICDAT"]) for r in ic}
        start = min(ic_dates | {sowing})
        harvest_year = int(m["EXP_YEAR"])
        latest_harvest = date(harvest_year, *LATEST_HARVEST)

        fertilizations = []
        for f in by_trno(sheets["Fertilization"], trno):
            kg_n = float(f["FEAMN"])
            if kg_n <= 0:
                continue  # the P/K applications (FE009)
            name, nh4, no3 = FERTILIZERS.get(str(f["FECD"]), (str(f["FECD"]), 0.5, 0.5))
            fertilizations.append(Fertilization(to_date(f["FEDATE"]), kg_n, nh4, no3, name))

        et_rows = by_trno(sheets["ET_daily"], trno, "TRNO")
        summary = (by_trno(sheets["Obs_summary"], trno) or [{}])[0]
        anthesis, maturity = to_date(summary.get("ADAT")), to_date(summary.get("MDAT"))
        if anthesis is None or maturity is None:
            est_anthesis, est_maturity = phenology_from_bbch(by_trno(sheets["BBCH_Obs"], trno), wth, notes)
            anthesis, maturity = anthesis or est_anthesis, maturity or est_maturity
        setups.append(
            Setup(
                id=tid,
                start=start,
                end=latest_harvest,
                latitude=float(m["FL_LAT"]),
                elevation=float(m["FLELE"]),
                weather=wth,
                wind_height=2.0,  # converted in weather()
                horizons=profile,
                initial_moisture=initial_moisture,
                sowing=sowing,
                latest_harvest=latest_harvest,
                fertilizations=fertilizations,
                irrigations=[(to_date(i["IDATE"]), float(i["IRVAL"])) for i in by_trno(sheets["Irrigations"], trno)],
                cultivar=CULTIVAR,
                no_of_layers=NO_OF_LAYERS,
                obs=Observations(
                    et=series(et_rows, "ETDAILY_100", "ETDAILY_90"),
                    et_daytime=series(et_rows, "ETDAY_100", "ETDAY_90"),
                    emergence=to_date(summary.get("PLDAE")),
                    anthesis=anthesis,
                    maturity=maturity,
                    lai_max=nan_to_none(summary.get("LAIX")),
                    grain_yield=nan_to_none(summary.get("HWAH_machine")),  # recommended in the comments
                ),
                notes=notes,
            )
        )
    return setups
