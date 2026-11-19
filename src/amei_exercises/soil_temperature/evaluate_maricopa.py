"""Run all 16 Maricopa treatments with a cultivar (and species) and compare them with the observations.

The comparison behind soil_temperature/FINDINGS.md, "Maricopa calibration" (calibrated parameters vs
the Maricopa flows'), in the soil temperature study's setup (site_maricopa.py), harvest as observed:

    pixi run evaluate-maricopa --cultivar crops/cultivar_spring-wheat_maricopa_3_yield.json \\
        --species crops/species_wheat_maricopa_2_growth.json --out /tmp/maricopa_eval

--cultivar/--species: an include path relative to the site's PARAMS, or a JSON file. Prints per
treatment the anthesis/maturity error, the LAI and biomass RMSE and the final values, then the
overall errors and the responses (FACE/ambient, dry/wet, low/high N); writes <out>/eval.csv.
"""

from __future__ import annotations

import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

from amei_exercises.et_monica import DEFAULT_MONICA_RUN, Options, collect, number, prepare, run_monica
from amei_exercises.icasa import read_sheets
from amei_exercises.soil_temperature import site_maricopa

CONTROLS = ["902", "906", "910", "914"]  # wet, high N, ambient CO2: the calibration treatments
RESPONSES = {
    "FACE/Amb": [("903", "901"), ("904", "902"), ("907", "905"), ("908", "906")]
    + [("911", "909"), ("912", "910"), ("915", "913"), ("916", "914")],
    "Dry/Wet": [("901", "902"), ("903", "904"), ("905", "906"), ("907", "908")],
    "LowN/HighN": [("909", "910"), ("911", "912"), ("913", "914"), ("915", "916")],
}


def rmse(x: pd.Series | list[float]) -> float:
    x = pd.Series(x, dtype=float).dropna()
    return float(np.sqrt((x**2).mean())) if len(x) else float("nan")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cultivar", default=site_maricopa.CULTIVAR, help="default: the site's")
    ap.add_argument("--species", help="default: crop.json's")
    ap.add_argument("--out", type=Path, required=True, help="the MONICA runs and eval.csv")
    ap.add_argument("--monica-run", type=Path, default=DEFAULT_MONICA_RUN)
    ap.add_argument("--jobs", type=int, default=os.cpu_count())
    args = ap.parse_args()
    logging.basicConfig(level=logging.ERROR, format="%(levelname)-7s %(message)s")

    def resolve(include: str | None) -> str | None:  # a JSON file -> absolute, an include path as is
        return str(Path(include).resolve()) if include and Path(include).is_file() else include

    cultivar, species = resolve(args.cultivar), resolve(args.species)
    sheets = read_sheets(site_maricopa.XLSX)
    obs = {str(r["TREAT_ID"]): r for r in sheets["Obs_crop_summary_means"].to_dict("records")}
    names = {str(r["TREAT_ID"]): r["TRT_NAME"] for r in sheets["Treatments"].to_dict("records")}

    def one(s):  # noqa: ANN001, ANN202
        s.cultivar, s.species = cultivar, species
        d = args.out / s.id
        prepare(s, d, Options(params=site_maricopa.PARAMS, water_stress=True, nitrogen_stress=True))
        run_monica(args.monica_run, d)
        return s, collect(d)

    def day(v):  # noqa: ANN001, ANN202
        return pd.Timestamp(v).date() if v else None

    rows = []
    with ThreadPoolExecutor(args.jobs) as pool:
        for s, (daily, summary) in pool.map(one, site_maricopa.load()):
            sim = {r["DATE"]: r for r in daily}

            def pairs(series: pd.Series, code: str, scale: float) -> list[float]:
                return [
                    (x - v) / scale
                    for k, v in series.items()
                    if (r := sim.get(str(k))) is not None and (x := number(r.get(code))) is not None
                ]

            o = obs[s.id]
            lai = pairs(s.obs.lai, "LAID", 1)
            rows.append(
                {
                    "id": s.id,
                    "treatment": names[s.id],
                    "ADAT": (day(summary.get("ADAT")) - s.obs.anthesis).days if summary.get("ADAT") else None,
                    "MDAT": (day(summary.get("MDAT")) - s.obs.maturity).days if summary.get("MDAT") else None,
                    "LAI rmse": rmse(lai),
                    "CWAD rmse t": rmse(pairs(s.obs.biomass, "CWAD", 1000)),
                    "LAIX obs": o.get("LAIX"),
                    "CWAM obs": o.get("CWAM"),
                    "CWAM sim": number(summary.get("CWAM")),
                    "GWAM obs": o.get("GWAM"),
                    "GWAM sim": number(summary.get("GWAM")),
                    "CNAM obs": o.get("CNAM"),
                    "CNAM sim": number(summary.get("CNAM")),
                }
            )
    df = pd.DataFrame(rows).set_index("id").sort_index()
    df.to_csv(args.out / "eval.csv")
    pd.set_option("display.width", 250)
    print(df.round(2).to_string())  # noqa: T201

    y = (df["GWAM sim"] - df["GWAM obs"]) / 1000
    print(  # noqa: T201
        f"\nanthesis RMSE {rmse(df.ADAT):.1f} d, maturity RMSE {rmse(df.MDAT):.1f} d "
        f"(not reached: {df.MDAT.isna().sum()}), LAI RMSE {df['LAI rmse'].mean():.2f}, "
        f"biomass RMSE {df['CWAD rmse t'].mean():.2f} t/ha (both: mean of the treatments'), "
        f"yield RMSE {rmse(y):.2f} t/ha (controls {rmse(y[CONTROLS]):.2f})"
    )
    for k, ps in RESPONSES.items():
        for v in ("CWAM", "GWAM"):
            o = " ".join(f"{df.loc[a, f'{v} obs'] / df.loc[b, f'{v} obs']:.2f}" for a, b in ps)
            m = " ".join(f"{df.loc[a, f'{v} sim'] / df.loc[b, f'{v} sim']:.2f}" for a, b in ps)
            print(f"{k} {v}: obs {o} | sim {m}")  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
