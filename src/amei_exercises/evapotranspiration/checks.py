"""Reproducible checks behind statements in FINDINGS.md.

    pixi run et-checks humidity   # what RHUMD/RHAVD and VPRSD really are, per site
    pixi run et-checks et0        # MONICA's ET0: crop vs no crop, vapor pressure in hPa
    pixi run et-checks            # both

humidity: each RH column is compared with what it would be under each reading, from the dew point
(ea = ea(TDEW)): minimum ≈ ea / es(Tmax), mean ≈ ea / es_mean, maximum ≈ ea / es(Tmin). VPRSD is
compared with ea(TDEW), with ea from the RH min/max (FAO56 eq. 17, independent of TDEW) and with the
deficit es_mean - ea. On days that are saturated all day (RHMXD = 100, RHUMD >= 95) a vapor pressure
must be about es; a deficit about 0.

et0: one treatment run three times with run_et.py's inputs: as is, without a crop (so ET0 comes from
the soil moisture module's FAO56-PM all season), and with the vapor pressure ×10 (as if hPa were
written where MONICA expects kPa).
"""

from __future__ import annotations

import argparse
import importlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from amei_exercises.et_monica import DEFAULT_MONICA_RUN, Options, prepare, read_sections, run_monica, sat_vapor_pressure
from amei_exercises.paths import ET_DIR, MARICOPA_DIR

WORKBOOKS = {
    "Bushland": (ET_DIR / "AMEI_Bushland_v4_2026-01-24(no_soil_T).xlsx", 2),
    "Avignon": (ET_DIR / "AgMIP_Wheat_Avignon_full_vPublish.xlsx", 0),
    "Maricopa": (
        MARICOPA_DIR / "MARICOPA Wheat FACE data_2026-10-15 (ICASA data format v4.1)(PM9)(BAK1)(no SoilT).xlsx",
        2,
    ),
}


def compare(x: pd.Series, candidates: dict[str, pd.Series], digits: int) -> str:
    return "\n".join(
        f"    vs {name:<44} mean {v.mean():8.{digits}f}  MAE {(x - v).abs().mean():7.{digits}f}  corr {np.corrcoef(x, v)[0, 1]:6.3f}"
        for name, v in candidates.items()
    )


def humidity() -> None:
    for site, (xlsx, header_row) in WORKBOOKS.items():
        if not xlsx.exists():
            print(f"== {site}: {xlsx} not found, skipped")  # noqa: T201
            continue
        w = pd.read_excel(xlsx, sheet_name="Weather_daily", header=header_row).dropna(how="all")
        ea = sat_vapor_pressure(w["TDEW"])
        es_min, es_max = sat_vapor_pressure(w["TMIN"]), sat_vapor_pressure(w["TMAX"])
        es_mean = (es_min + es_max) / 2
        lines = [f"== {site} ({len(w)} days)"]
        rh_candidates = {
            "RH min = ea(TDEW) / es(Tmax)": 100 * ea / es_max,
            "RH mean = ea(TDEW) / es_mean": 100 * ea / es_mean,
            "RH max = ea(TDEW) / es(Tmin), <= 100": (100 * ea / es_min).clip(upper=100),
        }
        for col in [c for c in w.columns if str(c).startswith("RH")]:
            x = w[col].astype(float)
            lines += [f"  {col}: mean {x.mean():.1f} %", compare(x, rh_candidates, 1)]
        vp = w["VPRSD"].astype(float)
        vp_candidates = {"vapor pressure ea(TDEW)": ea, "deficit es_mean - ea(TDEW)": es_mean - ea}
        if "RHUMD" in w and "RHMXD" in w:
            vp_candidates["ea from RHUMD as min, RHMXD (FAO56 eq. 17)"] = (
                es_min * w["RHMXD"] / 100 + es_max * w["RHUMD"] / 100
            ) / 2
            vp_candidates["ea from RHUMD as mean (RHUMD * es_mean)"] = w["RHUMD"] / 100 * es_mean
        lines += [f"  VPRSD: mean {vp.mean():.3f} kPa", compare(vp, vp_candidates, 3)]
        if "RHUMD" in w and "RHMXD" in w:
            sat = (w["RHMXD"] >= 100) & (w["RHUMD"] >= 95)
            if sat.any():
                lines.append(
                    f"  saturated all day (RHMXD = 100, RHUMD >= 95): {int(sat.sum())} days, VPRSD mean "
                    f"{vp[sat].mean():.3f}, es_mean {es_mean[sat].mean():.3f}, ea(TDEW) {ea[sat].mean():.3f} kPa"
                )
        print("\n".join(lines) + "\n")  # noqa: T201


def daily_et0(run_dir: Path, monica_run: Path) -> pd.DataFrame:
    run_monica(monica_run, run_dir)
    daily = pd.DataFrame(read_sections(run_dir / "monica_out.csv")[0])
    return daily.set_index("DATE")[["ETO", "Stage"]].astype(float)


def et0(site_name: str, treatment: str, monica_run: Path, params: Path) -> None:
    site = importlib.import_module(f"amei_exercises.evapotranspiration.site_{site_name}")
    setup = next(s for s in site.load() if s.id == treatment)
    tmp = Path(tempfile.mkdtemp(prefix="et0_check_"))
    try:
        runs = {}
        for name, vp_factor in (("crop", 1.0), ("no crop", 1.0), ("vapor pressure x10", 10.0)):
            d = tmp / name.replace(" ", "_")
            prepare(setup, d, Options(params=params, vp_factor=vp_factor))
            if name == "no crop":
                crop = json.loads((d / "crop.json").read_text())
                ws = crop["cropRotation"][0]["worksteps"]
                crop["cropRotation"][0]["worksteps"] = [
                    w for w in ws if w["type"] not in ("Sowing", "Harvest", "AutomaticHarvest")
                ]
                (d / "crop.json").write_text(json.dumps(crop))
            runs[name] = daily_et0(d, monica_run)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    crop = runs["crop"]
    # before the observed emergence: no crop yet, so all three runs use the soil moisture module's ET0
    pre = [d for d in crop.index if d < str(setup.obs.emergence or setup.sowing)]
    print(f"== ET0 of {treatment} [mm], {crop.index[0]}..{crop.index[-1]}")  # noqa: T201
    for name, df in runs.items():
        season, before = df["ETO"].sum(), df["ETO"].loc[pre].sum()
        print(f"  {name:<20} season {season:7.1f}   before emergence ({len(pre)} days) {before:6.1f}")  # noqa: T201
    diff = (runs["no crop"]["ETO"] - crop["ETO"]).abs()
    print(f"  crop vs no crop: largest daily difference {diff.max():.2f} mm on {diff.idxmax()}")  # noqa: T201


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("check", nargs="?", choices=("humidity", "et0", "all"), default="all")
    ap.add_argument("--site", default="bushland", choices=("bushland", "avignon"), help="for et0")
    ap.add_argument("--treatment", default="TXBL1990F", help="for et0")
    ap.add_argument("--monica-run", type=Path, default=DEFAULT_MONICA_RUN, help="default: the Odin port")
    ap.add_argument("--params", type=Path, default=ET_DIR / "phase_1/params")
    args = ap.parse_args()
    if args.check in ("humidity", "all"):
        humidity()
    if args.check in ("et0", "all"):
        et0(args.site, args.treatment, args.monica_run, args.params)
    return 0


if __name__ == "__main__":
    sys.exit(main())
