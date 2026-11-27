"""ET study phase 1: the sensitivity analysis of MONICA's ET0 (protocol steps 6 and 7).

27 treatments (ALPHA x ALBEDO x lambda, `Treatments.xlsx`) at 7 sites, 1991-2020 (NASA POWER),
MONICA's FAO-56 Penman-Monteith (MOFAPM):

    pixi run et-sensitivity --weather standardized [--sites USGA ...] [--out DIR]
    pixi run et-sensitivity --weather actual [--sites USGA ...] [--out DIR]

standardized (step 6, `WeatherData_standardized/`): the intermediates come from the weather file
(NETRAD, VPRSD, VPRSAT, SLOPE) and the treatment (lambda, PSYCON), so no MONICA run: `monica-module`
runs the ET0 equation alone, twice on the same input -
  - ETstudyOSAstand...: MONICA's own code (Monica_Internal_Reference_Evapotranspiration, MONICA's
    fao56_penman_monteith as a module),
  - ETstudyCSAstand...: the Crop2ML package Monica_Reference_Evapotranspiration.
actual (step 7, `WeatherData_actual/`): MONICA without a crop calculates the intermediates itself
(net radiation with the treatment's ALBEDO as its reference albedo, vapour pressures from RH2M, slope,
the psychrometric constant from the station's elevation and the treatment's lambda - not the
treatment's PSYCON, which is CAQC's, 332 m, for all sites), with the treatment's lambda; the Crop2ML
package runs alongside as a driver.
  - ETstudySAactual...: MONICA's own ET0 (soil moisture's, the FAO stomata resistance 100 s m-1).

MONICA's FAO-56 PM has no ALPHA (Priestley-Taylor), so the three ALPHA levels are identical; ALBEDO
acts only through the net radiation, i.e. only with actual weather. PHTD and LAID aren't used (the
reference grass). The template's ETOS, ETRS and ETP: NA (MONICA has only the FAO-56 grass reference).

Needs the monica_odin branch amei-et-sensitivity (lambda and PSYCON as inputs, the internal module,
the Crop2ML package with lambda, slope and PSYCON). Writes <out>/results/*.txt (gitignored, see
checksums.txt) and <out>/summary.csv (mean annual ET0 per site and treatment).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd

from amei_exercises.et_monica import (
    DEFAULT_MONICA_RUN,
    dump,
    monica_build,
    read_sections,
    run_monica,
    write_checksums,
    write_result,
)
from amei_exercises.paths import ET_DIR, ST_DIR
from amei_exercises.soil_temperature.run_st import modeler_name

logger = logging.getLogger("et_sensitivity")

MODULE_CODE = "MOFAPM"
TEMPLATE = ET_DIR / "3_Templates/Sensitivity_analysis/AMEI_ET_sensitivity_template_Daily.txt"
TREATMENTS = ET_DIR / "Treatments.xlsx"
WEATHER = {"standardized": ET_DIR / "WeatherData_standardized", "actual": ET_DIR / "WeatherData_actual"}
# the sites of the soil temperature sensitivity analysis: latitude (and TAV, TAMP)
STATIONS = ST_DIR / "sensitivity_analysis/input_data/WeatherMetadata_corr.csv"
# NASA POWER's grid elevation at the stations' coordinates [m] (its API, 2026-11-27); what the
# standardized NETRAD was calculated with, as far as it tells (fitted: COCA 790, USMA 430 m)
ELEVATION = {"USGA": 25.35, "DEMU": 54.17, "COCA": 793.56, "FRLU": 127.97, "USMA": 433.97, "FRMO": 86.14,
             "CAQC": 332.2}  # fmt: skip
WIND_HEIGHT = 2.0  # [m], NASA POWER's WS2M
STOMATA_RESISTANCE = 100.0  # [s m-1], MONICA's FAO default for the reference grass (soil_moisture.odin)

INTERNAL = "Monica_Internal_Reference_Evapotranspiration"
CROP2ML = "Monica_Reference_Evapotranspiration"
DRIVER_PATH = "modules.evapotranspiration_drivers.{}.output.reference_evapotranspiration"


def treatments() -> pd.DataFrame:
    """Treatments.xlsx: SM (<site>_<TREAT_ID>), TREAT_ID, WST_ID, ALBEDO, ALPHA, lambda, PSYCON."""
    raw = pd.read_excel(TREATMENTS, header=None)
    codes = raw.iloc[2].tolist()  # names, units, codes
    t = raw.iloc[3:].set_axis(codes, axis=1).dropna(subset=["SM"]).reset_index(drop=True)
    for c in ("ALBEDO", "ALPHA", "lambda", "PSYCON"):
        t[c] = t[c].astype(float)
    return t


def stations() -> dict[str, dict[str, float]]:
    s = pd.read_csv(STATIONS, sep="\t", skiprows=[0, 1])
    return {
        r.WST_ID: {"latitude": r.XLAT, "elevation": ELEVATION[r.WST_ID], "tav": r.TAV, "tamp": r.TAMP}
        for r in s.itertuples()
    }


def read_weather(kind: str, site: str) -> pd.DataFrame:
    w = pd.read_csv(WEATHER[kind] / f"{site}.WTH", sep="\t")
    w["date"] = pd.to_datetime(w["Date"].astype(str), format="%Y%j")
    w["wind"] = w["WIND"] / 86.4  # km d-1 -> m s-1
    return w


def odin_build_dir(monica_run: Path) -> Path:
    return monica_run.resolve().parent


def run_module(monica_module: Path, impl: str, input_json: Path, out_csv: Path) -> pd.Series:
    res = subprocess.run(
        [str(monica_module), "run", "Evapotranspiration", impl, str(input_json), str(out_csv)],
        capture_output=True, text=True, check=False,
    )  # fmt: skip
    if res.returncode != 0:
        msg = f"monica-module {impl} {input_json} failed: {res.stderr[-600:]}"
        raise RuntimeError(msg)
    return pd.read_csv(out_csv, index_col="date", parse_dates=True)["reference_evapotranspiration"]


def standardized(site: str, st: dict[str, float], trts: pd.DataFrame, runs: Path, monica_module: Path) -> dict:
    """monica-module with both modules, one run per lambda level: {(kind, TREAT_ID): ET0 series}."""
    w = read_weather("standardized", site)
    out = {}
    for (lam, psy), group in trts.groupby(["lambda", "PSYCON"]):
        run_dir = runs / f"{site}_{group.TREAT_ID.iloc[0].split('_')[-1]}"
        run_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "date": w["date"].dt.strftime("%Y-%m-%d"),
            "tavg": w["TAVD"],
            "wind": w["wind"],
            "wind_speed_height": WIND_HEIGHT,
            "vapor_pressure": w["VPRSD"],
            "saturated_vapor_pressure": w["VPRSAT"],
            "stomata_resistance": STOMATA_RESISTANCE,
            "net_radiation": w["NETRAD"],
            "saturated_vapor_pressure_slope": w["SLOPE"],
            "psychrometric_constant": psy,
        }).to_csv(run_dir / "daily.csv", index=False)  # fmt: skip
        (run_dir / "input.json").write_text(
            dump({"HeightNN": st["elevation"], "LatentHeatOfVaporization": lam, "Daily": "daily.csv"})
        )
        for impl, kind in ((INTERNAL, "OSAstand"), (CROP2ML, "CSAstand")):
            et0 = run_module(monica_module, impl, run_dir / "input.json", run_dir / f"{impl}.csv")
            for tid in group.TREAT_ID:
                out[(kind, tid)] = et0
    return out


def actual(site: str, st: dict[str, float], trts: pd.DataFrame, runs: Path, monica_run: Path, build: str) -> dict:
    """MONICA without a crop, one run per ALBEDO x lambda: {(kind, TREAT_ID): ET0 series}; kind
    SAactual (MONICA's own) and, for the check, the two drivers'."""
    w = read_weather("actual", site)
    params = ET_DIR / "params"
    climate = runs / site / "climate.csv"
    climate.parent.mkdir(parents=True, exist_ok=True)
    with climate.open("w") as f:
        f.write("iso-date;tmin;tavg;tmax;precip;globrad;wind;relhumid\n;C_deg;C_deg;C_deg;mm;MJ m-2;m/s;%\n")
        for r in w.itertuples():
            f.write(f"{r.date:%Y-%m-%d};{r.TMIN};{r.TAVD};{r.TMAX};0;{r.SRAD};{r.wind:.6f};{r.RH2M}\n")
    crop_params = json.loads((params / "general/crop.json").read_text())
    site_base = json.loads((ET_DIR / "site.json").read_text())
    sim_base = json.loads((ET_DIR / "sim_full_run.json").read_text())

    out = {}
    for (albedo, lam), group in trts.groupby(["ALBEDO", "lambda"]):
        alb_lam = "_".join(group.TREAT_ID.iloc[0].split("_")[1:])
        run_dir = runs / f"{site}_{alb_lam}"
        run_dir.mkdir(parents=True, exist_ok=True)
        crop = {
            "CropParameters": crop_params
            # no PsychrometricConstant: MONICA calculates it (from HeightNN and lambda)
            | {"ReferenceAlbedo": albedo, "LatentHeatOfVaporization": lam},
            "cropRotation": [{"worksteps": []}],
        }
        site_json = json.loads(json.dumps(site_base))
        sp = site_json["SiteParameters"]
        sp |= {
            "Latitude": st["latitude"], "HeightNN": [st["elevation"], "m"], "Slope": 0, "NumberOfLayers": 20,
            "LayerThickness": [0.1, "m"],
            # any soil: the ET0 doesn't depend on it
            "SoilProfileParameters": [{
                "Thickness": [2.0, "m"], "Sand": [0.4, "kg kg-1"], "Clay": [0.2, "kg kg-1"],
                "SoilOrganicCarbon": [1.0, "mass%"], "SoilBulkDensity": [1400, "kg m-3"],
                "FieldCapacity": [0.3, "m3 m-3"], "PermanentWiltingPoint": [0.12, "m3 m-3"],
                "PoreVolume": [0.45, "m3 m-3"], "pH": 7, "CN": 10,
            }],
        }  # fmt: skip
        env = json.loads((params / site_json["EnvironmentParameters"][1]).read_text())
        site_json["EnvironmentParameters"] = env | {"WindSpeedHeight": WIND_HEIGHT}
        sim = {k: v for k, v in sim_base.items() if k != "output"} | {
            "crop.json": "crop.json",
            "site.json": "site.json",
            "climate.csv": str(climate.resolve()),
            "climate.csv-options": {
                "no-of-climate-file-header-lines": 2, "lineNoOfHeaderLine": 1, "csv-separator": ";",
                "header-to-acd-names": {}, "tav": st["tav"], "tamp": st["tamp"],
            },
            "include-file-base-path": f"{params.resolve()}/",
            "path-to-output": "./",
            "NumberOfLayers": 20,
            "LayerThickness": [0.1, "m"],
            "Modules": {"EvapotranspirationDrivers": [CROP2ML, INTERNAL]},
            "output": {
                "write-file?": False,
                "csv-options": {"include-header-row": True, "include-units-row": False, "csv-separator": ","},
                "obj-outputs?": True,
                "events": ["daily", [
                    # "ET0" is rounded to 0.1 mm
                    "Date|DATE",
                    {"path": "soil_moisture.reference_evapotranspiration", "name": "ETO", "round": 8},
                    {"path": DRIVER_PATH.format(CROP2ML), "name": "ETO_CROP2ML", "round": 8},
                    {"path": DRIVER_PATH.format(INTERNAL), "name": "ETO_INTERNAL", "round": 8},
                ]],
            },
        }  # fmt: skip
        (run_dir / "crop.json").write_text(dump(crop))
        (run_dir / "site.json").write_text(dump(site_json))
        (run_dir / "sim.json").write_text(dump(sim))
        run_monica(monica_run, run_dir, build)
        daily = pd.DataFrame(read_sections(run_dir / "monica_out.csv")[0])
        daily.index = pd.to_datetime(daily.pop("DATE"))
        daily = daily.astype(float)
        for tid in group.TREAT_ID:
            out[("SAactual", tid)] = daily["ETO"]
            out[("check_crop2ml", tid)] = daily["ETO_CROP2ML"]
            out[("check_internal", tid)] = daily["ETO_INTERNAL"]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--weather", choices=WEATHER, required=True)
    ap.add_argument("--sites", nargs="*", help="default: all 7")
    ap.add_argument("--monica-run", type=Path, default=DEFAULT_MONICA_RUN, help="its build dir has monica-module")
    ap.add_argument("--out", type=Path, help="default: evapotranspiration/out_sensitivity_<weather>")
    ap.add_argument("--modeler", default=modeler_name())
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    out = args.out or ET_DIR / f"out_sensitivity_{args.weather}"
    results, runs = out / "results", out / "runs"
    results.mkdir(parents=True, exist_ok=True)

    build_dir = odin_build_dir(args.monica_run)
    build = monica_build(args.monica_run)
    os.environ["MONICA_MODULE_PLUGINS"] = str(build_dir / "plugins" / "amei.so")
    trts, sts = treatments(), stations()
    sites = args.sites or list(dict.fromkeys(trts.WST_ID))

    def one(site: str) -> dict:
        t = trts[trts.WST_ID == site]
        if args.weather == "standardized":
            return standardized(site, sts[site], t, runs, build_dir / "monica-module")
        return actual(site, sts[site], t, runs, args.monica_run, build)

    with ThreadPoolExecutor(max_workers=min(len(sites), os.cpu_count() or 1)) as ex:
        per_site = dict(zip(sites, ex.map(one, sites), strict=True))

    module = {"OSAstand": f"MONICA's own ET0 equation ({INTERNAL})", "CSAstand": f"Crop2ML {CROP2ML}",
              "SAactual": "MONICA's own ET0, MONICA's intermediates"}  # fmt: skip
    summary = []
    for site, series in per_site.items():
        for (kind, tid), et0 in series.items():
            if kind.startswith("check_"):
                continue
            rows = [[MODULE_CODE, f"{d:%Y-%m-%d}", v, None, None, None] for d, v in et0.items()]
            write_result(
                results / f"ETstudy{kind}{MODULE_CODE}{site}_{tid}.txt", TEMPLATE,
                f"Model:\tMONICA FAO-56 Penman-Monteith, {module[kind]} ({build})", rows,
                preamble_lines={"Modeler_name:": f"Modeler_name:\t{args.modeler}"},
            )  # fmt: skip
            annual = et0.groupby(et0.index.year).sum()
            row = {"site": site, "TREAT_ID": tid, "kind": kind, "ET0 mm/a": round(annual.mean(), 1)}
            if kind == "CSAstand":
                diff = (series[("OSAstand", tid)] - et0).abs().max()
                row["max |CSAstand - OSAstand| mm"] = float(f"{diff:.3g}")
            if kind == "SAactual":
                for check in ("crop2ml", "internal"):
                    diff = (series[(f"check_{check}", tid)] - et0).abs().max()
                    row[f"max |{check} - MONICA| mm"] = float(f"{diff:.3g}")
            summary.append(row)
    summary = pd.DataFrame(summary).sort_values(["site", "kind", "TREAT_ID"])
    summary.to_csv(out / "summary.csv", index=False)
    for c in [c for c in summary.columns if c.startswith("max |")]:
        logger.info("%s, any day: %.3g", c, summary[c].max())
    write_checksums(out, ["results/*.txt"])
    logger.info("%d result files in %s", len(list(results.glob("*.txt"))), results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
