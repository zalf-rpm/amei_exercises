"""Calibrate MONICA cultivar parameters against a site's observations, one step at a time.

    pixi run calibrate --site bushland --step phenology \\
        --params-csv evapotranspiration/calibration/bushland_1_phenology.csv [--reps 3000]

The steps (see STEPS): phenology (emergence, ear emergence, anthesis, maturity dates); growth
(daily LAI and above ground biomass, LAI in units of LAI_SCALE, biomass of BIOMASS_SCALE); yield
(grain yield at maturity); water (measured ET per period and soil water, where the site has them;
not for the ET study, where ET is what's evaluated). Later steps (LAI and
biomass, yield, water) start from the cultivar a previous step wrote (--cultivar).

The parameters come from a CSV in the format of the Maricopa flows' calibratethese_*.csv:

    name,position in array,low,high,step,optguess,minbound,maxbound,derived params

- `species.<Name>` instead of a cultivar parameter's name: the species parameter
- a cultivar parameter with a position ("1", or "0_1" for an array of arrays) and low/high: that
  element is calibrated
- a name that isn't a cultivar parameter, without a position, with low/high: a free variable,
  only used in the expressions of derived rows
- a row with "derived params": the element is set to that Python expression of the free variables,
  the calibrated elements (`<name>_<position>`) and `cultivar` (the original cultivar, as in the
  JSON file: `cultivar['StageTemperatureSum'][0][1]`)

optguess is the start value (default: the middle of low..high).

Every MONICA run of the calibration lasts until maturity (AutomaticHarvest), at the latest
EXTEND_DAYS after the observed harvest or where the weather has a gap; with the fixed harvest a late
crop would never reach maturity, and the error would be unknown. A date the model doesn't reach
counts as MISSING_PENALTY_DAYS after the last simulated day.

The calibrations done so far, in order: evapotranspiration/calibration/calibrate.sh (Bushland, Avignon) and
soil_temperature/calibration/calibrate.sh (Maricopa).

Writes to <out> (default <the site's CALIBRATION_DIR, else evapotranspiration/calibration>/<site>_<step>/):
- cultivar.json (species.json, if species parameters were calibrated): with the best values, ready
  as the next step's --cultivar/--species
- best.csv: the best values of the calibrated rows; report.csv: obs/default/calibrated per treatment
- spotpy.csv: every evaluation (spotpy's database)
"""

from __future__ import annotations

import argparse
import copy
import csv
import importlib
import json
import logging
import math
import os
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import spotpy

from amei_exercises.et_monica import (
    DEFAULT_MONICA_RUN,
    EXTRA_DAILY,
    SOIL_WATER_DEPTHS_CM,
    Options,
    Setup,
    collect,
    daily_outputs,
    dump,
    number,
    output_events,
    prepare,
    run_monica,
)
from amei_exercises.paths import ET_DIR

logger = logging.getLogger("calibrate")

SITES = {
    "bushland": "amei_exercises.evapotranspiration.site_bushland",
    "avignon": "amei_exercises.evapotranspiration.site_avignon",
    "maricopa": "amei_exercises.soil_temperature.site_maricopa",
}
EXTEND_DAYS = 60  # the runs last at most this long after the observed harvest
MISSING_PENALTY_DAYS = 30  # a date not reached: this many days after the last simulated day


# ---------------------------------------------------------------------------------------------
# the parameters
# ---------------------------------------------------------------------------------------------


@dataclass
class Row:
    name: str
    position: tuple[int, ...] | None
    low: float | None
    high: float | None
    optguess: float | None
    derived: str | None

    @property
    def key(self) -> str:
        """spotpy's parameter name and the name in expressions."""
        return self.name if self.position is None else f"{self.name}_{'_'.join(map(str, self.position))}"


def read_rows(path: Path) -> list[Row]:
    def num(v: str) -> float | None:
        return float(v) if v.strip() else None

    rows = []
    with path.open(newline="") as f:
        for r in csv.DictReader(f):
            pos = r["position in array"].strip()
            rows.append(
                Row(
                    name=r["name"].strip(),
                    position=tuple(int(i) for i in pos.split("_")) if pos else None,
                    low=num(r["low"]),
                    high=num(r["high"]),
                    optguess=num(r.get("optguess", "")),
                    derived=r.get("derived params", "").strip() or None,
                )
            )
    keys = [r.key for r in rows]
    if dups := sorted({k for k in keys if keys.count(k) > 1}):
        msg = f"{path}: rows given twice: {dups}"
        raise ValueError(msg)
    return rows


SPECIES = "species."  # the prefix of species parameters in the combined parameters


def combine(cultivar: dict[str, Any], species: dict[str, Any]) -> dict[str, Any]:
    """The cultivar and the species parameters in one dict, the latter as species.<Name>."""
    return cultivar | {SPECIES + k: v for k, v in species.items()}


def split(params: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """combine()'s inverse: (cultivar, species)."""
    return (
        {k: v for k, v in params.items() if not k.startswith(SPECIES)},
        {k.removeprefix(SPECIES): v for k, v in params.items() if k.startswith(SPECIES)},
    )


def values_of(cultivar: dict[str, Any], name: str) -> list[Any]:
    """The array of a cultivar parameter: [[...], "unit"] -> [...], [...] -> [...]."""
    v = cultivar[name]
    if isinstance(v, list) and len(v) == 2 and isinstance(v[0], list) and isinstance(v[1], str):
        return v[0]
    if isinstance(v, list):
        return v
    msg = f"cultivar parameter {name} is no array: {v!r}"
    raise ValueError(msg)


def scalar(v: Any) -> Any:
    """[value, "unit"] -> value."""
    return v[0] if isinstance(v, list) and len(v) == 2 and isinstance(v[1], str) else v


def set_value(cultivar: dict[str, Any], row: Row, value: float) -> None:
    if row.position is None:
        v = cultivar[row.name]
        if isinstance(v, list) and len(v) == 2 and isinstance(v[1], str):
            v[0] = value
        else:
            cultivar[row.name] = value
        return
    a = values_of(cultivar, row.name)
    for i in row.position[:-1]:
        a = a[i]
    a[row.position[-1]] = value


def apply(cultivar: dict[str, Any], rows: list[Row], x: dict[str, float]) -> dict[str, Any]:
    """The cultivar with the calibrated values x (key -> value) and the derived ones."""
    c = copy.deepcopy(cultivar)
    species = {k.removeprefix(SPECIES): v for k, v in cultivar.items() if k.startswith(SPECIES)}
    ns: dict[str, Any] = {"cultivar": cultivar, "species": species, "math": math, **x}
    for r in rows:
        if r.derived is not None:
            value = eval(r.derived, {"__builtins__": {}}, ns)  # noqa: S307 - our own CSV
            ns[r.key] = value
        elif r.key in x:
            value = x[r.key]
        else:
            continue
        if r.name in cultivar:
            set_value(c, r, value)
    return c


# ---------------------------------------------------------------------------------------------
# the steps: residuals per treatment
# ---------------------------------------------------------------------------------------------


def day(v: Any) -> date | None:
    return pd.Timestamp(v).date() if v not in (None, "") and not pd.isna(v) else None


# MONICA's wheat stage (the Stage output, 1 = sowing) that ear emergence starts: stage 4 (to anthesis,
# ~180 °C d) fits the observed 1-13 days between ear emergence and anthesis; stage 3 starts in winter
# already, so jointing (JDAT) falls inside it and isn't used
EAR_EMERGENCE_STAGE = 4


def phenology(s: Setup, daily: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, float]:
    """Days simulated - observed for emergence, ear emergence, anthesis and maturity."""
    last = day(daily[-1]["DATE"]) if daily else s.end
    sim_ear = next((day(r["DATE"]) for r in daily if int(float(r["Stage"])) >= EAR_EMERGENCE_STAGE), None)
    res = {}
    for code, obs, sim in (
        ("PLDAE", s.obs.emergence, day(summary.get("PLDAE"))),
        ("EEMD", s.obs.ear_emergence, sim_ear),
        ("ADAT", s.obs.anthesis, day(summary.get("ADAT"))),
        ("MDAT", s.obs.maturity, day(summary.get("MDAT"))),
    ):
        if obs is None:
            continue
        res[code] = float(((sim or last + timedelta(days=MISSING_PENALTY_DAYS)) - obs).days)
    return res


# the residuals' units: 1 LAI unit weighs as much as 3 t/ha biomass (about their ranges' ratio)
LAI_SCALE = 1.0
BIOMASS_SCALE = 3000.0  # kg DM ha-1
GREEN_LEAF_SCALE = 1000.0  # kg DM ha-1, where the site has them (Observations.green_leaf)


def growth(s: Setup, daily: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, float]:
    """(simulated - observed) / scale of the daily LAI, above ground biomass and green leaves, keys <code>@<date>."""
    sim = {day(r["DATE"]): r for r in daily}
    # the runs harvest at maturity: an observation after it is compared with the crop at maturity
    maturity = day(summary.get("MDAT"))
    res = {}
    for code, obs, scale in (
        ("LAID", s.obs.lai, LAI_SCALE),
        ("CWAD", s.obs.biomass, BIOMASS_SCALE),
        ("WLVG", s.obs.green_leaf, GREEN_LEAF_SCALE),
    ):
        for d, v in (obs if obs is not None else {}).items():
            at = maturity if maturity is not None and d > maturity else d
            if (r := sim.get(at)) is not None and (x := number(r.get(code))) is not None:
                res[f"{code}@{d}"] = (x - v) / scale
    return res


def grain_yield(s: Setup, daily: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, float]:
    """Grain yield at maturity (GWAM; the last day's GWAD if not reached) - observed (HWAM) [t/ha]."""
    if s.obs.grain_yield is None:
        return {}
    sim = number(summary.get("GWAM"))
    if sim is None:
        sim = number(daily[-1].get("GWAD")) if daily else 0.0
    return {"HWAM": ((sim or 0.0) - s.obs.grain_yield) / 1000}


# the residuals' units of the water step: 1 mm/d ET weighs as much as 50 mm soil water
ET_SCALE = 1.0  # mm/d
SOIL_WATER_SCALE = 50.0  # mm
SOIL_WATER_OUTPUT = "SW_profile"  # the extra daily output of the water step: mean moisture of SOIL_WATER_DEPTHS_CM


def water(s: Setup, daily: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, float]:
    """(simulated - observed) / scale of the measured ET per period (mean mm/d) and the soil water [mm] in
    SOIL_WATER_DEPTHS_CM, keys <code>@<date>. Only where the site has them (Observations.et_periods,
    soil_water)."""
    sim = {day(r["DATE"]): r for r in daily}
    top, bottom = SOIL_WATER_DEPTHS_CM
    res = {}
    for a, b, v in s.obs.et_periods:
        days = [a + timedelta(days=i) for i in range((b - a).days + 1)]
        et = [number(sim[d].get("ETAD")) for d in days if d in sim]
        if len(et) == len(days) and None not in et:
            res[f"ETAP@{a}"] = (sum(et) / len(et) - v) / ET_SCALE
    for d, v in (s.obs.soil_water if s.obs.soil_water is not None else {}).items():
        if (r := sim.get(d)) is not None and (x := number(r.get(SOIL_WATER_OUTPUT))) is not None:
            res[f"SW@{d}"] = (x * (bottom - top) * 10 - v) / SOIL_WATER_SCALE
    return res


@dataclass
class Step:
    residuals: Callable[[Setup, list[dict[str, Any]], dict[str, Any]], dict[str, float]]
    unit: str


STEPS = {
    "phenology": Step(phenology, "d"),
    "growth": Step(growth, "LAI, 3 t/ha"),
    "yield": Step(grain_yield, "t/ha"),
    "water": Step(water, "mm/d ET, 50 mm soil water"),
}


# ---------------------------------------------------------------------------------------------
# running MONICA
# ---------------------------------------------------------------------------------------------


@dataclass
class Calibration:
    setups: list[Setup]
    run_dirs: dict[str, Path]
    crop_json: dict[str, dict[str, Any]]  # per treatment, the cultivar include replaced per evaluation
    cultivar: dict[str, Any]
    rows: list[Row]
    step: Step
    monica_run: Path
    jobs: int
    evaluations: int = 0
    labels: list[str] = field(default_factory=list)  # "<treatment> <code>", the order of residuals()

    def run(self, cultivar: dict[str, Any]) -> dict[str, tuple[list[dict[str, Any]], dict[str, Any]] | None]:
        def one(s: Setup) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
            crop = copy.deepcopy(self.crop_json[s.id])
            for ws in crop["cropRotation"][0]["worksteps"]:
                if ws["type"] == "Sowing":
                    ws["crop"]["cropParams"]["cultivar"], ws["crop"]["cropParams"]["species"] = split(cultivar)
            (self.run_dirs[s.id] / "crop.json").write_text(dump(crop))
            try:
                run_monica(self.monica_run, self.run_dirs[s.id])
            except RuntimeError as e:
                logger.warning("%s: %s", s.id, str(e).splitlines()[0])
                return None
            return collect(self.run_dirs[s.id])

        with ThreadPoolExecutor(max_workers=self.jobs) as pool:
            return dict(zip([s.id for s in self.setups], pool.map(one, self.setups), strict=True))

    def residuals_of(self, cultivar: dict[str, Any]) -> dict[str, dict[str, float]]:
        out = {}
        results = self.run(cultivar)
        for s in self.setups:
            res = results[s.id]
            if res is None:  # a failed run counts as every date missed
                out[s.id] = {c: float(MISSING_PENALTY_DAYS * 10) for c in self.step.residuals(s, [], {})}
            else:
                out[s.id] = self.step.residuals(s, *res)
        return out

    def residuals(self, x: dict[str, float]) -> list[float]:
        self.evaluations += 1
        per = self.residuals_of(apply(self.cultivar, self.rows, x))
        flat = {f"{tid} {code}": v for tid, r in per.items() for code, v in r.items()}
        if not self.labels:
            self.labels = list(flat)
        return [flat.get(k, float(MISSING_PENALTY_DAYS * 10)) for k in self.labels]


class SpotSetup:
    """spotpy's interface: simulation() returns the residuals, the evaluation is 0."""

    def __init__(self, cal: Calibration, free: list[Row]):
        self.cal = cal
        self.free = free
        self.params = [
            spotpy.parameter.Uniform(
                name=r.key,
                low=r.low,
                high=r.high,
                optguess=r.optguess if r.optguess is not None else (r.low + r.high) / 2,
            )
            for r in free
        ]
        self.n = len(cal.residuals({r.key: p.optguess for r, p in zip(free, self.params, strict=True)}))

    def parameters(self):
        return spotpy.parameter.generate(self.params)

    def simulation(self, vector):
        return self.cal.residuals({r.key: float(v) for r, v in zip(self.free, vector, strict=True)})

    def evaluation(self):
        return [0.0] * self.n

    def objectivefunction(self, simulation, evaluation, params=None):
        return spotpy.objectivefunctions.rmse(evaluation, simulation)


def rmse(values: list[float]) -> float:
    return math.sqrt(sum(v * v for v in values) / len(values)) if values else float("nan")


# ---------------------------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", choices=SITES, required=True)
    ap.add_argument("--step", choices=list(STEPS), required=True)
    ap.add_argument("--params-csv", type=Path, required=True, help="the parameters to calibrate")
    ap.add_argument("--xlsx", type=Path, help="default: the site's workbook")
    ap.add_argument("--treatments", nargs="*", help="only these")
    ap.add_argument("--params", type=Path, help="default: the site's PARAMS, else evapotranspiration/params")
    ap.add_argument(
        "--cultivar",
        help="the cultivar to start from: a JSON file, or an include path relative to --params; default: the site's",
    )
    ap.add_argument(
        "--species",
        help="the species to start from: a JSON file, or an include path relative to --params; default: crop.json's",
    )
    ap.add_argument(
        "--nitrogen-stress",
        action=argparse.BooleanOptionalAction,
        help="NitrogenResponseOn; default: the site's (NITROGEN_STRESS)",
    )
    ap.add_argument("--monica-run", type=Path, default=DEFAULT_MONICA_RUN, help="default: the Odin port")
    ap.add_argument("--reps", type=int, default=3000, help="maximum number of evaluations (SCE-UA)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--jobs", type=int, default=os.cpu_count(), help="MONICA runs in parallel")
    ap.add_argument(
        "--out",
        type=Path,
        help="default: the site's CALIBRATION_DIR (else evapotranspiration/calibration)/<site>_<step>",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    site = importlib.import_module(SITES[args.site])
    params_dir: Path = args.params or getattr(site, "PARAMS", ET_DIR / "params")
    out: Path = args.out or getattr(site, "CALIBRATION_DIR", ET_DIR / "calibration") / f"{args.site}_{args.step}"
    rows = read_rows(args.params_csv)
    free = [r for r in rows if r.derived is None and r.low is not None and r.high is not None]

    setups = site.load(args.xlsx or site.XLSX)
    if args.treatments:
        setups = [s for s in setups if s.id in args.treatments]
    cultivar_path = Path(args.cultivar) if args.cultivar and Path(args.cultivar).is_file() else None
    cultivar_include = None if cultivar_path else (args.cultivar or setups[0].cultivar)
    cultivar = json.loads((cultivar_path or params_dir / cultivar_include).read_text())
    species_path = Path(args.species) if args.species and Path(args.species).is_file() else None
    species_include = args.species or next(
        ws["crop"]["cropParams"]["species"][1]
        for ws in json.loads((ET_DIR / "crop.json").read_text())["cropRotation"][0]["worksteps"]
        if ws["type"] == "Sowing"
    )
    species = json.loads((species_path or params_dir / species_include).read_text())
    params = combine(cultivar, species)
    if unknown := [r.key for r in rows if r.position is not None and r.name not in params]:
        logger.error("not in the cultivar/species: %s", unknown)
        return 1

    nitrogen_stress = getattr(site, "NITROGEN_STRESS", False) if args.nitrogen_stress is None else args.nitrogen_stress
    logger.info("NitrogenResponseOn: %s", nitrogen_stress)
    run_dirs, crop_json = {}, {}
    for s in setups:
        # as long as the weather goes on without a gap
        weather_days, limit = set(s.weather.index), (s.harvest or s.end) + timedelta(days=EXTEND_DAYS)
        while s.end < limit and s.end + timedelta(days=1) in weather_days:
            s.end += timedelta(days=1)
        if s.co2_daily is not None and len(s.co2_daily) and s.co2_daily.index[-1] < s.end:
            # the daily CO2 (e.g. FACE) ends with the season: its last value for the extra days
            extra = pd.date_range(s.co2_daily.index[-1] + timedelta(days=1), s.end).date
            s.co2_daily = pd.concat([s.co2_daily, pd.Series(s.co2_daily.iloc[-1], index=extra)])
        s.latest_harvest, s.harvest = s.end, None
        if cultivar_include is not None:
            s.cultivar = cultivar_include
        run_dirs[s.id] = out / "work" / s.id
        events = None
        if args.step == "water":  # the soil water of the measured depths
            first = round(SOIL_WATER_DEPTHS_CM[0] / 100 / s.layer_thickness_m) + 1
            last = round(SOIL_WATER_DEPTHS_CM[1] / 100 / s.layer_thickness_m)
            events = output_events(
                daily_outputs(s.no_of_layers) | EXTRA_DAILY | {SOIL_WATER_OUTPUT: ["Mois", [first, last, "AVG"]]}
            )
        prepare(
            s,
            run_dirs[s.id],
            Options(params=params_dir, water_stress=True, nitrogen_stress=nitrogen_stress, events=events),
        )
        crop_json[s.id] = json.loads((run_dirs[s.id] / "crop.json").read_text())

    step = STEPS[args.step]
    cal = Calibration(setups, run_dirs, crop_json, params, rows, step, args.monica_run, args.jobs)
    default_res = cal.residuals_of(params)
    flat = [v for r in default_res.values() for v in r.values()]
    logger.info(
        "%d treatments, %d residuals; default cultivar: RMSE %.1f %s", len(setups), len(flat), rmse(flat), step.unit
    )
    logger.info("calibrating %s", ", ".join(f"{r.key} [{r.low:g}, {r.high:g}]" for r in free))

    spot = SpotSetup(cal, free)
    sampler = spotpy.algorithms.sceua(
        spot, dbname=str(out / "spotpy"), dbformat="csv", random_state=args.seed, save_sim=False
    )
    sampler.sample(args.reps, ngs=max(2, len(free)), kstop=5, peps=0.01, pcento=0.005)
    results = sampler.getdata()
    best = spotpy.analyser.get_best_parameterset(results, maximize=False)[0]
    # by position: spotpy's names lose characters like the "." of species.<Name>
    x = {r.key: float(v) for r, v in zip(free, list(best)[: len(free)], strict=True)}

    best_params = apply(params, rows, x)
    best_cultivar, best_species = split(best_params)
    (out / "cultivar.json").write_text(dump(best_cultivar))
    if any(r.name.startswith(SPECIES) for r in rows):
        (out / "species.json").write_text(dump(best_species))
    best_res = cal.residuals_of(best_params)

    with (out / "best.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["name", "position in array", "default", "calibrated", "low", "high", "derived params"])
        for r in rows:
            if r.name not in params:
                w.writerow(
                    [
                        r.name,
                        "",
                        "",
                        x.get(r.key, ""),
                        "" if r.low is None else r.low,
                        "" if r.high is None else r.high,
                        r.derived or "",
                    ]
                )
                continue
            if r.position:
                a, b = values_of(params, r.name), values_of(best_params, r.name)
                for i in r.position:
                    a, b = a[i], b[i]
            else:
                a, b = scalar(params[r.name]), scalar(best_params[r.name])
            w.writerow(
                [
                    r.name,
                    "_".join(map(str, r.position or ())),
                    a,
                    b,
                    "" if r.low is None else r.low,
                    "" if r.high is None else r.high,
                    r.derived or "",
                ]
            )

    report = []
    for s in setups:
        for code in default_res[s.id]:
            report.append(
                {
                    "treatment": s.id,
                    "variable": code,
                    "default": default_res[s.id][code],
                    "calibrated": best_res[s.id].get(code),
                }
            )
    df = pd.DataFrame(report)
    df.to_csv(out / "report.csv", index=False)
    print(f"\nsim - obs [{step.unit}], {cal.evaluations} evaluations")  # noqa: T201
    if df["variable"].nunique() <= 8:
        table = df.pivot(index="treatment", columns="variable", values=["default", "calibrated"])
    else:  # daily residuals: their RMSE per treatment and variable (the part before "@")
        df["var"] = df["variable"].str.split("@").str[0]
        table = df.groupby(["treatment", "var"])[["default", "calibrated"]].agg(lambda v: rmse(list(v.dropna())))
        table = table.unstack("var")
    print(table.round(2).to_string())  # noqa: T201
    rmse_default = rmse([v for r in default_res.values() for v in r.values()])
    rmse_best = rmse([v for r in best_res.values() for v in r.values()])
    print(f"\nRMSE: default {rmse_default:.1f}, calibrated {rmse_best:.1f} {step.unit}")  # noqa: T201
    print("\n" + (out / "best.csv").read_text())  # noqa: T201
    print(f"wrote {out}/cultivar.json (species.json), best.csv, report.csv, spotpy.csv")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
