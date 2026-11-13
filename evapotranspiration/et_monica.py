"""MONICA runs for the AgMIP Wheat ET2 study: from a site-independent description of a treatment
(`Setup`) to MONICA inputs, a monica-run call and the AMEI result templates.

The site readers (`site_bushland.py`, `site_avignon.py`) turn their xlsx into `Setup`s; everything
here is the same for all sites. `run_et.py` and `evaluate_et.py` are the command lines.
"""

from __future__ import annotations

import copy
import csv
import json
import logging
import math
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger("et_monica")

HERE = Path(__file__).resolve().parent
TEMPLATES = HERE / "3_Templates/Evaluation"
LAYER_THICKNESS_M = 0.05

# ---------------------------------------------------------------------------------------------
# what a site reader delivers
# ---------------------------------------------------------------------------------------------

WEATHER_COLUMNS = ["tmin", "tavg", "tmax", "precip", "globrad", "wind", "relhumid", "vaporpress"]
WEATHER_UNITS = ["C_deg", "C_deg", "C_deg", "mm", "MJ m-2", "m/s", "%", "kPa"]


@dataclass
class Horizon:
    top_cm: float
    bottom_cm: float
    sand: float  # %
    clay: float  # %
    soc: float  # %
    bulk_density: float  # g cm-3
    fc: float  # m3 m-3
    pwp: float  # m3 m-3
    sat: float  # m3 m-3
    ph: float | None = None
    cn: float | None = None
    stones: float | None = None  # %
    nitrate_kg_ha: float | None = None  # in the whole horizon
    ammonium_kg_ha: float | None = None


@dataclass
class Fertilization:
    date: date
    kg_n: float
    nh4: float  # fraction of the N
    no3: float
    name: str


@dataclass
class Observations:
    et: pd.Series  # 24 h ET [mm/d], date index
    et_daytime: pd.Series | None = None  # e.g. Avignon's 6-18 h eddy covariance sums
    emergence: date | None = None
    anthesis: date | None = None
    maturity: date | None = None
    lai_max: float | None = None
    grain_yield: float | None = None  # kg/ha


@dataclass
class Setup:
    id: str
    start: date
    end: date
    latitude: float
    elevation: float
    weather: pd.DataFrame  # index: date, columns: WEATHER_COLUMNS
    horizons: list[Horizon]
    sowing: date
    cultivar: str  # include path, relative to the params dir
    no_of_layers: int
    harvest: date | None = None
    latest_harvest: date | None = None  # harvest at maturity, at the latest on this date
    slope: float = 0.0  # m m-1
    wind_height: float = 2.0
    co2: float | None = None  # None: MONICA's own CO2 by year
    albedo: float | None = None
    initial_moisture: list[tuple[float, float, float]] = field(default_factory=list)  # (top cm, bottom cm, m3 m-3)
    initial_n_total_kg_ha: float | None = None  # spread evenly, if the horizons have no N
    fertilizations: list[Fertilization] = field(default_factory=list)
    irrigations: list[tuple[date, float]] = field(default_factory=list)  # (date, mm)
    obs: Observations = field(default_factory=lambda: Observations(pd.Series(dtype=float)))
    notes: list[str] = field(default_factory=list)


def sat_vapor_pressure(t: Any) -> Any:
    """[kPa], FAO56 eq. 11."""
    return 0.6108 * np.exp(17.27 * t / (t + 237.3))


def wind_at_2m(u: Any, height: Any) -> Any:
    """FAO56 eq. 47."""
    return u * 4.87 / np.log(67.8 * height - 5.42)


def nan_to_none(v: Any) -> Any:
    return None if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NaT else v


def to_date(v: Any) -> date | None:
    v = nan_to_none(v)
    return None if v is None else pd.Timestamp(v).date()


# ---------------------------------------------------------------------------------------------
# outputs: template code -> MONICA output spec (None = MONICA has no such output, written as NA)
# ---------------------------------------------------------------------------------------------


def daily_outputs(no_of_layers: int) -> dict[str, Any]:
    """In the column order of the daily template's code row."""
    n = no_of_layers
    return {
        "LNUM": None,
        "GSTZD": None,
        "LAID": "LAI",
        "LIPCD": None,
        "CWAD": "AbBiom",
        "GWAD": ["OrgBiom", "Fruit"],
        "GWGD": None,
        "CNAD": "AbBiomN",
        "GNAD": "GrainN",
        "GNGD": None,
        "RDPD": "EffRootDep",
        # the template asks for cm3/cm3 for the whole profile: the mean over the layers, not the sum
        "SWWPD": ["Mois", [1, n, "AVG"]],
        # what leaves the bottom of the simulated profile
        "DRND": ["PercolationRate", [n, n, "AVG"]],
        "ROFD": "RunOff",
        "NIAD": None,
        "NLCD": "NLeach",
        "NMND": ["Nmin", [1, n, "SUM"]],
        "N2OED": "N2O",
        "NIMD": None,
        "NDND": "Denit",
        "GHFD": None,
        "LHFD": None,
        "HHFD": None,
        "RND": "net_radiation",
        "EOAD": "Pot_ET",
        "ETAD": "Act_ET",
        "EPSAD": None,
        "ESAD": "Act_Ev",
        "EPPAD": None,
        "EPAD": "Act_Trans",
        "ETO": "ET0",
        "ETOS": None,
        "ETRS": None,
        "ETP": "Pot_ET",
    }


# not in the template, but useful to look at and to check a run with
EXTRA_DAILY: dict[str, Any] = {"Stage": "Stage", "Kc": "Kc", "Precip": "Precip", "Tavg": "Tavg"}

# at-event values: dates and the biomass/N at anthesis and maturity
EVENTS: dict[str, list[tuple[str, Any]]] = {
    "Sowing": [("PDATE", "Date")],
    "emergence": [("PLDAE", "Date")],
    "anthesis": [("ADAT", "Date"), ("CWAA", "AbBiom"), ("CNAA", "AbBiomN")],
    "maturity": [
        ("MDAT", "Date"),
        ("CWAM", "AbBiom"),
        ("GWAM", ["OrgBiom", "Fruit"]),
        ("CNAM", "AbBiomN"),
        ("GNAM", "GrainN"),
    ],
}

# season values from the daily output between sowing and maturity (the end of the simulation if
# MONICA didn't reach maturity): template code -> (daily code, how)
SEASON: dict[str, tuple[str, str]] = {
    "LAIX": ("LAID", "max"),
    "RDPM": ("RDPD", "max"),
    "DRCM": ("DRND", "sum"),
    "ROCM": ("ROFD", "sum"),
    "NLCM": ("NLCD", "sum"),
    "NMNCM": ("NMND", "sum"),
    "N2OECM": ("N2OED", "sum"),
    "NDNCM": ("NDND", "sum"),
    "EOCM": ("EOAD", "sum"),
    "ETCM": ("ETAD", "sum"),
    "ESCM": ("ESAD", "sum"),
    "EPCM": ("EPAD", "sum"),
    "ETO": ("ETO", "sum"),
    "ETP": ("ETP", "sum"),
}

DIGITS = 4  # what goes into the result files


def out_spec(alias: str, spec: Any) -> Any:
    """'LAI', 'LAID' -> 'LAI|LAID'; ['Mois', [1, 42, 'AVG']], 'SWWPD' -> ['Mois|SWWPD', [1, 42, 'AVG']]."""
    if isinstance(spec, str):
        return f"{spec}|{alias}"
    return [f"{spec[0]}|{alias}", *spec[1:]]


# ---------------------------------------------------------------------------------------------
# MONICA inputs
# ---------------------------------------------------------------------------------------------


@dataclass
class Options:
    params: Path
    water_stress: bool = True
    nitrogen_stress: bool = False
    vp_factor: float = 1.0  # VPRSD is multiplied by this; 10 reproduces hPa


def write_climate(path: Path, s: Setup, vp_factor: float) -> None:
    w = s.weather.loc[s.start : s.end]
    expected = pd.date_range(s.start, s.end).date
    missing_days = sorted(set(expected) - set(w.index))
    if missing_days:
        msg = f"{s.id}: no weather for {len(missing_days)} day(s), e.g. {missing_days[:5]}"
        raise ValueError(msg)
    gaps = {c: int(w[c].isna().sum()) for c in WEATHER_COLUMNS if w[c].isna().any()}
    if gaps:
        msg = f"{s.id}: missing weather values {gaps}"
        raise ValueError(msg)
    with path.open("w", newline="") as f:
        out = csv.writer(f, delimiter=";")
        out.writerow(["iso-date", *WEATHER_COLUMNS])
        out.writerow(["", *WEATHER_UNITS])
        for day, r in w.iterrows():
            vals = [round(float(r[c]), 6) for c in WEATHER_COLUMNS]
            vals[-1] = round(vals[-1] * vp_factor, 6)
            out.writerow([str(day), *vals])


def depth_weighted(intervals: Iterable[tuple[float, float, float]], top: float, bottom: float) -> float | None:
    """Mean of the values of the [top, bottom) intervals overlapping [top, bottom), weighted by overlap."""
    total = weight = 0.0
    for t, b, v in intervals:
        overlap = min(b, bottom) - max(t, top)
        if overlap > 0 and v is not None and not math.isnan(v):
            total += overlap * v
            weight += overlap
    return total / weight if weight > 0 else None


def soil_layers(s: Setup) -> tuple[list[dict[str, Any]], list[str]]:
    """The profile as MONICA layers of LAYER_THICKNESS_M with the initial moisture and N.

    A layer below the last horizon gets the last horizon's properties.
    """
    profile_depth = s.no_of_layers * LAYER_THICKNESS_M
    horizons_have_n = any(h.nitrate_kg_ha is not None or h.ammonium_kg_ha is not None for h in s.horizons)
    even_nitrate = (
        s.initial_n_total_kg_ha / 10000.0 / profile_depth
        if s.initial_n_total_kg_ha is not None and not horizons_have_n
        else None
    )
    notes: list[str] = []
    if s.horizons[-1].bottom_cm < profile_depth * 100:
        notes.append(
            f"profile ends at {s.horizons[-1].bottom_cm} cm, last horizon extended to {profile_depth * 100:g} cm"
        )

    layers, above_fc = [], []
    for i in range(s.no_of_layers):
        top_cm, bottom_cm = i * LAYER_THICKNESS_M * 100, (i + 1) * LAYER_THICKNESS_M * 100
        mid = (top_cm + bottom_cm) / 2
        h = next((h for h in s.horizons if h.top_cm <= mid < h.bottom_cm), s.horizons[-1])
        layer: dict[str, Any] = {
            "Thickness": [LAYER_THICKNESS_M, "m"],
            "Sand": [h.sand / 100, "kg kg-1"],
            "Clay": [h.clay / 100, "kg kg-1"],
            "SoilOrganicCarbon": [h.soc, "mass%"],
            "SoilBulkDensity": [h.bulk_density * 1000, "kg m-3"],
            "FieldCapacity": [h.fc, "m3 m-3"],
            "PermanentWiltingPoint": [h.pwp, "m3 m-3"],
            "PoreVolume": [h.sat, "m3 m-3"],
        }
        if h.ph is not None:
            layer["pH"] = h.ph
        if h.cn is not None:
            layer["CN"] = h.cn
        if h.stones is not None:
            layer["Sceleton"] = [h.stones / 100, "m3 m-3"]
        theta = depth_weighted(s.initial_moisture, top_cm, bottom_cm)
        if theta is not None:
            percent_fc = theta / h.fc * 100
            if percent_fc > 100:
                above_fc.append(f"{i + 1} ({theta:.3f} > {h.fc})")
                percent_fc = 100.0  # MONICA accepts at most 100 % of FC
            layer["SoilMoisturePercentFC"] = [round(percent_fc, 3), "% [0-100]"]
        horizon_m = (h.bottom_cm - h.top_cm) / 100
        if h.nitrate_kg_ha is not None:
            layer["SoilNitrate"] = [h.nitrate_kg_ha / 10000.0 / horizon_m, "kg NO3-N m-3"]
        elif even_nitrate is not None:
            layer["SoilNitrate"] = [even_nitrate, "kg NO3-N m-3"]
        if h.ammonium_kg_ha is not None:
            layer["SoilAmmonium"] = [h.ammonium_kg_ha / 10000.0 / horizon_m, "kg NH4-N m-3"]
        layers.append(layer)
    if above_fc:
        notes.append(f"initial moisture above field capacity, set to FC, in layer(s) {', '.join(above_fc)}")
    if not s.initial_moisture:
        notes.append("no initial moisture given, MONICA's default used")
    return layers, notes


def worksteps(s: Setup, sowing_template: dict[str, Any]) -> list[dict[str, Any]]:
    sowing = copy.deepcopy(sowing_template)
    sowing["date"] = str(s.sowing)
    sowing["crop"]["cropParams"]["cultivar"] = ["include-from-file", s.cultivar]
    steps: list[dict[str, Any]] = [sowing]
    if s.harvest is not None:
        steps.append({"type": "Harvest", "date": str(s.harvest)})
    if s.latest_harvest is not None:
        steps.append(
            {
                "type": "AutomaticHarvest",
                "date": str(s.latest_harvest),  # only for sorting, MONICA applies it when the condition holds
                "harvest-time": "maturity",
                "latest-date": str(s.latest_harvest),
            }
        )
    for f in s.fertilizations:
        steps.append(
            {
                "type": "MineralFertilization",
                "date": str(f.date),
                "amount": [f.kg_n, "kg N"],
                "partition": {"id": f.name, "name": f.name, "Carbamid": 0.0, "NH4": f.nh4, "NO3": f.no3},
            }
        )
    for day, mm in s.irrigations:
        steps.append(
            {
                "type": "Irrigation",
                "date": str(day),
                "amount": [mm, "mm"],
                "parameters": {"nitrateConcentration": [0.0, "mg dm-3"], "sulfateConcentration": [0.0, "mg dm-3"]},
            }
        )
    outside = [st for st in steps if not (str(s.start) <= st["date"] <= str(s.end))]
    for st in outside:
        logger.warning(
            "%s: %s on %s is outside the simulation %s..%s, dropped", s.id, st["type"], st["date"], s.start, s.end
        )
    return sorted((st for st in steps if st not in outside), key=lambda st: st["date"])


def sim_json(base: dict[str, Any], s: Setup, opts: Options) -> dict[str, Any]:
    sim = {k: v for k, v in base.items() if k != "output"}
    sim |= {
        "crop.json": "crop.json",
        "site.json": "site.json",
        "climate.csv": "climate.csv",
        "climate.csv-options": {
            "no-of-climate-file-header-lines": 2,
            "lineNoOfHeaderLine": 1,
            "csv-separator": ";",
            "header-to-acd-names": {},
        },
        "include-file-base-path": f"{opts.params.resolve()}/",
        "path-to-output": "./",
        "WaterDeficitResponseOn": opts.water_stress,
        "NitrogenResponseOn": opts.nitrogen_stress,
    }
    daily = ["Date|DATE"] + [
        out_spec(k, v) for k, v in (daily_outputs(s.no_of_layers) | EXTRA_DAILY).items() if v is not None
    ]
    events: list[Any] = ["daily", daily]
    for event, outs in EVENTS.items():
        events += [event, [out_spec(code, spec) for code, spec in outs]]
    sim["output"] = {
        "write-file?": False,  # monica-run prints the sections to stdout
        "csv-options": {"include-header-row": True, "include-units-row": False, "csv-separator": ","},
        "obj-outputs?": True,
        "events": events,
    }
    return sim


def site_json(base: dict[str, Any], s: Setup, layers: list[dict[str, Any]], params: Path) -> dict[str, Any]:
    site = copy.deepcopy(base)
    sp = site["SiteParameters"]
    sp["Latitude"] = s.latitude
    sp["HeightNN"] = [s.elevation, "m"]
    sp["Slope"] = s.slope
    sp["NumberOfLayers"] = s.no_of_layers
    sp["LayerThickness"] = [LAYER_THICKNESS_M, "m"]
    sp["SoilProfileParameters"] = layers
    env: dict[str, Any] = {"WindSpeedHeight": s.wind_height}
    if s.co2 is not None:
        env["AtmosphericCO2"] = s.co2
    if s.albedo is not None:
        env["Albedo"] = s.albedo
    # An object here replaces the include of general/environment.json entirely - the parameters not
    # in it fall back to MONICA's built-in defaults. So the include is resolved and ours put on top.
    env_base = site.get("EnvironmentParameters")
    if isinstance(env_base, list) and env_base[:1] == ["include-from-file"]:
        env_base = json.loads((params / env_base[1]).read_text())
    site["EnvironmentParameters"] = (env_base or {}) | env
    return site


def dump(o: Any) -> str:
    # numbers from pandas come as numpy scalars
    return json.dumps(o, indent=2, default=lambda v: v.item() if isinstance(v, np.generic) else str(v))


def prepare(s: Setup, run_dir: Path, opts: Options) -> list[str]:
    """Writes sim.json, crop.json, site.json and climate.csv into run_dir. Returns notes."""
    run_dir.mkdir(parents=True, exist_ok=True)
    crop_template = json.loads((HERE / "crop.json").read_text())
    sowing_template = next(ws for ws in crop_template["cropRotation"][0]["worksteps"] if ws["type"] == "Sowing")
    crop = {k: v for k, v in crop_template.items() if k not in ("ws", "cropRotation")}
    crop["cropRotation"] = [{"worksteps": worksteps(s, sowing_template)}]

    layers, notes = soil_layers(s)
    site_base = json.loads((HERE / "site.json").read_text())
    sim_base = json.loads((HERE / "sim_full_run.json").read_text())

    write_climate(run_dir / "climate.csv", s, opts.vp_factor)
    (run_dir / "crop.json").write_text(dump(crop))
    (run_dir / "site.json").write_text(dump(site_json(site_base, s, layers, opts.params)))
    (run_dir / "sim.json").write_text(dump(sim_json(sim_base, s, opts)))
    return s.notes + notes


def run_monica(monica_run: Path, run_dir: Path) -> None:
    res = subprocess.run([str(monica_run), "sim.json"], cwd=run_dir, capture_output=True, text=True, check=False)
    (run_dir / "monica_out.csv").write_text(res.stdout)
    (run_dir / "monica.log").write_text(res.stderr)
    if res.returncode != 0 or not res.stdout.startswith('"'):
        msg = f"monica-run failed in {run_dir} (exit {res.returncode}), see monica.log:\n{res.stderr[-600:]}"
        raise RuntimeError(msg)


# ---------------------------------------------------------------------------------------------
# collecting and writing the results
# ---------------------------------------------------------------------------------------------


def read_sections(path: Path) -> list[list[dict[str, str]]]:
    """monica_out.csv holds one table per output section.

    Each section is its quoted name ("daily", "Sowing", ...) on a line of its own, the header row,
    the data rows (none if the event never happened) and a blank line.
    """
    sections: list[list[dict[str, str]]] = []
    header: list[str] | None = None
    for line in path.read_text().splitlines():
        if line.startswith('"'):
            sections.append([])
            header = None
        elif not line.strip() or not sections:
            continue
        elif header is None:
            header = next(csv.reader([line]))
        else:
            sections[-1].append(dict(zip(header, next(csv.reader([line])), strict=True)))
    return sections


def number(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def collect(run_dir: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Daily rows (template codes) and the summary of one run."""
    daily, *event_sections = read_sections(run_dir / "monica_out.csv")
    summary: dict[str, Any] = {}
    for (_event, outs), rows in zip(EVENTS.items(), event_sections, strict=True):
        if rows:
            # one season per run, so one of each at most; take the first
            summary |= {code: rows[0].get(code) for code, _ in outs}

    days = [r["DATE"] for r in daily]
    start = summary.get("PDATE") or days[0]
    end = summary.get("MDAT") or days[-1]
    season = [r for r in daily if start <= r["DATE"] <= end]
    for code, (dcode, how) in SEASON.items():
        values = [x for r in season if (x := number(r.get(dcode))) is not None]
        if values:
            summary[code] = max(values) if how == "max" else sum(values)
    return daily, summary


def template_header(path: Path) -> tuple[list[str], list[list[str]]]:
    """The lines before the column header, and the name/unit/code rows, of an AMEI template."""
    rows = [line.split("\t") for line in path.read_text(encoding="utf-8").splitlines()]
    code_row = next(i for i, r in enumerate(rows) if r and r[0] == "MODEL_ID")
    names, units, codes = (r[:] for r in rows[code_row - 2 : code_row + 1])
    while names and len(names) > len(codes) and "soil_temp_surface_daily_avg" in names:
        # the daily template names a column (TSSAV) that has neither a unit nor a code
        names.remove("soil_temp_surface_daily_avg")
    preamble = ["\t".join(r) for r in rows[: code_row - 2]]
    return preamble, [names, units, codes]


def fmt(v: Any) -> str:
    if v is None or v == "":
        return "NA"
    x = number(v)
    if x is None:
        return str(v)
    return f"{round(x, DIGITS):g}"


def write_result(path: Path, template: Path, model_line: str, rows: list[list[Any]]) -> None:
    preamble, header = template_header(template)
    preamble = [model_line if line.startswith("Model:") else line for line in preamble]
    with path.open("w", newline="") as f:
        for line in preamble:
            f.write(line.rstrip("\t") + "\n")
        for r in header:
            f.write("\t".join(r) + "\n")
        for r in rows:
            f.write("\t".join(fmt(v) for v in r) + "\n")


def result_names(kind: str, module_code: str, site_name: str) -> tuple[str, str]:
    """(summary file name, daily file name pattern with {id})."""
    return (
        f"ETstudy{kind}{module_code}Summary{site_name}.txt",
        f"ETstudy{kind}{module_code}Daily{site_name}{{id}}.txt",
    )


def read_result(path: Path) -> pd.DataFrame:
    lines = path.read_text().splitlines()
    code_row = next(i for i, line in enumerate(lines) if line.startswith("MODEL_ID"))
    return pd.read_csv(path, sep="\t", skiprows=code_row, na_values="NA")
