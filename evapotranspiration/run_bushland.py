"""Run MONICA on the treatments of an AgMIP/ICASA style xlsx (Bushland) and write the AMEI ET templates.

Phase 1 of the AgMIP Wheat ET2 study, evaluation step: original model, MONICA FAO56-PM (MOFAPM).

Per treatment, everything MONICA gets is written to <out>/runs/<TREAT_ID>/ (sim.json, crop.json,
site.json, climate.csv) together with what it returns (daily.csv, events.json, monica.log), so any
single run can be inspected and re-run by hand:

    cd <out>/runs/TXBL1990F && monica-run sim.json

The template files go to <out>/results/.

    pixi run python evapotranspiration/run_bushland.py [--treatments TXBL1990F ...]
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import logging
import math
import os
import re
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger("run_bushland")

HERE = Path(__file__).resolve().parent
LAYER_THICKNESS_M = 0.05
NO_OF_LAYERS = 42  # 2.1 m

# ---------------------------------------------------------------------------------------------
# outputs: template code -> MONICA output spec (None = MONICA has no such output, written as NA)
# ---------------------------------------------------------------------------------------------

# daily, in the column order of the template's code row
DAILY: dict[str, Any] = {
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
    # the template asks for cm3/cm3 for the whole profile, so the mean over the layers, not the sum
    "SWWPD": ["Mois", [1, NO_OF_LAYERS, "AVG"]],
    # what leaves the bottom of the simulated profile
    "DRND": ["PercolationRate", [NO_OF_LAYERS, NO_OF_LAYERS, "AVG"]],
    "ROFD": "RunOff",
    "NIAD": None,
    "NLCD": "NLeach",
    "NMND": ["Nmin", [1, NO_OF_LAYERS, "SUM"]],
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
# not in the template, but useful to look at and to check the run with
EXTRA_DAILY: dict[str, Any] = {"Stage": "Stage", "Kc": "Kc", "Precip": "Precip", "Tavg": "Tavg"}

# at-event values; dates and the biomass/N at anthesis and maturity
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

# season values computed from the daily output between sowing and maturity (harvest if MONICA
# didn't reach maturity): template code -> (daily code, how)
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


# ---------------------------------------------------------------------------------------------
# reading the workbook
# ---------------------------------------------------------------------------------------------


def read_sheets(xlsx: Path) -> dict[str, pd.DataFrame]:
    """All sheets, with the code row (row 3) as header and the empty rows/columns dropped."""
    sheets = pd.read_excel(xlsx, sheet_name=None, header=2)
    return {
        name: df.dropna(how="all")
        .dropna(axis=1, how="all")
        .loc[:, lambda d: ~d.columns.astype(str).str.startswith("Unnamed")]
        for name, df in sheets.items()
    }


def iso(v: Any) -> str | None:
    if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NaT:
        return None
    return str(pd.Timestamp(v).date())


@dataclass
class Treatment:
    id: str
    sdat: date
    endat: date
    row: dict[str, Any]
    # rows of the other sheets that belong to this treatment
    rows: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def one(self, sheet: str) -> dict[str, Any]:
        rows = self.rows.get(sheet, [])
        if len(rows) != 1:
            msg = f"{self.id}: expected one row in {sheet}, found {len(rows)}"
            raise ValueError(msg)
        return rows[0]


def treatment_id_resolver(treatments: dict[str, Treatment]):
    """Maps the treatment ids used in the other sheets to the ones of the Treatments sheet.

    In the Bushland file, Treatments/Plots/Planting/Fertilizer use TXBL1990F (harvest year), while
    Irrigation, the observations and ET_Daily use TXBL1989F (sowing year) for the same season. An id
    that isn't in the Treatments sheet is matched on the same prefix and suffix with either the
    sowing or the harvest year of a treatment.
    """

    pattern = re.compile(r"^(.*?)(\d{4})(.*)$")

    def resolve(tid: str) -> str | None:
        if tid in treatments:
            return tid
        m = pattern.match(tid)
        if not m:
            return None
        prefix, year, suffix = m.group(1), int(m.group(2)), m.group(3)
        candidates = [
            t.id
            for t in treatments.values()
            if (p := pattern.match(t.id))
            and p.group(1) == prefix
            and p.group(3) == suffix
            and year in (t.sdat.year, t.endat.year)
        ]
        return candidates[0] if len(candidates) == 1 else None

    return resolve


def load_treatments(sheets: dict[str, pd.DataFrame]) -> dict[str, Treatment]:
    treatments = {}
    for r in sheets["Treatments"].to_dict("records"):
        tid = str(r["TREAT_ID"])
        treatments[tid] = Treatment(tid, pd.Timestamp(r["SDAT"]).date(), pd.Timestamp(r["ENDAT"]).date(), r)

    resolve = treatment_id_resolver(treatments)
    unmatched: dict[str, set[str]] = {}
    for name, df in sheets.items():
        if name == "Treatments" or "TREAT_ID" not in df.columns:
            continue
        for r in df.to_dict("records"):
            tid = resolve(str(r["TREAT_ID"]))
            if tid is None:
                unmatched.setdefault(name, set()).add(str(r["TREAT_ID"]))
                continue
            treatments[tid].rows.setdefault(name, []).append(r)
    for name, ids in unmatched.items():
        logger.warning("%s: rows of unknown treatments %s skipped", name, sorted(ids))
    return treatments


# ---------------------------------------------------------------------------------------------
# MONICA inputs
# ---------------------------------------------------------------------------------------------


def climate_rows(sheets: dict[str, pd.DataFrame], t: Treatment, vp_factor: float) -> list[list[Any]]:
    w = sheets["Weather_daily"]
    w = w[w["WST_DATASET"] == t.row["WST_DATASET"]].copy()
    w["day"] = pd.to_datetime(w["W_DATE"]).dt.date
    w = w[(w["day"] >= t.sdat) & (w["day"] <= t.endat)].sort_values("day")

    expected = pd.date_range(t.sdat, t.endat).date
    missing_days = sorted(set(expected) - set(w["day"]))
    if missing_days:
        msg = f"{t.id}: no weather for {len(missing_days)} day(s), e.g. {missing_days[:5]}"
        raise ValueError(msg)
    cols = ["TMIN", "TAVD", "TMAX", "RAIN", "SRAD", "WIND", "RHUMD", "VPRSD"]
    gaps = {c: int(w[c].isna().sum()) for c in cols if w[c].isna().any()}
    if gaps:
        msg = f"{t.id}: missing weather values {gaps}"
        raise ValueError(msg)

    return [
        [
            str(r.day),
            r.TMIN,
            r.TAVD,
            r.TMAX,
            r.RAIN,
            r.SRAD,  # MJ m-2 d-1
            round(r.WIND / 86.4, 4),  # km d-1 -> m s-1
            r.RHUMD,  # %
            round(r.VPRSD * vp_factor, 6),  # kPa, which MONICA's ET0 expects (see README)
        ]
        for r in w.itertuples()
    ]


def write_climate(path: Path, rows: list[list[Any]]) -> None:
    with path.open("w", newline="") as f:
        out = csv.writer(f, delimiter=";")
        out.writerow(["iso-date", "tmin", "tavg", "tmax", "precip", "globrad", "wind", "relhumid", "vaporpress"])
        out.writerow(["", "C_deg", "C_deg", "C_deg", "mm", "MJ m-2", "m/s", "%", "kPa"])
        out.writerows(rows)


def depth_weighted(intervals: Iterable[tuple[float, float, float]], top: float, bottom: float) -> float | None:
    """Mean of the values of the [top, bottom) intervals overlapping [top, bottom), weighted by overlap."""
    total = weight = 0.0
    for t, b, v in intervals:
        overlap = min(b, bottom) - max(t, top)
        if overlap > 0 and v is not None and not math.isnan(v):
            total += overlap * v
            weight += overlap
    return total / weight if weight > 0 else None


def soil_profile(sheets: dict[str, pd.DataFrame], t: Treatment) -> tuple[list[dict[str, Any]], list[str]]:
    """The soil profile as MONICA layers of LAYER_THICKNESS_M, with the treatment's initial moisture.

    Returns the layers and notes on what had to be adjusted.
    """
    soil_id = t.one("Plots")["SOIL_ID"]
    horizons = sheets["Soil_profile_layers"]
    horizons = horizons[horizons["SOIL_ID"] == soil_id].sort_values("SLLT").reset_index(drop=True)
    notes: list[str] = []
    # a value missing in a deeper horizon (e.g. pH below 140 cm in Bushland) is taken from the one above
    for col in ("SLSND", "SLCLY", "SLOC", "SLBDM", "SLDUL", "SLLL", "SLSAT", "SLPHW", "C_N"):
        gaps = horizons.index[horizons[col].isna()].tolist()
        if gaps:
            notes.append(
                f"{col} missing for horizon(s) starting at {horizons.loc[gaps, 'SLLT'].tolist()} cm, taken from above"
            )
            horizons[col] = horizons[col].ffill()
    horizons = horizons.to_dict("records")
    ic = sorted(t.rows.get("initial_condition_layers", []), key=lambda r: r["ICTL"])
    moisture = [(float(r["ICTL"]), float(r["ICBL"]), float(r["ICH2O"])) for r in ic]

    # initial mineral N: only a profile total (ICIN) is given, spread evenly over the profile
    icin = t.one("initial_conditions").get("ICIN")
    profile_depth = NO_OF_LAYERS * LAYER_THICKNESS_M
    nitrate_kg_per_m3 = float(icin) / 10000.0 / profile_depth if icin is not None and not math.isnan(icin) else None

    layers, above_fc = [], []
    for i in range(NO_OF_LAYERS):
        top_cm, bottom_cm = i * LAYER_THICKNESS_M * 100, (i + 1) * LAYER_THICKNESS_M * 100
        mid = (top_cm + bottom_cm) / 2
        h = next((h for h in horizons if h["SLLT"] <= mid < h["SLLB"]), horizons[-1])
        fc = float(h["SLDUL"])
        layer: dict[str, Any] = {
            "Thickness": [LAYER_THICKNESS_M, "m"],
            "Sand": [float(h["SLSND"]) / 100, "kg kg-1"],
            "Clay": [float(h["SLCLY"]) / 100, "kg kg-1"],
            "SoilOrganicCarbon": [float(h["SLOC"]), "mass%"],
            "SoilBulkDensity": [float(h["SLBDM"]) * 1000, "kg m-3"],
            "FieldCapacity": [fc, "m3 m-3"],
            "PermanentWiltingPoint": [float(h["SLLL"]), "m3 m-3"],
            "PoreVolume": [float(h["SLSAT"]), "m3 m-3"],
            "pH": float(h["SLPHW"]),
            "CN": float(h["C_N"]),
            "Sceleton": [float(h.get("SLCF", 0) or 0) / 100, "m3 m-3"],
        }
        theta = depth_weighted(moisture, top_cm, bottom_cm)
        if theta is not None:
            percent_fc = theta / fc * 100
            if percent_fc > 100:
                above_fc.append(f"{i + 1} ({theta:.3f} > {fc})")
                percent_fc = 100.0  # MONICA accepts at most 100 % of FC
            layer["SoilMoisturePercentFC"] = [round(percent_fc, 3), "% [0-100]"]
        if nitrate_kg_per_m3 is not None:
            layer["SoilNitrate"] = [nitrate_kg_per_m3, "kg NO3-N m-3"]
        layers.append(layer)
    if above_fc:
        notes.append(f"initial moisture above field capacity, set to FC, in layer(s) {', '.join(above_fc)}")
    if not moisture:
        notes.append("no initial moisture given, MONICA's default used")
    return layers, notes


def worksteps(t: Treatment, crop_template: dict[str, Any]) -> list[dict[str, Any]]:
    planting = t.one("Planting_events")
    harvest = t.one("Harvest_events")
    sowing = copy.deepcopy(next(ws for ws in crop_template["cropRotation"][0]["worksteps"] if ws["type"] == "Sowing"))
    sowing["date"] = iso(planting["PDATE"])
    steps = [sowing, {"type": "Harvest", "date": iso(harvest["HADAT"])}]

    for f in t.rows.get("Fertilizer_events", []):
        amount = float(f["FEAMN"])
        if amount <= 0:
            continue
        nh4 = float(f.get("FENH4") or 0)
        no3 = float(f.get("FENO3") or 0)
        steps.append(
            {
                "type": "MineralFertilization",
                "date": iso(f["FEDATE"]),
                "amount": [amount, "kg N"],
                "partition": {
                    "id": str(f["FECD"]),
                    "name": str(f["FECD"]),
                    "Carbamid": 0.0,
                    "NH4": nh4 / amount if nh4 + no3 > 0 else 1.0,
                    "NO3": no3 / amount if nh4 + no3 > 0 else 0.0,
                },
            }
        )

    for i in t.rows.get("Irrigation_events", []):
        amount = float(i["IRVAL"])  # not int(): that cut 10.2 mm down to 10 mm
        if amount <= 0:
            continue
        steps.append(
            {
                "type": "Irrigation",
                "date": iso(i["IDATE"]),
                "amount": [amount, "mm"],
                "parameters": {"nitrateConcentration": [0.0, "mg dm-3"], "sulfateConcentration": [0.0, "mg dm-3"]},
            }
        )

    outside = [s for s in steps if not (str(t.sdat) <= s["date"] <= str(t.endat))]
    for s in outside:
        logger.warning(
            "%s: %s on %s is outside the simulation %s..%s, dropped", t.id, s["type"], s["date"], t.sdat, t.endat
        )
    return sorted((s for s in steps if s not in outside), key=lambda s: s["date"])


def out_spec(alias: str, spec: Any) -> Any:
    """'LAI', 'LAID' -> 'LAI|LAID'; ['Mois', [1, 42, 'AVG']], 'SWWPD' -> ['Mois|SWWPD', [1, 42, 'AVG']]."""
    if isinstance(spec, str):
        return f"{spec}|{alias}"
    return [f"{spec[0]}|{alias}", *spec[1:]]


def sim_json(base: dict[str, Any], params_dir: Path, water_stress: bool, nitrogen_stress: bool) -> dict[str, Any]:
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
        "include-file-base-path": f"{params_dir}/",
        "path-to-output": "./",
        "WaterDeficitResponseOn": water_stress,
        "NitrogenResponseOn": nitrogen_stress,
    }
    daily = ["Date|DATE"] + [out_spec(k, v) for k, v in (DAILY | EXTRA_DAILY).items() if v is not None]
    events: list[Any] = ["daily", daily]
    for event, outs in EVENTS.items():
        events += [event, [out_spec(code, spec) for code, spec in outs]]
    sim["output"] = {
        "write-file?": False,
        "csv-options": {"include-header-row": True, "include-units-row": False, "csv-separator": ","},
        "obj-outputs?": True,
        "events": events,
    }
    return sim


def site_json(
    sheets: dict[str, pd.DataFrame], t: Treatment, base: dict[str, Any], layers: list[dict[str, Any]]
) -> dict[str, Any]:
    fld = sheets["Fields"]
    fld = fld[fld["FIELD_ID"] == t.row["FIELD_ID"]].iloc[0]
    station = sheets["Weather_stations"]
    station = station[station["WST_ID"] == t.row["wst_id"]].iloc[0]
    soil_meta = sheets["Soil_metadata"]
    soil_meta = soil_meta[soil_meta["SOIL_ID"] == t.one("Plots")["SOIL_ID"]].iloc[0]

    site = copy.deepcopy(base)
    sp = site["SiteParameters"]
    sp["Latitude"] = float(fld["FL_LAT"])
    sp["HeightNN"] = [float(fld["FLELE"]), "m"]
    sp["Slope"] = math.tan(math.radians(float(fld["FLSL"])))  # degree -> m m-1
    sp["NumberOfLayers"] = NO_OF_LAYERS
    sp["LayerThickness"] = [LAYER_THICKNESS_M, "m"]
    sp["SoilProfileParameters"] = layers
    site["EnvironmentParameters"] = {
        "Albedo": float(soil_meta["SALB"]),  # as in the Maricopa flow
        "AtmosphericCO2": float(station["CO2Y"]),
        "WindSpeedHeight": float(station["WNDHT"]),
    }
    return site


# ---------------------------------------------------------------------------------------------
# running and collecting
# ---------------------------------------------------------------------------------------------


def prepare(sheets: dict[str, pd.DataFrame], t: Treatment, run_dir: Path, args: argparse.Namespace) -> list[str]:
    run_dir.mkdir(parents=True, exist_ok=True)
    crop_template = json.loads((HERE / "crop.json").read_text())
    crop_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"] = [
        "include-from-file",
        args.cultivar,
    ]
    crop = {k: v for k, v in crop_template.items() if k not in ("ws", "cropRotation")}
    crop["cropRotation"] = [{"worksteps": worksteps(t, crop_template)}]

    layers, notes = soil_profile(sheets, t)
    site_base = json.loads((HERE / "site.json").read_text())
    sim_base = json.loads((HERE / "sim_full_run.json").read_text())

    write_climate(run_dir / "climate.csv", climate_rows(sheets, t, args.vp_factor))
    (run_dir / "crop.json").write_text(json.dumps(crop, indent=2))
    (run_dir / "site.json").write_text(json.dumps(site_json(sheets, t, site_base, layers), indent=2))
    (run_dir / "sim.json").write_text(
        json.dumps(sim_json(sim_base, args.params.resolve(), not args.no_water_stress, args.nitrogen_stress), indent=2)
    )
    return notes


def run_monica(monica_run: Path, run_dir: Path) -> None:
    res = subprocess.run([str(monica_run), "sim.json"], cwd=run_dir, capture_output=True, text=True, check=False)
    # monica-run prints the output sections to stdout
    (run_dir / "monica_out.csv").write_text(res.stdout)
    (run_dir / "monica.log").write_text(res.stderr)
    if res.returncode != 0 or not res.stdout.startswith('"'):
        msg = f"monica-run failed in {run_dir} (exit {res.returncode}), see monica.log:\n{res.stderr[-600:]}"
        raise RuntimeError(msg)


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
    sections = read_sections(run_dir / "monica_out.csv")
    daily, *event_sections = sections
    summary: dict[str, Any] = {}
    for (_event, outs), rows in zip(EVENTS.items(), event_sections, strict=True):
        if rows:
            # a winter crop can only ever have one of each per season; take the first
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


# ---------------------------------------------------------------------------------------------
# result files
# ---------------------------------------------------------------------------------------------


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
    return f"{round(x, DIGITS):g}" if abs(x) < 1e15 else str(x)


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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--xlsx", type=Path, default=HERE / "AMEI_Bushland_v4_2026-01-24(no_soil_T).xlsx")
    ap.add_argument("--site-name", default="Bushland")
    ap.add_argument("--treatments", nargs="*", help="only these (ids of the Treatments sheet)")
    ap.add_argument("--monica-run", type=Path, default=Path.home() / "GitHub/monica_amei/build/monica-run")
    ap.add_argument("--params", type=Path, default=HERE / "phase_1/params")
    ap.add_argument("--cultivar", default="crops/cultivar_winter-wheat.json", help="relative to --params")
    ap.add_argument("--module-code", default="MOFAPM")
    ap.add_argument("--kind", default="Orig", choices=["Orig", "Imp"])
    ap.add_argument("--out", type=Path, default=HERE / "out_bushland")
    ap.add_argument("--no-water-stress", action="store_true", help="WaterDeficitResponseOn = false")
    ap.add_argument("--nitrogen-stress", action="store_true", help="NitrogenResponseOn = true")
    ap.add_argument(
        "--vp-factor",
        type=float,
        default=1.0,
        help="VPRSD (kPa) is multiplied by this; 10 reproduces hPa as the flow sends it",
    )
    ap.add_argument("--prepare-only", action="store_true", help="write the MONICA inputs, don't run")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    sheets = read_sheets(args.xlsx)
    treatments = load_treatments(sheets)
    selected = [treatments[t] for t in args.treatments] if args.treatments else list(treatments.values())

    results_dir = args.out / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    templates = HERE / "3_Templates/Evaluation"
    _, (_, _, daily_codes) = template_header(templates / "AMEI_ET_template_Daily.csv")
    _, (_, _, summary_codes) = template_header(templates / "AMEI_ET_template_summary.csv")
    model_line = f"Model:\tMONICA ({args.module_code}, {args.monica_run.parent.parent.name})"

    summaries, failed = [], []
    for t in selected:
        run_dir = args.out / "runs" / t.id
        try:
            for note in prepare(sheets, t, run_dir, args):
                logger.info("%s: %s", t.id, note)
            if args.prepare_only:
                continue
            run_monica(args.monica_run, run_dir)
            daily, summary = collect(run_dir)
        except (ValueError, RuntimeError, OSError) as e:
            logger.error("%s: %s", t.id, e)  # noqa: TRY400 - the message is the useful part
            failed.append(t.id)
            continue

        rows = [[args.module_code, t.id, *(r.get(c) for c in daily_codes[2:])] for r in daily]
        write_result(
            results_dir / f"ETstudy{args.kind}{args.module_code}Daily{args.site_name}{t.id}.txt",
            templates / "AMEI_ET_template_Daily.csv",
            model_line,
            rows,
        )
        summaries.append([args.module_code, t.id, *(summary.get(c) for c in summary_codes[2:])])
        logger.info(
            "%s: sowing %s, emergence %s, anthesis %s, maturity %s, ET0 %s mm, ETa %s mm (season)",
            t.id,
            *(summary.get(k, "-") for k in ("PDATE", "PLDAE", "ADAT", "MDAT")),
            *(fmt(summary.get(k)) for k in ("ETO", "ETCM")),
        )

    if summaries:
        write_result(
            results_dir / f"ETstudy{args.kind}{args.module_code}Summary{args.site_name}.txt",
            templates / "AMEI_ET_template_summary.csv",
            model_line,
            summaries,
        )
    if failed:
        logger.error("failed: %s", ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    os.environ.setdefault("PYTHONWARNINGS", "ignore")
    sys.exit(main())
