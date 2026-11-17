"""Run MONICA with each AMEI soil temperature model on the treatments of a site and write the
protocol's result files: one summary per model, one daily and one layers file per model and
treatment.

Per model and treatment, everything MONICA gets is written to <out>/runs/<model>/<id>/ (sim.json,
crop.json, site.json, climate.csv) together with what it returns (monica_out.csv, monica.log) and
the MONICA build (monica_build.txt), so a single run can be inspected and repeated by hand:

    cd soil_temperature/out_maricopa/runs/DS/901 && monica-run sim.json

The result files go to <out>/results/. git ignores monica_out.csv and the layers results (too big);
<out>/checksums.txt holds their sha256, so `git diff` shows which changed after a rerun.

    pixi run python soil_temperature/run_st.py --site maricopa [--models MO DS] [--treatments 901 902]
    pixi run python soil_temperature/run_st.py --site bushland
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from st_monica import (
    DEFAULT_MONICA_RUN,
    FRAMEWORK_ID,
    HERE,
    MODELS,
    NA,
    TEMPLATE_ENCODING,
    Options,
    collect,
    model_options,
    monica_build,
    prepare,
    result_names,
    run_monica,
    st_daily,
    template_header,
    write_checksums,
    write_result,
)

logger = logging.getLogger("run_st")
SITES = ("maricopa", "bushland")


def modeler_name() -> str:
    res = subprocess.run(["git", "config", "user.name"], capture_output=True, text=True, check=False)
    return res.stdout.strip()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", choices=SITES, required=True)
    ap.add_argument("--xlsx", type=Path, help="default: the site's workbook")
    ap.add_argument("--treatments", nargs="*", help="only these")
    ap.add_argument("--models", nargs="*", choices=list(MODELS), help="default: all")
    ap.add_argument("--monica-run", type=Path, default=DEFAULT_MONICA_RUN, help="default: the Odin port")
    ap.add_argument("--params", type=Path, help="default: the site's")
    ap.add_argument("--cultivar", help="include path relative to --params; default: the site's")
    ap.add_argument("--out", type=Path, help="default: soil_temperature/out_<site>")
    ap.add_argument("--modeler", default=modeler_name(), help="for the Modeler_name line; default: git user.name")
    ap.add_argument("--jobs", type=int, default=os.cpu_count(), help="MONICA runs in parallel")
    ap.add_argument("--no-water-stress", action="store_true", help="WaterDeficitResponseOn = false")
    ap.add_argument(
        "--nitrogen-stress",
        action=argparse.BooleanOptionalAction,
        help="NitrogenResponseOn; default: the site's",
    )
    ap.add_argument("--prepare-only", action="store_true", help="write the MONICA inputs, don't run")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    site = importlib.import_module(f"site_{args.site}")
    out: Path = args.out or HERE / f"out_{args.site}"
    setups = site.load(args.xlsx or site.XLSX)
    if args.treatments:
        known = {s.id for s in setups}
        if unknown := set(args.treatments) - known:
            logger.error("unknown treatment(s) %s, known: %s", sorted(unknown), sorted(known))
            return 1
        setups = [s for s in setups if s.id in args.treatments]
    if args.cultivar:
        for s in setups:
            s.cultivar = args.cultivar
    for s in setups:
        for note in s.notes:
            logger.info("%s: %s", s.id, note)

    base = Options(
        params=args.params or site.PARAMS,
        water_stress=not args.no_water_stress,
        nitrogen_stress=site.NITROGEN_STRESS if args.nitrogen_stress is None else args.nitrogen_stress,
    )
    models = args.models or list(MODELS)
    jobs = [(m, s) for m in models for s in setups]

    run_dirs = {}
    for m, s in jobs:
        run_dirs[(m, s.id)] = out / "runs" / m / s.id
        notes = prepare(s, run_dirs[(m, s.id)], model_options(m, base, s.no_of_layers))
        if m == models[0]:  # the same for every model; the site's own were logged above
            for note in notes[len(s.notes) :]:
                logger.info("%s: %s", s.id, note)
    if args.prepare_only:
        return 0

    build = monica_build(args.monica_run)

    def run(job: tuple[str, object]) -> str | None:
        m, s = job
        try:
            run_monica(args.monica_run, run_dirs[(m, s.id)], build)  # type: ignore[attr-defined]
        except RuntimeError as e:
            return str(e)
        return None

    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        errors = dict(zip([(m, s.id) for m, s in jobs], pool.map(run, jobs), strict=True))

    results_dir = out / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    templates = site.TEMPLATES
    _, (_, _, daily_codes) = template_header(templates["daily"], TEMPLATE_ENCODING)
    _, (_, _, summary_codes) = template_header(templates["summary"], TEMPLATE_ENCODING)
    failed = []
    for m in models:
        summary_name, daily_name, layers_name = result_names(m, site.NAME)
        model_line = f"Model:\tMONICA ({MODELS[m]}, {build})"
        write = {
            "model_line": model_line,
            "na": NA,
            "encoding": TEMPLATE_ENCODING,
            "preamble_lines": {"Modeler_name:": f"Modeler_name:\t{args.modeler}"},
            "title": site.TITLE,
            "min_preamble_lines": 4,  # data from line 8, as the checklist wants
        }
        summaries = []
        for s in setups:
            if err := errors[(m, s.id)]:
                logger.error("%s %s: %s", m, s.id, err)
                failed.append(f"{m}/{s.id}")
                continue
            daily, layers, summary = collect(run_dirs[(m, s.id)], s.no_of_layers)
            ids = [FRAMEWORK_ID, m, s.id]
            write_result(
                results_dir / daily_name.format(id=s.id),
                templates["daily"],
                rows=[[*ids, r["DATE"], *(r.get(c) for c in daily_codes[4:])] for r in daily],
                **write,
            )
            write_result(
                results_dir / layers_name.format(id=s.id),
                templates["layers"],
                rows=[[*ids, *r] for r in layers],
                **write,
            )
            summaries.append([*ids, *(summary.get(c) for c in summary_codes[3:])])
        if summaries:
            write_result(results_dir / summary_name, templates["summary"], rows=summaries, **write)
        logger.info("%s (%s): %d of %d treatments", m, MODELS[m], len(summaries), len(setups))

    write_checksums(out, ["runs/*/*/monica_out.csv", "results/*Layers*.txt"])
    unused = set(st_daily(site.NO_OF_LAYERS)) - set(daily_codes)
    if unused:
        logger.warning("outputs not in the daily template: %s", sorted(unused))
    if failed:
        logger.error("failed: %s", ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
