"""Run MONICA on the treatments of an ET study site and write the AMEI evaluation templates.

Phase 1 of the AgMIP Wheat ET2 study, evaluation step; default: original model, MONICA FAO56-PM
(MOFAPM).

Per treatment, everything MONICA gets is written to <out>/runs/<id>/ (sim.json, crop.json,
site.json, climate.csv) together with what it returns (monica_out.csv, monica.log), so a single run
can be inspected and repeated by hand:

    cd evapotranspiration/out_bushland/runs/TXBL1990F && monica-run sim.json

The template files go to <out>/results/.

    pixi run run-et --site bushland [--treatments TXBL1990F ...]
    pixi run run-et --site avignon

--soil-moisture puts parameters on top of general/soil-moisture.json, e.g. Odin's
MaxPotentialEvapotranspiration=15, PondEvaporationByCoverage=true, InfiltrationByDepth=true,
InfiltrationDepth=0.1 (values as JSON).
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path

from amei_exercises.et_monica import (
    DEFAULT_MONICA_RUN,
    TEMPLATES,
    Options,
    collect,
    fmt,
    monica_build,
    prepare,
    result_names,
    run_monica,
    template_header,
    write_checksums,
    write_result,
)
from amei_exercises.paths import ET_DIR

logger = logging.getLogger("run_et")
SITES = ("bushland", "avignon")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", choices=SITES, required=True)
    ap.add_argument("--xlsx", type=Path, help="default: the site's workbook in evapotranspiration/")
    ap.add_argument("--treatments", nargs="*", help="only these")
    ap.add_argument("--monica-run", type=Path, default=DEFAULT_MONICA_RUN, help="default: the Odin port")
    ap.add_argument("--params", type=Path, default=ET_DIR / "phase_1/params")
    ap.add_argument("--cultivar", help="include path relative to --params; default: the site's")
    ap.add_argument("--species", help="include path relative to --params; default: crop.json's")
    ap.add_argument("--module-code", default="MOFAPM")
    ap.add_argument("--kind", default="Orig", choices=["Orig", "Imp"])
    ap.add_argument("--out", type=Path, help="default: evapotranspiration/out_<site>")
    ap.add_argument("--no-water-stress", action="store_true", help="WaterDeficitResponseOn = false")
    ap.add_argument(
        "--nitrogen-stress",
        action=argparse.BooleanOptionalAction,
        help="NitrogenResponseOn; default: the site's (NITROGEN_STRESS)",
    )
    ap.add_argument("--vp-factor", type=float, default=1.0, help="vapor pressure (kPa) is multiplied by this")
    ap.add_argument(
        "--soil-moisture",
        nargs="*",
        default=[],
        metavar="KEY=VALUE",
        help="soil moisture parameters on top of general/soil-moisture.json (VALUE as JSON)",
    )
    ap.add_argument("--prepare-only", action="store_true", help="write the MONICA inputs, don't run")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    site = importlib.import_module(f"amei_exercises.evapotranspiration.site_{args.site}")
    out: Path = args.out or ET_DIR / f"out_{args.site}"
    setups = site.load(args.xlsx or site.XLSX)
    if args.treatments:
        known = {s.id for s in setups}
        if unknown := set(args.treatments) - known:
            logger.error("unknown treatment(s) %s, known: %s", sorted(unknown), sorted(known))
            return 1
        setups = [s for s in setups if s.id in args.treatments]
    for s in setups:
        s.cultivar = args.cultivar or s.cultivar
        s.species = args.species or s.species
    try:
        soil_moisture = {k: json.loads(v) for k, v in (kv.split("=", 1) for kv in args.soil_moisture)}
    except ValueError as e:
        logger.error("--soil-moisture: %s", e)  # noqa: TRY400 - the message is the useful part
        return 1
    if soil_moisture:
        for s in setups:
            s.site_params["SoilMoistureParameters"] = s.site_params.get("SoilMoistureParameters", {}) | soil_moisture

    opts = Options(
        params=args.params,
        water_stress=not args.no_water_stress,
        nitrogen_stress=getattr(site, "NITROGEN_STRESS", False)
        if args.nitrogen_stress is None
        else args.nitrogen_stress,
        vp_factor=args.vp_factor,
    )
    results_dir = out / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    _, (_, _, daily_codes) = template_header(TEMPLATES / "AMEI_ET_template_Daily.csv")
    _, (_, _, summary_codes) = template_header(TEMPLATES / "AMEI_ET_template_summary.csv")
    summary_name, daily_name = result_names(args.kind, args.module_code, site.NAME)
    build = monica_build(args.monica_run)
    model_line = f"Model:\tMONICA ({args.module_code}, {build})"

    summaries, failed = [], []
    for s in setups:
        run_dir = out / "runs" / s.id
        try:
            for note in prepare(s, run_dir, opts):
                logger.info("%s: %s", s.id, note)
            if args.prepare_only:
                continue
            run_monica(args.monica_run, run_dir, build)
            daily, summary = collect(run_dir)
        except (ValueError, RuntimeError, OSError) as e:
            logger.error("%s: %s", s.id, e)  # noqa: TRY400 - the message is the useful part
            failed.append(s.id)
            continue

        rows = [[args.module_code, s.id, *(r.get(c) for c in daily_codes[2:])] for r in daily]
        write_result(
            results_dir / daily_name.format(id=s.id), TEMPLATES / "AMEI_ET_template_Daily.csv", model_line, rows
        )
        summaries.append([args.module_code, s.id, *(summary.get(c) for c in summary_codes[2:])])
        logger.info(
            "%s: sowing %s, emergence %s, anthesis %s, maturity %s, ET0 %s mm, ETa %s mm (season)",
            s.id,
            *(summary.get(k, "-") for k in ("PDATE", "PLDAE", "ADAT", "MDAT")),
            *(fmt(summary.get(k)) for k in ("ETO", "ETCM")),
        )

    if not args.prepare_only:
        write_checksums(out, ["runs/*/monica_out.csv"])
    if summaries:
        write_result(results_dir / summary_name, TEMPLATES / "AMEI_ET_template_summary.csv", model_line, summaries)
    if failed:
        logger.error("failed: %s", ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
