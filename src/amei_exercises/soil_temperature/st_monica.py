"""MONICA runs for the AMEI soil temperature model intercomparison: what differs from the ET study.

The MONICA inputs (`Setup` -> sim.json, crop.json, site.json, climate.csv) come from the ET study's
amei_exercises.et_monica and the ICASA readers from amei_exercises.icasa. Here: the
soil temperature models, the outputs and the three AMEI result files (summary, daily, layers).
`run_st.py` is the command line.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from amei_exercises.et_monica import (
    DEFAULT_MONICA_RUN,
    EVENTS,
    LAYER_THICKNESS_M,
    SEASON,
    WEATHER_COLUMNS,
    Fertilization,
    Observations,
    Options,
    OrganicAddition,
    Setup,
    daily_obs,
    daily_outputs,
    monica_build,
    nan_to_none,
    number,
    output_events,
    prepare,
    read_sections,
    run_monica,
    template_header,
    to_date,
    write_checksums,
    write_result,
)
from amei_exercises.icasa import horizons, read_sheets, treatment_id_resolver, weather
from amei_exercises.paths import ET_DIR, MARICOPA_DIR, ST_DIR

__all__ = [
    "daily_obs",
    "DEFAULT_MONICA_RUN",
    "ET_DIR",
    "MARICOPA_DIR",
    "ST_DIR",
    "WEATHER_COLUMNS",
    "Fertilization",
    "Observations",
    "OrganicAddition",
    "Options",
    "Setup",
    "horizons",
    "monica_build",
    "nan_to_none",
    "prepare",
    "read_sheets",
    "run_monica",
    "template_header",
    "to_date",
    "treatment_id_resolver",
    "weather",
    "write_checksums",
    "write_result",
]

FRAMEWORK_ID = "MO"

# the protocol's 2-letter model code -> Modules.SoilTemperature.impl of the Odin MONICA
MODELS = {
    "AP": "ApsimCampbell",
    "PS": "BiomaSurfacePartonSoilSWATC",
    "SW": "BiomaSurfaceSWATSoilSWATC",
    "DE": "DSSAT_EPICST_standalone",
    "DS": "DSSAT_ST_standalone",
    "MO": "Monica_SoilTemp",  # MONICA's soil temperature as Crop2ML module
    "SA": "Simplace_Soil_Temperature",
    "SQ": "SQ_Soil_Temperature",
    "ST": "Stics_soil_temperature",
    # not part of the protocol: MONICA's own implementation, to compare MO with
    "iMO": "internal",
}

# the AMEI templates are Windows-1252 (°C, m²/m²) and want "na" for what a model doesn't simulate
TEMPLATE_ENCODING = "cp1252"
NA = "na"

# MONICA's development stage -> Zadoks, as in the Maricopa flow's consumer (stage 3 has no single
# Zadoks stage): sowing 0, emergence 9, (stem elongation), heading 51, anthesis 65, maturity 89
STAGE_TO_ZADOKS = {1: 0, 2: 9, 4: 51, 5: 65, 6: 89}


def model_options(code: str, base: Options, no_of_layers: int) -> Options:
    """`base` with the soil temperature model `code` and this study's outputs."""
    impl = MODELS[code]
    return Options(
        params=base.params,
        water_stress=base.water_stress,
        nitrogen_stress=base.nitrogen_stress,
        vp_factor=base.vp_factor,
        modules=None if impl == "internal" else {"SoilTemperature": {"impl": impl}},
        events=st_events(no_of_layers),
    )


def st_daily(no_of_layers: int) -> dict[str, Any]:
    """Template code -> MONICA output spec (None: written as na) of the daily canopy/profile file."""
    d = daily_outputs(no_of_layers)
    return {
        "LNUM": None,
        "GSTZD": None,  # from Stage, see collect
        **{c: d[c] for c in ("LAID", "LIPCD", "CWAD", "GWAD", "GWGD", "CNAD", "GNAD", "GNGD", "RDPD")},
        **{c: d[c] for c in ("SWWPD", "DRND", "ROFD")},
        "NIAD": None,  # the sum of the layers' NO3 + NH4, see collect
        **{c: d[c] for c in ("NLCD", "NMND", "N2OED", "NIMD", "NDND", "GHFD", "LHFD", "HHFD", "RND")},
        # unrounded (the SurfTemp alias rounds to 0.1 °C)
        "TSSAV": "soil_column.soil_surface_temperature",
        "TSSMX": None,
        "TSSMN": None,
        "TGAV": None,
        "TGMX": None,
        "TGMN": None,
        **{c: d[c] for c in ("EOAD", "ETAD", "EPSAD", "ESAD", "EPPAD", "EPAD")},
    }


# per-layer outputs: column prefix in monica_out.csv -> spec. Without a display name, MONICA names
# the columns <prefix>_1 .. <prefix>_n.
def st_layers(no_of_layers: int) -> dict[str, Any]:
    n = no_of_layers
    return {
        # unrounded (the STemp alias rounds to 0.1 °C)
        "soil_column.layers.soil_temperature": ["soil_column.layers.soil_temperature", [1, n]],
        "Mois": ["Mois", [1, n]],  # m3 m-3, 3 digits
        "NO3": ["NO3", [1, n]],  # kg N m-3, 6 digits
        "NH4": ["NH4", [1, n]],
    }


EXTRA_DAILY = {"Stage": "Stage", "Precip": "Precip", "Tavg": "Tavg", "ETO": "ET0"}


def st_events(no_of_layers: int) -> list[Any]:
    events = output_events(st_daily(no_of_layers) | EXTRA_DAILY)
    events[1] += list(st_layers(no_of_layers).values())
    return events


def layer_columns(row: dict[str, str], prefix: str, n: int) -> list[float | None]:
    return [number(row.get(f"{prefix}_{i}")) for i in range(1, n + 1)]


def collect(run_dir: Path, no_of_layers: int) -> tuple[list[dict[str, Any]], list[list[Any]], dict[str, Any]]:
    """(daily rows by template code, layer rows [date, top, bottom, TSAV, TSMX, TSMN, SWLD, SNLD],
    summary) of one run."""
    n = no_of_layers
    thickness_cm = LAYER_THICKNESS_M * 100
    kg_ha_per_kg_m3 = 10000 * LAYER_THICKNESS_M
    daily, *event_sections = read_sections(run_dir / "monica_out.csv")

    layer_rows: list[list[Any]] = []
    for r in daily:
        temps = layer_columns(r, "soil_column.layers.soil_temperature", n)
        mois = layer_columns(r, "Mois", n)
        no3, nh4 = layer_columns(r, "NO3", n), layer_columns(r, "NH4", n)
        n_kg_ha = [None if a is None or b is None else (a + b) * kg_ha_per_kg_m3 for a, b in zip(no3, nh4, strict=True)]
        for i in range(n):
            layer_rows.append(
                [r["DATE"], i * thickness_cm, (i + 1) * thickness_cm, temps[i], None, None, mois[i], n_kg_ha[i]]
            )
        if all(x is not None for x in n_kg_ha):
            r["NIAD"] = sum(n_kg_ha)  # type: ignore[arg-type]
        stage = number(r.get("Stage"))
        r["GSTZD"] = None if stage is None else STAGE_TO_ZADOKS.get(int(stage))
        if (x := number(r.get("RND"))) is not None:
            r["RND"] = x * 1e6 / 86400  # MJ m-2 d-1 -> W m-2 (daily mean)

    summary: dict[str, Any] = {}
    for (_event, outs), rows in zip(EVENTS.items(), event_sections, strict=True):
        if rows:
            summary |= {code: rows[0].get(code) for code, _ in outs}
    days = [r["DATE"] for r in daily]
    start = summary.get("PDATE") or days[0]
    end = summary.get("MDAT") or days[-1]
    season = [r for r in daily if start <= r["DATE"] <= end]
    for code, (dcode, how) in SEASON.items():
        values = [x for r in season if (x := number(r.get(dcode))) is not None]
        if values:
            summary[code] = max(values) if how == "max" else sum(values)
    return daily, layer_rows, summary


def result_names(model_code: str, site_name: str) -> tuple[str, str, str]:
    """Summary file name, daily and layers file name patterns with {id} (protocol's naming rule)."""
    prefix = f"{model_code}{FRAMEWORK_ID}"
    return (
        f"{prefix}Summary{site_name}.txt",
        f"{prefix}Daily{site_name}{{id}}.txt",
        f"{prefix}Layers{site_name}{{id}}.txt",
    )
