"""MONICA's soil temperature runs against the other frameworks' AMEI submissions (Maricopa, Bushland).

The same 9 soil temperature models run in several frameworks (MO = MONICA, DC = DSSAT, SQ = SiriusQuality),
each with its own crop, water balance and layering. Neither is the reference: this shows where MONICA's
drivers (crop, ET, soil water) and the resulting soil temperatures stand among them.

    pixi run compare-frameworks [--site maricopa|bushland] [--monica DIR] [--other DIR] [--out DIR]

The other frameworks: one folder each in <other> (default soil_temperature/out_<site>_other_models),
named <framework>_<date> (e.g. DSSAT_2026-09-17, SiriusQuality_2026-03-12; the latest date if several),
with the Daily, Layers and Summary files like MONICA's soil_temperature/out_<site>/results (the default
of --monica). Writes into <out> (default soil_temperature/out_<site>/framework_comparison): crop.csv,
water.csv, soil_temperature.csv, soil_temperature_spread.csv and figures.
"""

from __future__ import annotations

import argparse
import logging
import math
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from amei_exercises.et_monica import Observations, daily_obs, nan_to_none, to_date
from amei_exercises.evapotranspiration.site_bushland import zadoks_date
from amei_exercises.icasa import read_sheets, treatment_id_resolver
from amei_exercises.paths import ST_DIR
from amei_exercises.soil_temperature import site_bushland, site_maricopa
from amei_exercises.soil_temperature.evaluate_st import read_layers

FRAMEWORKS = {"MO": "MONICA", "DC": "DSSAT", "SQ": "SiriusQuality"}
FOLDER_FRAMEWORK = {name: fw for fw, name in FRAMEWORKS.items()}  # the other frameworks' folder names
MODELS = ["AP", "DE", "DS", "MO", "PS", "SA", "SQ", "ST", "SW"]  # the 9 AMEI models (not MONICA's iMO)
# the soil temperature ensembles (medians, spreads, figures): all 9, MO with MONICA's default lower
# boundary (BaseTemperature 9.5 °C) in all frameworks (st_monica.BASE_TEMPERATURE_FROM_TAV off; with the
# site's TAV leave MO out here)
ENSEMBLE = MODELS
# DSSAT's layers, the coarsest: the others' 5 cm layers are averaged onto them
LAYERS = [(0, 5), (5, 15), (15, 30), (30, 45), (45, 60), (60, 90), (90, 120), (120, 150), (150, 180), (180, 210)]

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#1f1f1e", "#5f5e58", "#e6e5e0"
COLOR = {"MO": "#2a78d6", "DC": "#eb6834", "SQ": "#1baf7a"}  # categorical slots 1-3


@dataclass
class Site:
    name: str  # as in the file names (APMODailyMaricopa901.txt)
    observations: Callable[[], dict[str, Observations]]  # TREAT_ID -> observations
    soil_water_cm: tuple[int, int]  # the layer of Observations.soil_water: top, bottom
    treat_ids: dict[str, str] = field(default_factory=dict)  # the other frameworks' TREAT_ID -> ours


def maricopa_observations() -> dict[str, Observations]:
    return {s.id: s.obs for s in site_maricopa.load()}


BUSHLAND_SOIL_WATER_CM = (20, 180)  # the lysimeters' neutron probe: 10-170 cm every 20 cm


def bushland_observations() -> dict[str, Observations]:
    """The soil temperature study's workbook: its summary observations as in the runs, plus daily ET,
    LAI and biomass, anthesis from the daily Zadoks stages (GSTZD 61) where ADAT is missing (as in the ET
    study), and the lysimeters' soil water [mm] in BUSHLAND_SOIL_WATER_CM."""
    sheets = read_sheets(site_bushland.XLSX)
    treatments = {str(r["TREAT_ID"]): r for r in sheets["Treatments"].to_dict("records")}
    resolve = treatment_id_resolver(treatments)
    rows: dict[str, dict[str, list[dict]]] = {t: {} for t in treatments}
    for name in ("Daily_ET", "Obs_crop_daily_means", "Soil_moisture_lysimeter"):
        for r in sheets[name].to_dict("records"):
            if (tid := resolve(str(r["TREAT_ID"]).strip())) is not None:
                rows[tid].setdefault(name, []).append(r)

    top, bottom = BUSHLAND_SOIL_WATER_CM
    wanted = set(range(top + 10, bottom, 20))
    out = {}
    for s in site_bushland.load():
        o, r = s.obs, rows[s.id]
        crop = r.get("Obs_crop_daily_means", [])
        by_date: dict = {}
        for m in r.get("Soil_moisture_lysimeter", []):
            if (v := nan_to_none(m.get("SWLD"))) is not None and int(m["SLSD"]) in wanted:
                by_date.setdefault(to_date(m["DATE"]), {})[int(m["SLSD"])] = float(v)
        out[s.id] = Observations(
            et=daily_obs(r.get("Daily_ET", []), "ETAD"),
            emergence=o.emergence,
            anthesis=o.anthesis or zadoks_date(crop, 61),
            maturity=o.maturity,
            lai_max=o.lai_max,
            grain_yield=o.grain_yield,
            lai=daily_obs(crop, "LAID"),
            biomass=daily_obs(crop, "CWAD"),
            soil_water=pd.Series(
                {d: sum(v.values()) * 200 for d, v in by_date.items() if set(v) == wanted}, dtype=float
            ).sort_index(),
        )
    return out


SITES = {
    "maricopa": Site("Maricopa", maricopa_observations, site_maricopa.SOIL_WATER_DEPTHS_CM),
    "bushland": Site(
        "Bushland",
        bushland_observations,
        BUSHLAND_SOIL_WATER_CM,
        # SiriusQuality numbers the treatments in the Treatments sheet's order (the seasons' years and
        # F > P in ET confirm it)
        treat_ids={"501": "TXBL1990F", "502": "TXBL1990P", "503": "TXBL1992F",
                   "504": "TXBL1992P", "505": "TXBL1993F", "506": "TXBL1993P"},  # fmt: skip
    ),
}


def framework_dirs(monica: Path, other: Path) -> dict[str, Path]:
    """MO -> monica; the others' folders <framework>_<date> in `other`, the latest date per framework."""
    dirs = {"MO": monica}
    for d in sorted(p for p in other.iterdir() if p.is_dir()):  # the dates sort: the latest last
        fw = FOLDER_FRAMEWORK.get(d.name.rsplit("_", 1)[0])
        if fw is None:
            logging.getLogger("compare_frameworks").warning("%s: no known framework, skipped", d)
            continue
        dirs[fw] = d
    return dirs


def load_all(dirs: dict[str, Path], kind: str, site: Site) -> pd.DataFrame:
    """All models' Daily or Layers files of all frameworks, long, with columns framework and model."""
    frames = []
    for fw, folder in dirs.items():
        pattern = re.compile(rf"^(?P<model>[A-Z]+?){fw}{kind}{site.name}(?P<t>\w+)\.txt$")
        for f in sorted(folder.glob(f"*{fw}{kind}{site.name}*.txt")):
            m = pattern.match(f.name)
            if m is None or m["model"] not in MODELS:
                continue
            df = read_layers(f)
            df["TREAT_ID"] = df["TREAT_ID"].astype(str).replace(site.treat_ids)
            frames.append(df.assign(framework=fw, model=m["model"]))
    return pd.concat(frames, ignore_index=True)


def load_summaries(dirs: dict[str, Path], site: Site) -> dict[tuple[str, str, str], dict]:
    """(framework, model, TREAT_ID) -> {ADAT, MDAT, LAIX} of the Summary files: one per model
    (<model><fw>Summary<site>.txt) or one for all (<fw>Summary<site>.txt, by MODEL_ID)."""
    out = {}
    for fw, folder in dirs.items():
        for f in sorted(folder.glob(f"*{fw}Summary{site.name}.txt")):
            lines = f.read_text(encoding="cp1252").splitlines()
            code_row = next(i for i, line in enumerate(lines) if line.startswith("FRAMEWORK_ID"))
            df = pd.read_csv(f, sep="\t", skiprows=code_row, na_values=["na", "NA"], encoding="cp1252", dtype=str)
            df.columns = [c.strip() for c in df.columns]
            for r in df.to_dict("records"):
                model = str(r["MODEL_ID"]).strip()
                if model not in MODELS:
                    continue
                v = {c: pd.to_datetime(str(r.get(c)).strip(), errors="coerce") for c in ("ADAT", "MDAT")}
                v = {c: None if pd.isna(d) else d for c, d in v.items()}
                v["LAIX"] = pd.to_numeric(str(r.get("LAIX")).strip(), errors="coerce")
                tid = str(r["TREAT_ID"]).strip()
                out[(fw, model, site.treat_ids.get(tid, tid))] = v
    return out


def onto_layers(layers: pd.DataFrame, column: str) -> pd.DataFrame:
    """Thickness weighted means of `column` on LAYERS: framework, model, TREAT_ID, DATE, top, value."""
    out = []
    for top, bottom in LAYERS:
        lo = layers["SLLT"].clip(lower=top)
        hi = layers["SLLB"].clip(upper=bottom)
        w = (hi - lo).clip(lower=0)
        sel = layers.assign(w=w, wv=w * layers[column])[w > 0]
        g = sel.groupby(["framework", "model", "TREAT_ID", "DATE"])[["w", "wv"]].sum()
        g = g[g["w"] >= (bottom - top) * 0.99]  # the whole layer covered
        out.append((g["wv"] / g["w"]).rename("value").reset_index().assign(top=top, bottom=bottom))
    return pd.concat(out, ignore_index=True)


def water_mm(layers: pd.DataFrame, top: float, bottom: float) -> pd.DataFrame:
    """Soil water [mm] between top and bottom [cm]."""
    lo = layers["SLLT"].clip(lower=top)
    hi = layers["SLLB"].clip(upper=bottom)
    w = (hi - lo).clip(lower=0) * 10  # mm of soil
    sel = layers.assign(mm=w * layers["SWLD"])[w > 0]
    return sel.groupby(["framework", "model", "TREAT_ID", "DATE"])["mm"].sum().reset_index()


def common_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Only the days all frameworks simulated (per treatment)."""
    days = df.groupby(["TREAT_ID", "framework"])["DATE"].agg(["min", "max"]).reset_index()
    span = days.groupby("TREAT_ID").agg(first=("min", "max"), last=("max", "min"))
    df = df.join(span, on="TREAT_ID")
    return df[(df["DATE"] >= df["first"]) & (df["DATE"] <= df["last"])].drop(columns=["first", "last"])


def first_date(d: pd.DataFrame, zadoks: float) -> pd.Timestamp | None:
    hit = d.loc[d["GSTZD"] >= zadoks, "DATE"]
    return hit.min() if len(hit) else None


def crop_table(daily: pd.DataFrame, summaries: dict, obs: dict[str, Observations]) -> pd.DataFrame:
    """Per treatment and framework: anthesis, maturity, LAI max, biomass max, yield, ET, drainage;
    the median over the 9 models (they differ a little: soil temperature acts on the crop). Anthesis and
    maturity from the Summary files (ADAT, MDAT), else the first day of Zadoks 65 and 89. LAI max from
    the daily LAID, LAIX (Summary) as reported."""
    rows = []
    for (fw, tid), d in daily.groupby(["framework", "TREAT_ID"]):
        per_model = []
        for model, dm in d.groupby("model"):
            dm = dm.sort_values("DATE")
            sm = summaries.get((fw, model, tid), {})
            a, m = sm.get("ADAT") or first_date(dm, 65), sm.get("MDAT") or first_date(dm, 89)
            per_model.append({
                "anthesis": a, "maturity": m,
                "LAI max": dm["LAID"].max(), "LAIX (Summary)": sm.get("LAIX"), "biomass max": dm["CWAD"].max(), "yield": dm["GWAD"].max(),
                "season days": len(dm), "first day": dm["DATE"].min(), "last day": dm["DATE"].max(),
            })  # fmt: skip
        pm = pd.DataFrame(per_model)
        row = {"TREAT_ID": tid, "framework": fw}
        for c in ["anthesis", "maturity", "first day", "last day"]:
            v = pm[c].dropna()
            row[c] = v.sort_values().iloc[len(v) // 2].date() if len(v) else None
        for c in ["LAI max", "LAIX (Summary)", "biomass max", "yield", "season days"]:
            row[c] = round(pm[c].median(), 2 if c.startswith("LAI") else 0)
        rows.append(row)
    for tid, o in obs.items():
        rows.append({
            "TREAT_ID": tid, "framework": "obs", "anthesis": o.anthesis, "maturity": o.maturity,
            "LAI max": o.lai_max, "biomass max": o.biomass.max() if o.biomass is not None and len(o.biomass) else None,
            "yield": o.grain_yield,
        })  # fmt: skip
    return pd.DataFrame(rows).sort_values(["TREAT_ID", "framework"])


def water_table(daily: pd.DataFrame, sw: pd.DataFrame, obs: dict[str, Observations], sw_cm: tuple[int, int]) -> pd.DataFrame:
    """ET against the measured (period means, else daily values) and soil water in sw_cm against the
    measured, per framework (median over the models), only on days all frameworks simulated."""
    et = daily.groupby(["framework", "TREAT_ID", "DATE"])[["ETAD", "DRND"]].median().reset_index()
    swm = sw.groupby(["framework", "TREAT_ID", "DATE"])["mm"].median()
    rows = []
    for (fw, tid), d in et.groupby(["framework", "TREAT_ID"]):
        d = d.set_index("DATE")
        o = obs[tid]
        periods = o.et_periods or [(day, day, mm_d) for day, mm_d in o.et.items()]
        res = []
        for first, last, mm_d in periods:
            sim = d.loc[str(first) : str(last), "ETAD"]
            if len(sim) == (pd.Timestamp(last) - pd.Timestamp(first)).days + 1:  # the whole period simulated
                res.append((sim.mean() - mm_d, len(sim)))
        sw_res = []
        if o.soil_water is not None and (fw, tid) in swm.index.droplevel(2):
            s = swm.loc[(fw, tid)]
            for day, mm in o.soil_water.items():
                if pd.Timestamp(day) in s.index:
                    sw_res.append(s.loc[pd.Timestamp(day)] - mm)
        days = sum(n for _, n in res)
        rows.append({
            "TREAT_ID": tid, "framework": fw, "ET sum mm": round(d["ETAD"].sum()), "drainage mm": round(d["DRND"].sum()),
            "ET bias mm/d": round(sum(r * n for r, n in res) / days, 2) if days else None,
            "ET RMSE mm/d (periods)": round(math.sqrt(sum(r * r * n for r, n in res) / days), 2) if days else None,
            "ET periods": len(res),
            f"soil water {sw_cm[0]}-{sw_cm[1]} cm bias mm": round(sum(sw_res) / len(sw_res)) if sw_res else None,
            "soil water dates": len(sw_res),
        })  # fmt: skip
    return pd.DataFrame(rows).sort_values(["TREAT_ID", "framework"])


def temperature_tables(t: pd.DataFrame, frameworks: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Each model in the other frameworks against itself in MONICA, per layer; and the spread: between the
    models within a framework against between the frameworks for the same model."""
    others = [fw for fw in frameworks if fw != "MO"]
    wide = t.pivot_table(index=["model", "TREAT_ID", "DATE", "top", "bottom"], columns="framework", values="value")
    wide = wide.dropna().reset_index()
    rows = []
    for (model, top, bottom), d in wide.groupby(["model", "top", "bottom"]):
        for fw in others:
            diff = d[fw] - d["MO"]
            rows.append({
                "layer cm": f"{top}-{bottom}", "top": top, "model": model, "framework": fw,
                "mean diff to MONICA C": round(diff.mean(), 2), "RMSD C": round(math.sqrt((diff**2).mean()), 2),
            })  # fmt: skip
    vs_monica = pd.DataFrame(rows)

    spread = []
    wide = wide[wide["model"].isin(ENSEMBLE)]
    for (top, bottom), d in wide.groupby(["top", "bottom"]):
        # between models: SD over the ENSEMBLE models per framework, treatment, day; mean over all
        between_models = {fw: d.groupby(["TREAT_ID", "DATE"])[fw].std().mean() for fw in frameworks}
        between_fw = d[frameworks].std(axis=1).mean()
        med = d.groupby(["TREAT_ID", "DATE"])[frameworks].median()
        spread.append({
            "layer cm": f"{top}-{bottom}",
            **{f"SD between models, {FRAMEWORKS[fw]}": round(v, 2) for fw, v in between_models.items()},
            "SD between frameworks, same model": round(between_fw, 2),
            **{f"median {FRAMEWORKS[fw]} - MONICA": round((med[fw] - med["MO"]).mean(), 2) for fw in others},
        })  # fmt: skip
    return vs_monica, pd.DataFrame(spread)


def style(ax: plt.Axes) -> None:
    ax.set_facecolor(SURFACE)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    locator = mdates.AutoDateLocator(minticks=3, maxticks=5)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))


def ncols(n: int) -> int:
    return 3 if n <= 6 else 4


def panels(title: str, n: int) -> tuple[plt.Figure, list[plt.Axes]]:
    plt.rcParams.update({"font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK_2,
                         "xtick.color": INK_2, "ytick.color": INK_2})  # fmt: skip
    cols = ncols(n)
    rows = math.ceil(n / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(4.2 * cols, 2.8 * rows), facecolor=SURFACE, squeeze=False)
    for ax in axes.flat[n:]:
        ax.set_visible(False)
    height = fig.get_figheight()
    fig.suptitle(title, x=0.01, y=1 - 0.12 / height, ha="left", va="top", fontsize=12)
    return fig, list(axes.flat[:n])


def finish(fig: plt.Figure, axes: list[plt.Axes], unit: str, path: Path) -> None:
    for i, ax in enumerate(axes):
        if i % ncols(len(axes)) == 0:
            ax.set_ylabel(unit)
    height = fig.get_figheight()
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.005, 1 - 0.42 / height), ncol=5, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.8 / height))
    fig.savefig(path, dpi=110, facecolor=SURFACE)
    plt.close(fig)


def plot_series(daily: pd.DataFrame, column: str, obs: dict[str, pd.Series], title: str, unit: str, path: Path) -> None:
    """Per treatment: each framework's median over the 9 models (line) and range (band), observations."""
    tids = sorted(daily["TREAT_ID"].unique())
    fig, axes = panels(title, len(tids))
    for ax, tid in zip(axes, tids, strict=True):
        style(ax)
        for fw, name in FRAMEWORKS.items():
            d = daily[(daily["TREAT_ID"] == tid) & (daily["framework"] == fw)]
            if d.empty:
                continue
            g = d.groupby("DATE")[column]
            ax.fill_between(g.min().index, g.min(), g.max(), color=COLOR[fw], alpha=0.18, linewidth=0)
            ax.plot(g.median().index, g.median(), color=COLOR[fw], linewidth=1.6, label=name)
        o = obs.get(tid)
        if o is not None and len(o):
            dots = {"marker": "o", "markersize": 4} if len(o) < 60 else {"marker": ".", "markersize": 3}
            ax.plot(pd.to_datetime(o.index), o.to_numpy(), linestyle="none", color=INK, label="observed", **dots)
        ax.set_title(tid, loc="left", fontsize=10, color=INK)
    finish(fig, axes, unit, path)


def plot_temperature(t: pd.DataFrame, site: str, top: int, bottom: int, path: Path) -> None:
    d = common_dates(t[(t["top"] == top) & (t["bottom"] == bottom) & t["model"].isin(ENSEMBLE)])
    tids = sorted(d["TREAT_ID"].unique())
    fig, axes = panels(
        f"{site}: soil temperature {top}-{bottom} cm, daily mean, median (line) and range (band) of the"
        f" {len(ENSEMBLE)} models per framework", len(tids),
    )  # fmt: skip
    for ax, tid in zip(axes, tids, strict=True):
        style(ax)
        for fw, name in FRAMEWORKS.items():
            g = d[(d["TREAT_ID"] == tid) & (d["framework"] == fw)].groupby("DATE")["value"]
            if g.ngroups == 0:
                continue
            ax.fill_between(g.min().index, g.min(), g.max(), color=COLOR[fw], alpha=0.15, linewidth=0)
            ax.plot(g.median().index, g.median(), color=COLOR[fw], linewidth=1.6, label=name)
        ax.set_title(tid, loc="left", fontsize=10, color=INK)
    finish(fig, axes, "°C", path)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", choices=SITES, default="maricopa")
    ap.add_argument("--monica", type=Path, help="default: soil_temperature/out_<site>/results")
    ap.add_argument("--other", type=Path, help="default: soil_temperature/out_<site>_other_models")
    ap.add_argument("--out", type=Path, help="default: soil_temperature/out_<site>/framework_comparison")
    args = ap.parse_args()
    site = SITES[args.site]
    monica = args.monica or ST_DIR / f"out_{args.site}" / "results"
    other = args.other or ST_DIR / f"out_{args.site}_other_models"
    out = args.out or ST_DIR / f"out_{args.site}" / "framework_comparison"
    logging.disable(logging.INFO)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 30)

    dirs = framework_dirs(monica, other)
    print("frameworks:", {fw: str(d) for fw, d in dirs.items()})  # noqa: T201
    frameworks = list(dirs)
    daily = load_all(dirs, "Daily", site)
    layers = load_all(dirs, "Layers", site)
    obs = site.observations()
    out.mkdir(parents=True, exist_ok=True)

    crop = crop_table(daily, load_summaries(dirs, site), obs)
    crop.to_csv(out / "crop.csv", index=False)
    print("Crop (median over the 9 models), each framework's own season\n", crop.to_string(index=False))  # noqa: T201

    sw_top, sw_bottom = site.soil_water_cm
    common = common_dates(daily)
    sw = common_dates(water_mm(layers, sw_top, sw_bottom))
    water = water_table(common, sw, obs, site.soil_water_cm)
    water.to_csv(out / "water.csv", index=False)
    print("\nWater (median over the 9 models), days all frameworks simulated\n", water.to_string(index=False))  # noqa: T201

    temps = onto_layers(layers, "TSAV")
    vs_monica, spread = temperature_tables(common_dates(temps), frameworks)
    vs_monica.to_csv(out / "soil_temperature.csv", index=False)
    spread.to_csv(out / "soil_temperature_spread.csv", index=False)
    table = vs_monica.pivot_table(index=["framework", "model"], columns="top", values="mean diff to MONICA C")
    print("\nSoil temperature: the model in another framework minus in MONICA, mean [°C], by layer top [cm]")  # noqa: T201
    print(table.to_string())  # noqa: T201
    print("\n", spread.to_string(index=False))  # noqa: T201

    n = site.name
    plot_series(daily, "LAID", {t: o.lai for t, o in obs.items() if o.lai is not None},
                f"{n}: LAI, median (line) and range (band) of the 9 models per framework", "m²/m²",
                out / "lai.png")  # fmt: skip
    plot_series(daily, "CWAD", {t: o.biomass for t, o in obs.items() if o.biomass is not None},
                f"{n}: above ground biomass, median and range of the 9 models per framework", "kg/ha",
                out / "biomass.png")  # fmt: skip
    if any(len(o.et) for o in obs.values()):
        plot_series(common, "ETAD", {t: o.et for t, o in obs.items()},
                    f"{n}: ET, median and range of the 9 models per framework", "mm/d", out / "et.png")  # fmt: skip
    sw_obs = {t: o.soil_water for t, o in obs.items() if o.soil_water is not None}
    plot_series(sw.rename(columns={"mm": "SW"}), "SW", sw_obs,
                f"{n}: soil water {sw_top}-{sw_bottom} cm, median and range of the 9 models per framework", "mm",
                out / "soil_water.png")  # fmt: skip
    for top, bottom in [(0, 5), (15, 30), (45, 60), (90, 120)]:
        plot_temperature(temps, n, top, bottom, out / f"soil_temperature_{top}-{bottom}cm.png")
    print(f"\nwrote {out}")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
