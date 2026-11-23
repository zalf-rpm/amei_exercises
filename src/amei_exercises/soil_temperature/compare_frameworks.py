"""Maricopa: MONICA's runs against DSSAT's and SiriusQuality's (the other frameworks' AMEI submissions).

The same 9 soil temperature models run in three frameworks (MO = MONICA, DC = DSSAT, SQ =
SiriusQuality), each with its own crop, water balance and layering. Neither is the reference: this shows
where MONICA's drivers (crop, ET, soil water) and the resulting soil temperatures stand among them.

    pixi run compare-frameworks [--dssat DIR] [--sq-daily DIR] [--sq-layers DIR] [--sq-summary DIR] [--out DIR]

Defaults: soil_temperature/out_maricopa_other_models/{AMEI_Maricopa_wheat_FACE_sim_results_20260917,CAN,SOIL,SUM}
and soil_temperature/out_maricopa/results. Writes into <out> (default
soil_temperature/out_maricopa/framework_comparison): crop.csv, water.csv, soil_temperature.csv,
soil_temperature_spread.csv and figures.
"""

from __future__ import annotations

import argparse
import logging
import math
import re
import sys
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from amei_exercises.paths import ST_DIR
from amei_exercises.soil_temperature import site_maricopa
from amei_exercises.soil_temperature.evaluate_st import read_layers

OTHER = ST_DIR / "out_maricopa_other_models"
FRAMEWORKS = {"MO": "MONICA", "DC": "DSSAT", "SQ": "SiriusQuality"}
MODELS = ["AP", "DE", "DS", "MO", "PS", "SA", "SQ", "ST", "SW"]  # the 9 AMEI models (not MONICA's iMO)
# DSSAT and SiriusQuality ran MO with its default lower boundary (BaseTemperature 9.5 °C instead of the
# site's TAV 21 °C): left out of the soil temperature ensembles (medians, spreads, figures)
ENSEMBLE = [m for m in MODELS if m != "MO"]
# DSSAT's layers, the coarsest: the others' 5 cm layers are averaged onto them
LAYERS = [(0, 5), (5, 15), (15, 30), (30, 45), (45, 60), (60, 90), (90, 120), (120, 150), (150, 180), (180, 210)]
SW_TOP, SW_BOTTOM = site_maricopa.SOIL_WATER_DEPTHS_CM

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#1f1f1e", "#5f5e58", "#e6e5e0"
COLOR = {"MO": "#2a78d6", "DC": "#eb6834", "SQ": "#1baf7a"}  # categorical slots 1-3


def load_all(dirs: dict[str, tuple[Path, Path]], kind: str) -> pd.DataFrame:
    """All models' Daily or Layers files of all frameworks, long, with columns framework and model."""
    frames = []
    for fw, (daily_dir, layers_dir) in dirs.items():
        folder = daily_dir if kind == "Daily" else layers_dir
        pattern = re.compile(rf"^(?P<model>[A-Z]+?){fw}{kind}Maricopa(?P<t>\d+)\.txt$")
        for f in sorted(folder.glob(f"*{fw}{kind}Maricopa*.txt")):
            m = pattern.match(f.name)
            if m is None or m["model"] not in MODELS:
                continue
            df = read_layers(f)
            df["TREAT_ID"] = df["TREAT_ID"].astype(str)
            frames.append(df.assign(framework=fw, model=m["model"]))
    return pd.concat(frames, ignore_index=True)


def load_summaries(summary_dirs: dict[str, Path]) -> dict[tuple[str, str, str], dict]:
    """(framework, model, TREAT_ID) -> {ADAT, MDAT, LAIX} of the Summary files: one per model
    (<model><fw>SummaryMaricopa.txt) or one for all (<fw>SummaryMaricopa.txt, by MODEL_ID)."""
    out = {}
    for fw, folder in summary_dirs.items():
        for f in sorted(folder.glob(f"*{fw}SummaryMaricopa.txt")):
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
                out[(fw, model, str(r["TREAT_ID"]).strip())] = v
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


def crop_table(daily: pd.DataFrame, summaries: dict, setups: dict) -> pd.DataFrame:
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
    for tid, s in setups.items():
        o = s.obs
        rows.append({
            "TREAT_ID": tid, "framework": "obs", "anthesis": o.anthesis, "maturity": o.maturity,
            "LAI max": o.lai_max, "biomass max": o.biomass.max() if o.biomass is not None else None,
            "yield": o.grain_yield,
        })  # fmt: skip
    return pd.DataFrame(rows).sort_values(["TREAT_ID", "framework"])


def water_table(daily: pd.DataFrame, sw: pd.DataFrame, setups: dict) -> pd.DataFrame:
    """ET against the measured period means and soil water 20-200 cm against the measured, per framework
    (median over the models), only on days all frameworks simulated."""
    et = daily.groupby(["framework", "TREAT_ID", "DATE"])[["ETAD", "DRND"]].median().reset_index()
    swm = sw.groupby(["framework", "TREAT_ID", "DATE"])["mm"].median()
    rows = []
    for (fw, tid), d in et.groupby(["framework", "TREAT_ID"]):
        d = d.set_index("DATE")
        o = setups[tid].obs
        res = []
        for first, last, mm_d in o.et_periods:
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
            f"soil water {SW_TOP}-{SW_BOTTOM} cm bias mm": round(sum(sw_res) / len(sw_res)) if sw_res else None,
            "soil water dates": len(sw_res),
        })  # fmt: skip
    return pd.DataFrame(rows).sort_values(["TREAT_ID", "framework"])


def temperature_tables(t: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Each model in DSSAT and SiriusQuality against itself in MONICA, per layer; and the spread: between the
    models within a framework against between the frameworks for the same model."""
    wide = t.pivot_table(index=["model", "TREAT_ID", "DATE", "top", "bottom"], columns="framework", values="value")
    wide = wide.dropna().reset_index()
    rows = []
    for (model, top, bottom), d in wide.groupby(["model", "top", "bottom"]):
        for fw in ("DC", "SQ"):
            diff = d[fw] - d["MO"]
            rows.append({
                "layer cm": f"{top}-{bottom}", "top": top, "model": model, "framework": fw,
                "mean diff to MONICA C": round(diff.mean(), 2), "RMSD C": round(math.sqrt((diff**2).mean()), 2),
            })  # fmt: skip
    vs_monica = pd.DataFrame(rows)

    spread = []
    wide = wide[wide["model"].isin(ENSEMBLE)]
    for (top, bottom), d in wide.groupby(["top", "bottom"]):
        # between models: SD over the 8 models per framework, treatment, day; mean over all
        between_models = {fw: d.groupby(["TREAT_ID", "DATE"])[fw].std().mean() for fw in FRAMEWORKS}
        between_fw = d[list(FRAMEWORKS)].std(axis=1).mean()
        med = d.groupby(["TREAT_ID", "DATE"])[list(FRAMEWORKS)].median()
        spread.append({
            "layer cm": f"{top}-{bottom}",
            **{f"SD between models, {FRAMEWORKS[fw]}": round(v, 2) for fw, v in between_models.items()},
            "SD between frameworks, same model": round(between_fw, 2),
            "median DSSAT - MONICA": round((med["DC"] - med["MO"]).mean(), 2),
            "median SiriusQuality - MONICA": round((med["SQ"] - med["MO"]).mean(), 2),
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


def panels(title: str, n: int, ncols: int = 4) -> tuple[plt.Figure, list[plt.Axes]]:
    plt.rcParams.update({"font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK_2,
                         "xtick.color": INK_2, "ytick.color": INK_2})  # fmt: skip
    rows = math.ceil(n / ncols)
    fig, axes = plt.subplots(rows, ncols, figsize=(4.2 * ncols, 2.8 * rows), facecolor=SURFACE, squeeze=False)
    for ax in axes.flat[n:]:
        ax.set_visible(False)
    height = fig.get_figheight()
    fig.suptitle(title, x=0.01, y=1 - 0.12 / height, ha="left", va="top", fontsize=12)
    return fig, list(axes.flat[:n])


def finish(fig: plt.Figure, ax0: plt.Axes, path: Path) -> None:
    height = fig.get_figheight()
    handles, labels = ax0.get_legend_handles_labels()
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
            g = d.groupby("DATE")[column]
            ax.fill_between(g.min().index, g.min(), g.max(), color=COLOR[fw], alpha=0.18, linewidth=0)
            ax.plot(g.median().index, g.median(), color=COLOR[fw], linewidth=1.6, label=name)
        o = obs.get(tid)
        if o is not None and len(o):
            ax.plot(pd.to_datetime(o.index), o.to_numpy(), "o", color=INK, markersize=4, label="observed")
        ax.set_title(tid, loc="left", fontsize=10, color=INK)
    for i, ax in enumerate(axes):
        if i % 4 == 0:
            ax.set_ylabel(unit)
    finish(fig, axes[0], path)


def plot_temperature(t: pd.DataFrame, top: int, bottom: int, path: Path) -> None:
    d = common_dates(t[(t["top"] == top) & (t["bottom"] == bottom) & t["model"].isin(ENSEMBLE)])
    tids = sorted(d["TREAT_ID"].unique())
    fig, axes = panels(
        f"Maricopa: soil temperature {top}-{bottom} cm, daily mean, median (line) and range (band) of 8 models"
        " per framework (without MO)", len(tids),
    )  # fmt: skip
    for ax, tid in zip(axes, tids, strict=True):
        style(ax)
        for fw, name in FRAMEWORKS.items():
            g = d[(d["TREAT_ID"] == tid) & (d["framework"] == fw)].groupby("DATE")["value"]
            ax.fill_between(g.min().index, g.min(), g.max(), color=COLOR[fw], alpha=0.15, linewidth=0)
            ax.plot(g.median().index, g.median(), color=COLOR[fw], linewidth=1.6, label=name)
        ax.set_title(tid, loc="left", fontsize=10, color=INK)
    for i, ax in enumerate(axes):
        if i % 4 == 0:
            ax.set_ylabel("°C")
    finish(fig, axes[0], path)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--monica", type=Path, default=ST_DIR / "out_maricopa" / "results")
    ap.add_argument("--dssat", type=Path, default=OTHER / "AMEI_Maricopa_wheat_FACE_sim_results_20260917")
    ap.add_argument("--sq-daily", type=Path, default=OTHER / "CAN")
    ap.add_argument("--sq-layers", type=Path, default=OTHER / "SOIL")
    ap.add_argument("--sq-summary", type=Path, default=OTHER / "SUM")
    ap.add_argument("--out", type=Path, default=ST_DIR / "out_maricopa" / "framework_comparison")
    args = ap.parse_args()
    logging.disable(logging.INFO)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 30)

    dirs = {"MO": (args.monica, args.monica), "DC": (args.dssat, args.dssat), "SQ": (args.sq_daily, args.sq_layers)}
    daily = load_all(dirs, "Daily")
    layers = load_all(dirs, "Layers")
    setups = {s.id: s for s in site_maricopa.load()}
    args.out.mkdir(parents=True, exist_ok=True)

    summaries = load_summaries({"MO": args.monica, "DC": args.dssat, "SQ": args.sq_summary})
    crop = crop_table(daily, summaries, setups)
    crop.to_csv(args.out / "crop.csv", index=False)
    print("Crop (median over the 9 models), each framework's own season\n", crop.to_string(index=False))  # noqa: T201

    common = common_dates(daily)
    sw = common_dates(water_mm(layers, SW_TOP, SW_BOTTOM))
    water = water_table(common, sw, setups)
    water.to_csv(args.out / "water.csv", index=False)
    print("\nWater (median over the 9 models), days all frameworks simulated\n", water.to_string(index=False))  # noqa: T201

    temps = onto_layers(layers, "TSAV")
    vs_monica, spread = temperature_tables(common_dates(temps))
    vs_monica.to_csv(args.out / "soil_temperature.csv", index=False)
    spread.to_csv(args.out / "soil_temperature_spread.csv", index=False)
    table = vs_monica.pivot_table(index=["framework", "model"], columns="top", values="mean diff to MONICA C")
    print("\nSoil temperature: model in DSSAT/SQ minus the same model in MONICA, mean [°C], by layer top [cm]")  # noqa: T201
    print(table.to_string())  # noqa: T201
    print("\n", spread.to_string(index=False))  # noqa: T201

    obs = {tid: s.obs for tid, s in setups.items()}
    plot_series(daily, "LAID", {t: o.lai for t, o in obs.items() if o.lai is not None},
                "Maricopa: LAI, median (line) and range (band) of the 9 models per framework", "m²/m²",
                args.out / "lai.png")  # fmt: skip
    plot_series(daily, "CWAD", {t: o.biomass for t, o in obs.items() if o.biomass is not None},
                "Maricopa: above ground biomass, median and range of the 9 models per framework", "kg/ha",
                args.out / "biomass.png")  # fmt: skip
    sw_obs = {t: o.soil_water for t, o in obs.items() if o.soil_water is not None}
    plot_series(sw.rename(columns={"mm": "SW"}), "SW", sw_obs,
                f"Maricopa: soil water {SW_TOP}-{SW_BOTTOM} cm, median and range of the 9 models per framework", "mm",
                args.out / "soil_water.png")  # fmt: skip
    for top, bottom in [(0, 5), (15, 30), (45, 60), (90, 120)]:
        plot_temperature(temps, top, bottom, args.out / f"soil_temperature_{top}-{bottom}cm.png")
    print(f"\nwrote {args.out}")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
