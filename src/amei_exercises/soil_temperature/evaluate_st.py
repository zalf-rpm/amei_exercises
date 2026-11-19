"""The soil temperature runs of run_st.py, model against model (the workbooks have no measured soil
temperature).

Per depth one figure, one panel per treatment: the range of the 9 AMEI models (band), their median,
and MONICA's (MO, the Crop2ML version) on top. iMO (MONICA's own implementation, not part of the
protocol) only goes into the table: so far it is identical to MO. Plus a table of each model's
deviation from the median of the 9 AMEI models, per depth, over all treatments and days.

    pixi run evaluate-st --site maricopa|bushland [--depths 5 20 50 100]

Writes <out>/evaluation/soil_temperature_<depth>cm.png and model_deviation.csv
(default out: soil_temperature/out_<site>).
"""

from __future__ import annotations

import argparse
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
from amei_exercises.soil_temperature.st_monica import MODELS

SITES = {"maricopa": ("Maricopa", 4), "bushland": ("Bushland", 2)}  # name in the file names, panel columns
INTERNAL = "iMO"  # MONICA's own implementation: shown, but not part of the ensemble

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#1f1f1e", "#5f5e58", "#e6e5e0"
BAND, MEDIAN, MONICA = "#d6d5cf", "#5f5e58", "#2a78d6"


def read_layers(path: Path) -> pd.DataFrame:
    """An AMEI layers file: the code row starts with FRAMEWORK_ID, then the data."""
    lines = path.read_text(encoding="cp1252").splitlines()
    code_row = next(i for i, line in enumerate(lines) if line.startswith("FRAMEWORK_ID"))
    df = pd.read_csv(path, sep="\t", skiprows=code_row, na_values=["na", "NA"], encoding="cp1252")
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df


def load(results: Path, site_name: str) -> pd.DataFrame:
    """TSAV of all models, treatments, layers: columns TREAT_ID, DATE, SLLT, SLLB, one per model."""
    frames = []
    pattern = re.compile(rf"^(?P<model>\w+?)MOLayers{site_name}(?P<treatment>\w+)\.txt$")
    for f in sorted(results.glob(f"*Layers{site_name}*.txt")):
        m = pattern.match(f.name)
        if m is None or m["model"] not in MODELS:
            continue
        df = read_layers(f)[["TREAT_ID", "DATE", "SLLT", "SLLB", "TSAV"]]
        df["TREAT_ID"] = df["TREAT_ID"].astype(str)
        frames.append(df.assign(model=m["model"]))
    long = pd.concat(frames, ignore_index=True)
    return long.pivot_table(index=["TREAT_ID", "DATE", "SLLT", "SLLB"], columns="model", values="TSAV").reset_index()


def deviations(wide: pd.DataFrame, ensemble: list[str], depths: list[float]) -> pd.DataFrame:
    rows = []
    median = wide[ensemble].median(axis=1)
    for depth in depths:
        at = wide["SLLB"] == depth
        for m in [*ensemble, INTERNAL]:
            if m not in wide:
                continue
            d = (wide.loc[at, m] - median[at]).dropna()
            rows.append(
                {
                    "depth_cm": depth,
                    "model": m,
                    "mean_deviation_C": round(d.mean(), 2),
                    "rmsd_C": round(math.sqrt((d**2).mean()), 2),
                    "max_abs_deviation_C": round(d.abs().max(), 2),
                    "days": len(d),
                }
            )
    return pd.DataFrame(rows)


def plot(wide: pd.DataFrame, ensemble: list[str], depth: float, title: str, ncols: int, path: Path) -> None:
    at = wide[wide["SLLB"] == depth]
    treatments = sorted(at["TREAT_ID"].unique())
    rows = math.ceil(len(treatments) / ncols)
    plt.rcParams.update(
        {
            "font.size": 9,
            "axes.edgecolor": GRID,
            "axes.labelcolor": INK_2,
            "xtick.color": INK_2,
            "ytick.color": INK_2,
        }
    )
    fig, axes = plt.subplots(
        rows, ncols, figsize=(6 * ncols if ncols <= 2 else 4.2 * ncols, 3 * rows), sharey=True,
        facecolor=SURFACE, squeeze=False,
    )  # fmt: skip
    for ax in axes.flat[len(treatments) :]:
        ax.set_visible(False)
    for ax, tid in zip(axes.flat, treatments, strict=False):
        d = at[at["TREAT_ID"] == tid].sort_values("DATE")
        ax.set_facecolor(SURFACE)
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ens = d[ensemble]
        ax.fill_between(
            d["DATE"], ens.min(axis=1), ens.max(axis=1), color=BAND, linewidth=0,
            label=f"range of the {len(ensemble)} AMEI models",
        )  # fmt: skip
        ax.plot(d["DATE"], ens.median(axis=1), color=MEDIAN, linewidth=1.2, label="their median")
        if "MO" in d:
            ax.plot(d["DATE"], d["MO"], color=MONICA, linewidth=2, label="MONICA (MO)")
        ax.set_title(tid, loc="left", fontsize=10, color=INK)
        locator = mdates.AutoDateLocator(minticks=3, maxticks=5)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    for ax in axes[:, 0]:
        ax.set_ylabel("°C")
    handles, labels = axes.flat[0].get_legend_handles_labels()
    height = fig.get_figheight()  # inches: the title on top, the legend on its own line below it
    fig.suptitle(title, x=0.01, y=1 - 0.12 / height, ha="left", va="top", fontsize=12)
    fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.005, 1 - 0.42 / height), ncol=3, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.8 / height))
    fig.savefig(path, dpi=110, facecolor=SURFACE)
    plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", choices=SITES, required=True)
    ap.add_argument("--out", type=Path, help="default: soil_temperature/out_<site>")
    ap.add_argument("--depths", type=float, nargs="*", default=[5, 20, 50, 100], help="layer base depths [cm]")
    args = ap.parse_args()

    site_name, ncols = SITES[args.site]
    out: Path = args.out or ST_DIR / f"out_{args.site}"
    wide = load(out / "results", site_name)
    ensemble = [m for m in MODELS if m != INTERNAL and m in wide]
    (out / "evaluation").mkdir(exist_ok=True)

    dev = deviations(wide, ensemble, args.depths)
    dev.to_csv(out / "evaluation" / "model_deviation.csv", index=False)
    table = dev.pivot(index="model", columns="depth_cm", values=["mean_deviation_C", "rmsd_C"])
    print(f"{site_name}: deviation from the median of the {len(ensemble)} AMEI models [°C], all treatments and days")  # noqa: T201
    print(table.loc[[*ensemble, INTERNAL]].to_string())  # noqa: T201

    for depth in args.depths:
        top = wide.loc[wide["SLLB"] == depth, "SLLT"].iloc[0]
        name = f"soil_temperature_{depth:g}cm.png"
        title = f"{site_name}: soil temperature {top:g}-{depth:g} cm, daily mean, MONICA with 9 soil temperature models"
        plot(wide, ensemble, depth, title, ncols, out / "evaluation" / name)
    print(f"wrote {out / 'evaluation'}/soil_temperature_<depth>cm.png and model_deviation.csv")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
