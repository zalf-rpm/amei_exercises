"""Quick check of a run_et.py run against the site's observations.

Prints a table per treatment (daily ET vs measured, phenology, yield, LAI max) and writes
<out>/evaluation.csv and <out>/evaluation.png: simulated ETa and ET0 against the measured ET, one
panel per treatment.

    pixi run python evapotranspiration/evaluate_et.py --site bushland
"""

from __future__ import annotations

import argparse
import importlib
import math
import sys
from pathlib import Path
from typing import Any

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from et_monica import HERE, read_result, result_names

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#1f1f1e", "#5f5e58", "#e6e5e0"
SIM_ETA, OBS_ET, SIM_ET0, OBS_ET_DAYTIME = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"


def fmt_date(v) -> str:
    return "-" if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NaT else str(pd.Timestamp(v).date())


def fmt_num(v, digits: int = 1) -> str:
    return "-" if v is None or (isinstance(v, float) and math.isnan(v)) else f"{v:.{digits}f}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--site", choices=("bushland", "avignon"), required=True)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--xlsx", type=Path)
    ap.add_argument("--kind", default="Orig")
    ap.add_argument("--module-code", default="MOFAPM")
    args = ap.parse_args()

    site = importlib.import_module(f"site_{args.site}")
    out: Path = args.out or HERE / f"out_{args.site}"
    res = out / "results"
    summary_name, daily_name = result_names(args.kind, args.module_code, site.NAME)
    summary = read_result(res / summary_name).set_index("TREAT_ID")

    stats, panels = [], []
    for s in site.load(args.xlsx or site.XLSX):
        f = res / daily_name.format(id=s.id)
        if not f.exists():
            continue
        sim = read_result(f).assign(DATE=lambda d: pd.to_datetime(d["DATE"]).dt.date).set_index("DATE")
        both = pd.concat([sim["ETAD"].rename("sim"), s.obs.et.rename("obs")], axis=1, join="inner").dropna()
        err = both["sim"] - both["obs"]
        sm = summary.loc[s.id]
        stats.append(
            {
                "treatment": s.id,
                "ET days": len(both),
                "ETa obs": round(both["obs"].sum()),
                "ETa sim": round(both["sim"].sum()),
                "bias mm/d": round(err.mean(), 2) if len(both) else None,
                "RMSE mm/d": round(math.sqrt((err**2).mean()), 2) if len(both) else None,
                "ET0 sim (same days)": round(sim["ETO"].reindex(both.index).sum()),
                "emergence obs/sim": f"{fmt_date(s.obs.emergence)} / {fmt_date(sm['PLDAE'])}",
                "anthesis obs/sim": f"{fmt_date(s.obs.anthesis)} / {fmt_date(sm['ADAT'])}",
                "maturity obs/sim": f"{fmt_date(s.obs.maturity)} / {fmt_date(sm['MDAT'])}",
                "LAI max obs/sim": f"{fmt_num(s.obs.lai_max)} / {fmt_num(sm['LAIX'])}",
                "yield obs/sim": f"{fmt_num(s.obs.grain_yield, 0)} / {fmt_num(sm['GWAM'], 0)}",
            }
        )
        panels.append((s, sim))

    table = pd.DataFrame(stats).set_index("treatment")
    with pd.option_context("display.width", 300, "display.max_columns", 20):
        print(table.to_string())  # noqa: T201
    table.to_csv(out / "evaluation.csv")

    plt.rcParams.update(
        {
            "font.size": 9,
            "axes.edgecolor": GRID,
            "text.color": INK,
            "axes.labelcolor": INK_2,
            "xtick.color": INK_2,
            "ytick.color": INK_2,
        }
    )
    rows = math.ceil(len(panels) / 2)
    fig, axes = plt.subplots(rows, 2, figsize=(12, 3 * rows), sharey=True, facecolor=SURFACE, squeeze=False)
    for ax in axes.flat[len(panels) :]:
        ax.set_visible(False)
    for ax, (s, sim) in zip(axes.flat, panels, strict=False):
        ax.set_facecolor(SURFACE)
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        x = pd.to_datetime(pd.Series(sim.index))
        ax.plot(x, sim["ETO"].to_numpy(), color=SIM_ET0, linewidth=1.2, label="ET0 simulated")
        ax.plot(x, sim["ETAD"].to_numpy(), color=SIM_ETA, linewidth=2, label="ETa simulated")
        if s.obs.et_daytime is not None and len(s.obs.et_daytime):
            ax.scatter(
                pd.to_datetime(pd.Series(s.obs.et_daytime.index)),
                s.obs.et_daytime.to_numpy(),
                s=14,
                facecolor="none",
                edgecolor=OBS_ET_DAYTIME,
                linewidth=1,
                zorder=3,
                label="ET measured, 6-18 h only",
            )
        ax.scatter(
            pd.to_datetime(pd.Series(s.obs.et.index)),
            s.obs.et.to_numpy(),
            s=12,
            color=OBS_ET,
            edgecolor=SURFACE,
            linewidth=0.5,
            zorder=4,
            label="ET measured, 24 h",
        )
        ax.set_title(s.id, loc="left", fontsize=10, color=INK)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    for ax in axes[:, 0]:
        ax.set_ylabel("mm/d")
    handles: dict[str, Any] = {}
    for ax in axes.flat:
        for h, label in zip(*ax.get_legend_handles_labels(), strict=True):
            handles.setdefault(label, h)
    fig.legend(list(handles.values()), list(handles.keys()), loc="upper right", ncol=4, frameon=False)
    fig.suptitle(f"{site.NAME}: MONICA {args.module_code} ({args.kind}) vs measured ET", x=0.01, ha="left", fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.35 / rows / 3 * 2))
    fig.savefig(out / "evaluation.png", dpi=110, facecolor=SURFACE)
    print(f"\nwrote {out / 'evaluation.png'} and evaluation.csv")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
