"""Quick check of a run_bushland.py run against the Bushland observations.

Prints a table per treatment (daily ET vs lysimeter, phenology, yield, LAI max) and writes
<out>/evaluation.png: simulated ETa and ET0 against the lysimeter ET, one panel per treatment.

    pixi run python evapotranspiration/evaluate_bushland.py [--out evapotranspiration/out_bushland]
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_bushland import HERE, load_treatments, read_sheets  # noqa: E402

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#1f1f1e", "#5f5e58", "#e6e5e0"
SIM_ETA, OBS_ET, SIM_ET0 = "#2a78d6", "#eb6834", "#1baf7a"


def read_result(path: Path) -> pd.DataFrame:
    lines = path.read_text().splitlines()
    code_row = next(i for i, line in enumerate(lines) if line.startswith("MODEL_ID"))
    df = pd.read_csv(path, sep="\t", skiprows=code_row, na_values="NA")
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=HERE / "out_bushland")
    ap.add_argument("--xlsx", type=Path, default=HERE / "AMEI_Bushland_v4_2026-01-24(no_soil_T).xlsx")
    ap.add_argument("--site-name", default="Bushland")
    ap.add_argument("--kind", default="Orig")
    ap.add_argument("--module-code", default="MOFAPM")
    args = ap.parse_args()

    treatments = load_treatments(read_sheets(args.xlsx))
    res = args.out / "results"
    summary = read_result(res / f"ETstudy{args.kind}{args.module_code}Summary{args.site_name}.txt").set_index(
        "TREAT_ID"
    )

    stats, panels = [], []
    for tid, t in treatments.items():
        f = res / f"ETstudy{args.kind}{args.module_code}Daily{args.site_name}{tid}.txt"
        if not f.exists():
            continue
        sim = read_result(f).assign(DATE=lambda d: pd.to_datetime(d["DATE"])).set_index("DATE")
        obs = pd.DataFrame(t.rows.get("ET_Daily", []))
        obs = (
            obs.assign(DATE=pd.to_datetime(obs["DATE"])).set_index("DATE")["ETAD"].astype(float)
            if len(obs)
            else pd.Series(dtype=float)
        )
        both = pd.concat([sim["ETAD"].rename("sim"), obs.rename("obs")], axis=1, join="inner").dropna()
        err = both["sim"] - both["obs"]
        o = t.rows.get("Obs_crop_summary_means", [{}])[0]
        s = summary.loc[tid]

        def d(v):
            return "-" if v is None or (isinstance(v, float) and math.isnan(v)) else str(pd.Timestamp(v).date())

        stats.append(
            {
                "treatment": tid,
                "days": len(both),
                "ETa obs": round(both["obs"].sum()),
                "ETa sim": round(both["sim"].sum()),
                "bias mm/d": round(err.mean(), 2),
                "RMSE mm/d": round(math.sqrt((err**2).mean()), 2),
                "ET0 sim (same days)": round(sim["ETO"].reindex(both.index).sum()),
                "anthesis obs/sim": f"{d(o.get('ADAT'))} / {d(s['ADAT'])}",
                "maturity obs/sim": f"{d(o.get('MDAT'))} / {d(s['MDAT'])}",
                "LAI max obs/sim": f"{o.get('LAIX', float('nan')):.1f} / {s['LAIX']:.1f}",
                "yield obs": round(float(o.get("HWAM", float("nan")))),
            }
        )
        panels.append((tid, sim, obs))

    table = pd.DataFrame(stats).set_index("treatment")
    with pd.option_context("display.width", 250, "display.max_columns", 20):
        print(table.to_string())  # noqa: T201
    table.to_csv(args.out / "evaluation.csv")

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
    fig, axes = plt.subplots(3, 2, figsize=(12, 9), sharey=True, facecolor=SURFACE)
    for ax, (tid, sim, obs) in zip(axes.flat, panels, strict=False):
        ax.set_facecolor(SURFACE)
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.plot(sim.index, sim["ETO"], color=SIM_ET0, linewidth=1.2, label="ET0 simulated")
        ax.plot(sim.index, sim["ETAD"], color=SIM_ETA, linewidth=2, label="ETa simulated")
        ax.scatter(
            obs.index, obs.values, s=10, color=OBS_ET, edgecolor=SURFACE, linewidth=0.5, zorder=3, label="ET lysimeter"
        )
        ax.set_title(tid, loc="left", fontsize=10, color=INK)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    for ax in axes[:, 0]:
        ax.set_ylabel("mm/d")
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", ncol=3, frameon=False)
    fig.suptitle(f"Bushland: MONICA {args.module_code} ({args.kind}) vs lysimeter", x=0.01, ha="left", fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(args.out / "evaluation.png", dpi=110, facecolor=SURFACE)
    print(f"\nwrote {args.out / 'evaluation.png'} and evaluation.csv")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
