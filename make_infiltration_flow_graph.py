import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, date, time
from pathlib import Path

INPUT_FILE = Path("Vazao_agua_vazamento_26_04.xlsx")

OUTPUT_DIR = Path("OUTPUT")
OUTPUT_DIR.mkdir(exist_ok=True)


OUTPUT_GRAPH = OUTPUT_DIR / "vazao_infiltracao_curva_continua.png"
OUTPUT_RATES_CSV = OUTPUT_DIR / "vazao_infiltracao_taxas_calculadas.csv"
OUTPUT_CURVE_CSV = OUTPUT_DIR / "vazao_infiltracao_curva_suavizada.csv"


def parse_excel_date(value):
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.to_datetime(str(value).strip()).date()


def parse_excel_time(value):
    if pd.isna(value):
        return None
    if isinstance(value, time):
        return value
    if isinstance(value, datetime):
        return value.time()
    return pd.to_datetime(str(value).strip()).time()


def combine_date_time(date_value, time_value):
    d = parse_excel_date(date_value)
    t = parse_excel_time(time_value)
    return pd.Timestamp(datetime.combine(d, t))


def load_measurements(path):
    df = pd.read_excel(path)
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    df.columns = [str(c).strip() for c in df.columns]

    required_cols = ["date", "time_start", "time_end", "ml"]
    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        raise ValueError(
            f"Missing columns in Excel: {missing}. "
            f"Expected columns: {required_cols}. "
            f"Found columns: {list(df.columns)}"
        )

    df = df[required_cols].dropna(subset=required_cols).copy()

    df["start_dt"] = [
        combine_date_time(d, t)
        for d, t in zip(df["date"], df["time_start"])
    ]

    df["end_dt"] = [
        combine_date_time(d, t)
        for d, t in zip(df["date"], df["time_end"])
    ]

    crosses_midnight = df["end_dt"] < df["start_dt"]
    df.loc[crosses_midnight, "end_dt"] = df.loc[crosses_midnight, "end_dt"] + pd.Timedelta(days=1)

    df["ml"] = pd.to_numeric(df["ml"], errors="coerce")
    df = df.dropna(subset=["ml"]).copy()

    return df.sort_values("start_dt").reset_index(drop=True)


def calculate_rates(df):
    df = df.copy()

    df["duration_h"] = (df["end_dt"] - df["start_dt"]).dt.total_seconds() / 3600

    invalid_rows = df[df["duration_h"] <= 0]
    if not invalid_rows.empty:
        raise ValueError(
            "Some rows have invalid duration. Check time_start and time_end:\n"
            f"{invalid_rows[['date', 'time_start', 'time_end', 'ml']]}"
        )

    df["ml_per_h"] = df["ml"] / df["duration_h"]
    df["mid_dt"] = df["start_dt"] + (df["end_dt"] - df["start_dt"]) / 2
    df["gap_after_h"] = df["start_dt"].shift(-1).sub(df["end_dt"]).dt.total_seconds() / 3600

    return df.sort_values("mid_dt").reset_index(drop=True)


def make_continuous_curve(df, points=1000):
    x_dt = df["mid_dt"]
    y = df["ml_per_h"].to_numpy()

    x0 = x_dt.iloc[0]
    x_hours = np.array([
        (t - x0).total_seconds() / 3600
        for t in x_dt
    ])

    x_dense = np.linspace(x_hours.min(), x_hours.max(), points)

    try:
        from scipy.interpolate import PchipInterpolator

        interpolator = PchipInterpolator(x_hours, y)
        y_dense = interpolator(x_dense)
        method = "PCHIP"
    except Exception:
        y_linear = np.interp(x_dense, x_hours, y)
        window = 31
        kernel = np.ones(window) / window
        y_pad = np.pad(y_linear, (window // 2, window // 2), mode="edge")
        y_dense = np.convolve(y_pad, kernel, mode="valid")
        method = "linear interpolation + moving average"

    x_dense_dt = [
        x0 + pd.Timedelta(hours=float(h))
        for h in x_dense
    ]

    curve_df = pd.DataFrame({
        "time": x_dense_dt,
        "ml_per_h_smooth": y_dense,
    })

    return curve_df, method


def plot_graph(df, curve_df, method):
    plt.figure(figsize=(15, 7))

    plt.plot(
        curve_df["time"],
        curve_df["ml_per_h_smooth"],
        linewidth=2.8,
        label=f"Curva contínua aproximada ({method})",
    )

    plt.scatter(
        df["mid_dt"],
        df["ml_per_h"],
        s=60,
        zorder=3,
        label="Pontos medidos",
    )

    for _, row in df.iterrows():
        plt.annotate(
            f"{row['ml_per_h']:.1f}",
            (row["mid_dt"], row["ml_per_h"]),
            xytext=(0, 9),
            textcoords="offset points",
            ha="center",
            fontsize=9,
        )

    df_trend = df.sort_values("mid_dt").copy()

    df_trend["trend_ma"] = (
        df_trend["ml_per_h"]
        .rolling(window=3, center=True, min_periods=1)
        .mean()
    )

    plt.plot(
        df_trend["mid_dt"],
        df_trend["trend_ma"],
        linestyle="--",
        linewidth=2.2,
        label="Tendência média móvel",
    )

    plt.title(
        f"Vazão da infiltração ao longo do tempo",
        fontsize=16,
        pad=14,
    )

    plt.xlabel("Tempo")
    plt.ylabel("Vazão (ml/h)")
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.xticks(rotation=30)
    plt.tight_layout()
    plt.savefig(OUTPUT_GRAPH, dpi=220, bbox_inches="tight")
    plt.show()


def main():
    df = load_measurements(INPUT_FILE)
    df = calculate_rates(df)

    df_export = df[
        [
            "date",
            "time_start",
            "time_end",
            "ml",
            "duration_h",
            "ml_per_h",
            "start_dt",
            "end_dt",
            "mid_dt",
            "gap_after_h",
        ]
    ].copy()

    df_export["duration_h"] = df_export["duration_h"].round(4)
    df_export["ml_per_h"] = df_export["ml_per_h"].round(2)
    df_export["gap_after_h"] = df_export["gap_after_h"].round(4)

    df_export.to_csv(OUTPUT_RATES_CSV, index=False, encoding="utf-8-sig")

    curve_df, method = make_continuous_curve(df)
    curve_df.to_csv(OUTPUT_CURVE_CSV, index=False, encoding="utf-8-sig")

    plot_graph(df, curve_df, method)

    print("Done.")
    print(f"Input file: {INPUT_FILE}")
    print(f"Interpolation method: {method}")
    print(f"Graph saved to: {OUTPUT_GRAPH}")
    print(f"Calculated rates saved to: {OUTPUT_RATES_CSV}")
    print(f"Smoothed curve saved to: {OUTPUT_CURVE_CSV}")
    print()
    print(df_export)


if __name__ == "__main__":
    main()
