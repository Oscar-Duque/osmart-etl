from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import os

EXCLUSION_COLS = ["store_id","art_id","hist_id","fecha_iso","reason","detail","detected_at_iso","uniq"]

def _ensure_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        pd.DataFrame(columns=EXCLUSION_COLS).to_csv(path, index=False, encoding="utf-8")

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _normalize_rows(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Required columns
    out["store_id"] = pd.to_numeric(out["store_id"], errors="coerce").astype("Int64")
    out["art_id"]   = pd.to_numeric(out["art_id"],   errors="coerce").astype("Int64")
    if "hist_id" not in out.columns:
        out["hist_id"] = pd.Series([pd.NA] * len(out), dtype="Int64")
    else:
        out["hist_id"] = pd.to_numeric(out["hist_id"], errors="coerce").astype("Int64")

    # fecha -> fecha_iso if needed
    if "fecha_iso" not in out.columns:
        if "fecha" in out.columns:
            out["fecha_iso"] = pd.to_datetime(out["fecha"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            # allow missing; caller should pass fecha_iso normally
            out["fecha_iso"] = ""

    if "reason" not in out.columns:
        out["reason"] = "manual_exclusion"
    if "detail" not in out.columns:
        out["detail"] = ""

    if "detected_at_iso" not in out.columns:
        out["detected_at_iso"] = _now_iso()

    # Stable de-dup key (treat NaN hist_id as empty)
    out["uniq"] = out.apply(
        lambda r: f"{r['store_id']}|{r['art_id']}|{'' if pd.isna(r['hist_id']) else int(r['hist_id'])}|{r['fecha_iso']}|{r['reason']}",
        axis=1
    )

    return out[EXCLUSION_COLS]

def load_exclusions(csv_path: Path) -> pd.DataFrame:
    _ensure_csv(csv_path)
    return pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8")

def append_exclusions(csv_path: Path, rows: pd.DataFrame) -> None:
    """Append rows to CSV with atomic replace and de-dup on uniq."""
    _ensure_csv(csv_path)
    cur = load_exclusions(csv_path)
    add = _normalize_rows(rows)
    combined = pd.concat([cur, add], ignore_index=True)
    combined = combined.drop_duplicates(subset=["uniq"], keep="first")
    tmp_path = csv_path.with_suffix(".tmp.csv")
    combined.to_csv(tmp_path, index=False, encoding="utf-8")
    os.replace(tmp_path, csv_path)

def get_manual_hist_ids(csv_path: Path, store_id: int) -> set[str]:
    df = load_exclusions(csv_path)
    df = df[df["store_id"] == str(store_id)]
    # Only hist_id rows, ignore blanks
    return set(df.loc[df["hist_id"] != "", "hist_id"])

def apply_exclusions_and_log(
    df: pd.DataFrame,
    store_id: int,
    csv_path: Path,
    abs_max: int = 1_000_000
) -> tuple[pd.DataFrame, int]:
    """
    Exclude rows by (a) manual hist_id in CSV and (b) threshold rule for absurd absolute snapshots.
    Log newly detected threshold violations into CSV.
    """
    df = df.copy()
    # Columns we rely on
    df["is_absolute"] = df.get("is_absolute", 0).fillna(0).astype(bool)
    df["abs_stock_after"] = pd.to_numeric(df.get("abs_stock_after", np.nan), errors="coerce")

    # (a) manual by hist_id (if hist_id present)
    if "hist_id" in df.columns:
        manual_hist = get_manual_hist_ids(csv_path, store_id)
        bad_manual = df["hist_id"].astype(str).isin(manual_hist) if manual_hist else pd.Series(False, index=df.index)
    else:
        bad_manual = pd.Series(False, index=df.index)

    # (b) rule-based absurd absolute snapshot
    bad_rule = df["is_absolute"] & (df["abs_stock_after"].abs() > abs_max)

    bad = bad_manual | bad_rule
    flagged = df.loc[bad].copy()

    # Log only newly detected threshold violations (not already in CSV)
    if flagged.any(axis=None):
        to_log = flagged.copy()
        to_log["store_id"] = store_id
        if "fecha_iso" not in to_log.columns:
            to_log["fecha_iso"] = pd.to_datetime(to_log["fecha"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        to_log["reason"] = np.where(bad_rule.loc[bad] & ~bad_manual.loc[bad], "abs_stock_after_too_large",
                             np.where(~bad_rule.loc[bad] & bad_manual.loc[bad], "manual_exclusion",
                                      "manual_and_threshold"))
        to_log["detail"] = to_log["abs_stock_after"].apply(lambda v: f"value={int(v)}" if pd.notnull(v) else "")
        
        # Ensure columns exist even if your extract doesn't include them
        if "hist_id" not in to_log.columns:
            to_log["hist_id"] = pd.NA
        if "fecha_iso" not in to_log.columns:
            to_log["fecha_iso"] = pd.to_datetime(to_log["fecha"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        append_exclusions(csv_path, to_log[["store_id","art_id","hist_id","fecha_iso","reason","detail"]])

    return df.loc[~bad].copy(), len(flagged)
