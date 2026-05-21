from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger("pipeline.dedup")

try:
    from rapidfuzz import fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    logger.warning("rapidfuzz not installed — Pass 3 fuzzy matching skipped")
    _HAS_RAPIDFUZZ = False

SOURCE_PRIORITY: dict[str, int] = {
    "globaltech_hris": 3,
    "acquiredco_api":  3,
    "payroll":         2,
    "benefits":        1,
}

FUZZY_THRESHOLD     = 88
HIRE_DATE_TOLERANCE = 30


def _merge_rows(group: pd.DataFrame) -> dict:
    merged: dict = {}
    for col in group.columns:
        if col in ("source_system", "_priority"):
            continue
        non_null = group[col].dropna()
        merged[col] = non_null.iloc[0] if len(non_null) > 0 else pd.NA
    merged["source_systems"] = ",".join(group["source_system"].dropna().unique())
    return merged


def dedup_exact_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_priority"] = df["source_system"].map(SOURCE_PRIORITY).fillna(0)
    df = df.sort_values("_priority", ascending=False)

    out_rows: list[dict] = []
    for emp_id, group in df.groupby("employee_id", sort=False):
        if len(group) == 1:
            row = group.iloc[0].to_dict()
            row.setdefault("source_systems", row.get("source_system", ""))
            row.setdefault("dedup_method", "single_source")
        else:
            row = _merge_rows(group)
            row["dedup_method"] = "exact_id"
        out_rows.append(row)

    result = pd.DataFrame(out_rows).drop(columns=["_priority"], errors="ignore")
    logger.info("Pass 1 (exact_id): %d -> %d records", len(df), len(result))
    return result.reset_index(drop=True)


def dedup_email(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "email" not in df.columns:
        return df

    df["_email_lower"] = df["email"].astype(str).str.strip().str.lower()
    duped = df["_email_lower"][df["_email_lower"].duplicated(keep=False) & (df["_email_lower"] != "nan")]

    if duped.empty:
        df.drop(columns=["_email_lower"], inplace=True)
        logger.info("Pass 2 (email): no cross-company duplicates found")
        return df

    out_rows: list[dict] = []
    seen: set[str] = set()
    for idx, row in df.iterrows():
        em = row["_email_lower"]
        if em in seen:
            continue
        if em in duped.values:
            group = df[df["_email_lower"] == em]
            merged = _merge_rows(group)
            merged["dedup_method"] = "email_match"
            out_rows.append(merged)
            seen.add(em)
        else:
            out_rows.append(row.to_dict())

    result = pd.DataFrame(out_rows).drop(columns=["_email_lower"], errors="ignore")
    logger.info("Pass 2 (email): %d -> %d records", len(df), len(result))
    return result.reset_index(drop=True)


def dedup_fuzzy_name(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not _HAS_RAPIDFUZZ:
        logger.warning("Pass 3 skipped: rapidfuzz not available")
        return df, pd.DataFrame()

    df = df.copy()
    df["_full_name"] = (
        df["first_name"].fillna("").astype(str) + " " +
        df["last_name"].fillna("").astype(str)
    ).str.strip().str.lower()

    hris_sources = {"globaltech_hris", "acquiredco_api"}
    src_col = df.get("source_systems", df.get("source_system", pd.Series(dtype=str)))
    hris_mask = src_col.astype(str).str.contains("hris|api", na=False)
    df_hris = df[hris_mask & df["hire_date"].notna()].copy().sort_values("hire_date").reset_index(drop=True)

    review_pairs: list[dict] = []
    n = len(df_hris)
    for i in range(n):
        row_i = df_hris.iloc[i]
        date_i = pd.Timestamp(row_i["hire_date"])
        id_i   = str(row_i.get("employee_id", ""))
        for j in range(i + 1, n):
            row_j = df_hris.iloc[j]
            date_j = pd.Timestamp(row_j["hire_date"])
            if (date_j - date_i).days > HIRE_DATE_TOLERANCE:
                break
            id_j = str(row_j.get("employee_id", ""))
            if id_i[:2] == id_j[:2]:
                continue
            score = fuzz.token_sort_ratio(row_i["_full_name"], row_j["_full_name"])
            if score >= FUZZY_THRESHOLD:
                review_pairs.append({
                    "record_1_id":         id_i,
                    "record_2_id":         id_j,
                    "record_1_name":       f"{row_i.get('first_name','')} {row_i.get('last_name','')}",
                    "record_2_name":       f"{row_j.get('first_name','')} {row_j.get('last_name','')}",
                    "similarity_score":    round(score, 2),
                    "hire_date_diff_days": (date_j - date_i).days,
                    "record_1_source":     row_i.get("source_systems", row_i.get("source_system", "")),
                    "record_2_source":     row_j.get("source_systems", row_j.get("source_system", "")),
                    "recommended_action":  "REVIEW",
                })

    df.drop(columns=["_full_name"], inplace=True, errors="ignore")
    review_df = pd.DataFrame(review_pairs)
    logger.info("Pass 3 (fuzzy): %d probable matches (threshold=%d%%)", len(review_df), FUZZY_THRESHOLD)
    return df, review_df


def detect_ghost_employees(hris_df: pd.DataFrame, payroll_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    hris_ids = set(hris_df["employee_id"].dropna().unique())
    payroll_df = payroll_df.copy()
    is_ghost = ~payroll_df["employee_id"].isin(hris_ids)
    ghost_df = payroll_df[is_ghost].copy()
    ghost_df["ghost_employee"]    = True
    ghost_df["ghost_flag_reason"] = "Payroll record has no matching HRIS entry"
    logger.info("Ghost detection: %d ghosts out of %d payroll records", len(ghost_df), len(payroll_df))
    return payroll_df[~is_ghost].copy(), ghost_df


def ensure_provenance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "source_systems" not in df.columns:
        df["source_systems"] = df.get("source_system", pd.NA)
    if "dedup_method" not in df.columns:
        df["dedup_method"] = "single_source"
    mask = df["source_systems"].isna() | (df["source_systems"].astype(str) == "nan")
    if "source_system" in df.columns:
        df.loc[mask, "source_systems"] = df.loc[mask, "source_system"]
    df.loc[df["dedup_method"].isna(), "dedup_method"] = "single_source"
    return df


def run_all(
    combined_df: pd.DataFrame,
    hris_df: Optional[pd.DataFrame] = None,
    payroll_df: Optional[pd.DataFrame] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Deduplication pipeline on %d rows", len(combined_df))
    if hris_df is None:
        hris_df = combined_df[combined_df["source_system"].isin(["globaltech_hris", "acquiredco_api"])]
    if payroll_df is None:
        payroll_df = combined_df[combined_df["source_system"] == "payroll"]

    _, ghost_df = detect_ghost_employees(hris_df, payroll_df)
    df = dedup_exact_id(combined_df)
    df = dedup_email(df)
    df, review_df = dedup_fuzzy_name(df)
    df = ensure_provenance(df)
    logger.info("Deduplication complete: %d golden records", len(df))
    return df, ghost_df, review_df
