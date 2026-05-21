from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent / "src"))

from pipeline.ingest import (
    load_globaltech_hris, load_acquiredco_api,
    load_payroll, load_benefits,
    align_schemas, export_dead_letters,
)
from pipeline.clean import run_all as clean_all, export_unmapped_departments
from pipeline.dedup import run_all as dedup_all
from pipeline.validate import DataQualityValidator, PipelineGateError

BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "pipeline.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")


def _step(n: int, title: str) -> None:
    logger.info("=" * 60)
    logger.info("STEP %d: %s", n, title)
    logger.info("=" * 60)


def _enrich_benefits(df: pd.DataFrame) -> pd.DataFrame:
    ben = df[df["source_system"] == "benefits"]
    if ben.empty:
        return df
    summary = ben.groupby("employee_id").agg(
        benefits_enrolled=("benefits_enrolled", "any"),
        plan_type=("plan_type", lambda x: ",".join(x.dropna().astype(str).unique())),
        coverage_level=("coverage_level", "first"),
        enrollment_date=("enrollment_date", "first"),
    ).reset_index()
    rest = df[df["source_system"] != "benefits"].copy().merge(summary, on="employee_id", how="left", suffixes=("", "_b"))
    for col in ("benefits_enrolled", "plan_type", "coverage_level", "enrollment_date"):
        if f"{col}_b" in rest.columns:
            rest[col] = rest[col].fillna(rest.pop(f"{col}_b"))
    logger.info("Benefits enrichment: %d employees enrolled", summary["employee_id"].nunique())
    return rest


def _enrich_payroll(df: pd.DataFrame) -> pd.DataFrame:
    pay = df[df["source_system"] == "payroll"]
    if pay.empty:
        return df
    cols = [c for c in ("employee_id", "base_salary", "salary_currency", "pay_frequency",
                        "bonus_target_pct", "salary_usd_annual", "base_salary_numeric", "base_salary_annual")
            if c in pay.columns]
    summary = pay[cols].drop_duplicates(subset=["employee_id"])
    rest = df[df["source_system"] != "payroll"].copy().merge(summary, on="employee_id", how="left", suffixes=("", "_p"))
    for col in cols[1:]:
        if f"{col}_p" in rest.columns:
            rest[col] = rest[col].fillna(rest.pop(f"{col}_p"))
    logger.info("Payroll enrichment: %d employees have salary data", summary["employee_id"].nunique())
    return rest


def _export_outputs(golden_df: pd.DataFrame, ghost_df: pd.DataFrame, review_df: pd.DataFrame) -> None:
    parquet_dir = OUTPUT_DIR / "golden_dataset"; parquet_dir.mkdir(exist_ok=True)
    for origin, group in golden_df.groupby("company_origin", dropna=False):
        part = parquet_dir / f"company_origin={str(origin).replace(' ', '_')}" / "data.parquet"
        part.parent.mkdir(parents=True, exist_ok=True)
        group.to_parquet(part, index=False)
        logger.info("Partition '%s': %d records", origin, len(group))
    golden_df.to_parquet(OUTPUT_DIR / "golden_dataset.parquet", index=False)
    logger.info("Full golden dataset: %d records", len(golden_df))

    ghost_cols = ["payroll_employee_id", "name", "salary_usd_annual", "ghost_flag_reason"]
    if not ghost_df.empty:
        g = ghost_df.copy()
        g["payroll_employee_id"] = g.get("employee_id", pd.NA)
        g["name"] = (g.get("first_name", pd.Series("", index=g.index)).fillna("") + " " +
                     g.get("last_name",  pd.Series("", index=g.index)).fillna("")).str.strip()
        g[ghost_cols].to_csv(OUTPUT_DIR / "ghost_employees.csv", index=False)
        logger.info("Ghost employees: %d records", len(ghost_df))
    else:
        pd.DataFrame(columns=ghost_cols).to_csv(OUTPUT_DIR / "ghost_employees.csv", index=False)
        logger.info("Ghost employees: 0 records")

    review_cols = ["record_1_id", "record_2_id", "similarity_score", "hire_date_diff_days", "recommended_action"]
    if not review_df.empty:
        review_df.to_csv(OUTPUT_DIR / "probable_match_review.csv", index=False)
        logger.info("Probable matches: %d pairs", len(review_df))
    else:
        pd.DataFrame(columns=review_cols).to_csv(OUTPUT_DIR / "probable_match_review.csv", index=False)
        logger.info("Probable matches: 0 pairs")


def run_pipeline() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "reports").mkdir(exist_ok=True)

    _step(1, "INGESTION")
    gt_df  = load_globaltech_hris(DATA_DIR / "globaltech_hris.csv")
    ac_df  = load_acquiredco_api(DATA_DIR / "acquiredco_api.json")
    pay_df = load_payroll(DATA_DIR / "payroll_data.xlsx")
    ben_df = load_benefits(DATA_DIR / "benefits_enrollment.xml")
    combined_raw = align_schemas(gt_df, ac_df, pay_df, ben_df)
    export_dead_letters(OUTPUT_DIR / "dead_letters.json")
    logger.info("GT=%d  AC=%d  Payroll=%d  Benefits=%d  -> Combined=%d",
                len(gt_df), len(ac_df), len(pay_df), len(ben_df), len(combined_raw))

    _step(2, "CLEANING & TRANSFORMATION")
    combined_clean = clean_all(combined_raw)
    export_unmapped_departments(OUTPUT_DIR / "reports" / "unmapped_departments.csv")

    _step(3, "BENEFITS & PAYROLL ENRICHMENT")
    payroll_clean = combined_clean[combined_clean["source_system"] == "payroll"].copy()
    combined_clean = _enrich_payroll(_enrich_benefits(combined_clean))

    _step(4, "DEDUPLICATION")
    hris_clean = combined_clean[combined_clean["source_system"].isin(["globaltech_hris", "acquiredco_api"])]
    golden_df, ghost_df, review_df = dedup_all(combined_clean, hris_df=hris_clean, payroll_df=payroll_clean)
    unres = golden_df["manager_id"].notna() & ~golden_df["manager_id"].isin(set(golden_df["employee_id"].dropna()))
    if unres.any():
        logger.info("Manager ID cleanup: %d unresolvable manager_ids set to NA", int(unres.sum()))
        golden_df.loc[unres, "manager_id"] = pd.NA

    _step(5, "DATA QUALITY VALIDATION")
    try:
        validator = DataQualityValidator(golden_df)
        validation_report = validator.run_all()
        validator.export(validation_report, OUTPUT_DIR / "reports")
    except PipelineGateError as err:
        logger.critical("Pipeline halted: %s", err); sys.exit(1)

    _step(6, "EXPORT OUTPUTS")
    _export_outputs(golden_df, ghost_df, review_df)
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE — outputs in: %s", OUTPUT_DIR.resolve())
    logger.info("EDA: open notebooks/eda_report.ipynb and run all cells")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
