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


def _enrich_benefits(df: pd.DataFrame) -> pd.DataFrame:
    ben = df[df["source_system"] == "benefits"].copy()
    if ben.empty:
        return df
    summary = (
        ben.groupby("employee_id")
        .agg(
            benefits_enrolled=("benefits_enrolled", "any"),
            plan_type=("plan_type", lambda x: ",".join(x.dropna().astype(str).unique())),
            coverage_level=("coverage_level", "first"),
            enrollment_date=("enrollment_date", "first"),
        )
        .reset_index()
    )
    non_ben = df[df["source_system"] != "benefits"].copy()
    non_ben = non_ben.merge(summary, on="employee_id", how="left", suffixes=("", "_ben"))
    for col in ("benefits_enrolled", "plan_type", "coverage_level", "enrollment_date"):
        bcol = f"{col}_ben"
        if bcol in non_ben.columns:
            non_ben[col] = non_ben[col].fillna(non_ben[bcol])
            non_ben.drop(columns=[bcol], inplace=True)
    logger.info("Benefits enrichment: %d employees enrolled", summary["employee_id"].nunique())
    return non_ben


def _enrich_payroll(df: pd.DataFrame) -> pd.DataFrame:
    pay = df[df["source_system"] == "payroll"].copy()
    if pay.empty:
        return df
    pay_cols = ["employee_id", "base_salary", "salary_currency", "pay_frequency",
                "bonus_target_pct", "salary_usd_annual", "base_salary_numeric", "base_salary_annual"]
    available = [c for c in pay_cols if c in pay.columns]
    summary = pay[available].drop_duplicates(subset=["employee_id"])
    non_pay = df[df["source_system"] != "payroll"].copy()
    non_pay = non_pay.merge(summary, on="employee_id", how="left", suffixes=("", "_pay"))
    for col in available[1:]:
        pcol = f"{col}_pay"
        if pcol in non_pay.columns:
            non_pay[col] = non_pay[col].fillna(non_pay[pcol])
            non_pay.drop(columns=[pcol], inplace=True)
    logger.info("Payroll enrichment: %d employees have salary data", summary["employee_id"].nunique())
    return non_pay


def _export_outputs(golden_df: pd.DataFrame, ghost_df: pd.DataFrame, review_df: pd.DataFrame) -> None:
    parquet_dir = OUTPUT_DIR / "golden_dataset"
    parquet_dir.mkdir(exist_ok=True)
    for origin, group in golden_df.groupby("company_origin", dropna=False):
        safe = str(origin).replace(" ", "_")
        part_path = parquet_dir / f"company_origin={safe}" / "data.parquet"
        part_path.parent.mkdir(parents=True, exist_ok=True)
        group.to_parquet(part_path, index=False)
        logger.info("Partition '%s': %d records", origin, len(group))

    golden_df.to_parquet(OUTPUT_DIR / "golden_dataset.parquet", index=False)
    logger.info("Full golden dataset: %d records", len(golden_df))

    ghost_required = ["payroll_employee_id", "name", "salary_usd_annual", "ghost_flag_reason"]
    if not ghost_df.empty:
        ghost_out = ghost_df.copy()
        ghost_out["payroll_employee_id"] = ghost_out.get("employee_id", pd.NA)
        ghost_out["name"] = (
            ghost_out.get("first_name", pd.Series("", index=ghost_out.index)).fillna("") + " " +
            ghost_out.get("last_name",  pd.Series("", index=ghost_out.index)).fillna("")
        ).str.strip()
        ghost_out[ghost_required].to_csv(OUTPUT_DIR / "ghost_employees.csv", index=False)
        logger.info("Ghost employees: %d records", len(ghost_df))
    else:
        pd.DataFrame(columns=ghost_required).to_csv(OUTPUT_DIR / "ghost_employees.csv", index=False)
        logger.info("Ghost employees: 0 records (empty file written for deliverable completeness)")

    if not review_df.empty:
        review_df.to_csv(OUTPUT_DIR / "probable_match_review.csv", index=False)
        logger.info("Probable matches: %d pairs", len(review_df))
    else:
        pd.DataFrame(columns=["record_1_id", "record_2_id", "similarity_score",
                               "hire_date_diff_days", "recommended_action"]
                     ).to_csv(OUTPUT_DIR / "probable_match_review.csv", index=False)
        logger.info("Probable matches: 0 pairs (empty file written)")


def run_pipeline() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "reports").mkdir(exist_ok=True)

    logger.info("=" * 60)
    logger.info("STEP 1: INGESTION")
    logger.info("=" * 60)
    gt_df  = load_globaltech_hris(DATA_DIR / "globaltech_hris.csv")
    ac_df  = load_acquiredco_api(DATA_DIR / "acquiredco_api.json")
    pay_df = load_payroll(DATA_DIR / "payroll_data.xlsx")
    ben_df = load_benefits(DATA_DIR / "benefits_enrollment.xml")
    combined_raw = align_schemas(gt_df, ac_df, pay_df, ben_df)
    export_dead_letters(OUTPUT_DIR / "dead_letters.json")
    logger.info("GT=%d  AC=%d  Payroll=%d  Benefits=%d  -> Combined=%d",
                len(gt_df), len(ac_df), len(pay_df), len(ben_df), len(combined_raw))

    logger.info("=" * 60)
    logger.info("STEP 2: CLEANING & TRANSFORMATION")
    logger.info("=" * 60)
    combined_clean = clean_all(combined_raw)
    export_unmapped_departments(OUTPUT_DIR / "reports" / "unmapped_departments.csv")

    logger.info("=" * 60)
    logger.info("STEP 3: BENEFITS & PAYROLL ENRICHMENT")
    logger.info("=" * 60)
    payroll_clean = combined_clean[combined_clean["source_system"] == "payroll"].copy()
    combined_clean = _enrich_benefits(combined_clean)
    combined_clean = _enrich_payroll(combined_clean)

    logger.info("=" * 60)
    logger.info("STEP 4: DEDUPLICATION")
    logger.info("=" * 60)
    hris_clean = combined_clean[combined_clean["source_system"].isin(["globaltech_hris", "acquiredco_api"])]
    golden_df, ghost_df, review_df = dedup_all(combined_clean, hris_df=hris_clean, payroll_df=payroll_clean)

    valid_ids = set(golden_df["employee_id"].dropna().unique())
    unresolvable = golden_df["manager_id"].notna() & ~golden_df["manager_id"].isin(valid_ids)
    if unresolvable.any():
        logger.info(
            "Manager ID cleanup: %d unresolvable manager_ids set to NA (manager not in golden dataset)",
            int(unresolvable.sum()),
        )
        golden_df.loc[unresolvable, "manager_id"] = pd.NA

    logger.info("=" * 60)
    logger.info("STEP 5: DATA QUALITY VALIDATION")
    logger.info("=" * 60)
    try:
        validator = DataQualityValidator(golden_df)
        validation_report = validator.run_all()
        validator.export(validation_report, OUTPUT_DIR / "reports")
    except PipelineGateError as err:
        logger.critical("Pipeline halted: %s", err)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("STEP 6: EXPORT OUTPUTS")
    logger.info("=" * 60)
    _export_outputs(golden_df, ghost_df, review_df)

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE — outputs in: %s", OUTPUT_DIR.resolve())
    logger.info("EDA: open notebooks/eda_report.ipynb and run all cells")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
