from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger("pipeline.ingest")

STANDARD_COLUMNS = [
    "source_employee_id", "first_name", "last_name", "email",
    "department", "job_title", "hire_date", "country",
    "employment_type", "employment_status", "manager_id",
    "base_salary", "salary_currency", "pay_frequency", "bonus_target_pct",
    "benefits_enrolled", "plan_type", "coverage_level", "enrollment_date",
    "company_origin", "source_system",
]

_DEAD_LETTERS: list[dict] = []

_EMP_TYPE_MAP = {"FT": "Full-Time", "PT": "Part-Time", "CONTRACTOR": "Contractor"}


def _dead_letter(source: str, record: Any, reason: str) -> None:
    _DEAD_LETTERS.append({"source": source, "reason": reason, "record": str(record)[:300]})
    logger.warning("Dead-letter [%s]: %s", source, reason)


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=STANDARD_COLUMNS)


def load_globaltech_hris(filepath: str | Path) -> pd.DataFrame:
    path = Path(filepath)
    if not path.exists():
        logger.error("File not found: %s", path)
        return _empty_df()
    try:
        raw = pd.read_csv(path, dtype=str, encoding="utf-8", na_values=["", "N/A", "NULL"])
        logger.info("GlobalTech HRIS: %d raw rows", len(raw))
    except Exception as exc:
        logger.error("Read failed: %s", exc)
        return _empty_df()

    if missing := {"employee_id", "first_name", "last_name"} - set(raw.columns):
        logger.error("Missing required columns: %s", missing)
        return _empty_df()

    valid = raw["employee_id"].notna() & raw["first_name"].notna() & raw["last_name"].notna()
    for _, row in raw[~valid].iterrows():
        _dead_letter("globaltech_hris", row.to_dict(), "Missing employee_id or name")
    raw = raw[valid].reset_index(drop=True)

    return pd.DataFrame({
        "source_employee_id": raw["employee_id"],
        "first_name":         raw["first_name"],
        "last_name":          raw["last_name"],
        "email":              raw.get("email", pd.NA),
        "department":         raw.get("department", pd.NA),
        "job_title":          raw.get("job_title", pd.NA),
        "hire_date":          raw.get("hire_date", pd.NA),
        "country":            raw.get("country", pd.NA),
        "employment_type":    raw.get("employment_type", pd.NA),
        "employment_status":  "Active",
        "manager_id":         raw.get("manager_id", pd.NA),
        "base_salary": pd.NA, "salary_currency": pd.NA,
        "pay_frequency": pd.NA, "bonus_target_pct": pd.NA,
        "benefits_enrolled": False, "plan_type": pd.NA,
        "coverage_level": pd.NA, "enrollment_date": pd.NA,
        "company_origin": "GlobalTech",
        "source_system": "globaltech_hris",
    })


def load_acquiredco_api(filepath: str | Path, page_size: int = 200) -> pd.DataFrame:
    path = Path(filepath)
    if not path.exists():
        logger.error("File not found: %s", path)
        return _empty_df()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("JSON parse failed: %s", exc)
        return _empty_df()

    employees: list[dict] = payload.get("employees", [])
    logger.info("AcquiredCo API: %d total records (page_size=%d)", len(employees), page_size)

    pages, page_num = [], 0
    while chunk := employees[page_num * page_size : (page_num + 1) * page_size]:
        logger.info("  Page %d: %d records", page_num + 1, len(chunk))
        pages.append(_parse_acquiredco_page(chunk, page_num + 1))
        page_num += 1

    if not pages:
        return _empty_df()
    df = pd.concat(pages, ignore_index=True)
    logger.info("AcquiredCo API: %d records ingested across %d pages", len(df), page_num)
    return df


def _parse_acquiredco_page(records: list[dict], page: int) -> pd.DataFrame:
    rows = []
    for rec in records:
        try:
            name       = rec.get("name", {})
            contact    = rec.get("contact", {})
            assignment = rec.get("assignment", {})
            employment = rec.get("employment", {})
            raw_type   = str(employment.get("type", "")).upper()
            rows.append({
                "source_employee_id": rec.get("employee_identifier"),
                "first_name":         name.get("first"),
                "last_name":          name.get("last"),
                "email":              contact.get("email"),
                "department":         assignment.get("department"),
                "job_title":          assignment.get("role"),
                "hire_date":          assignment.get("hire_timestamp"),
                "country":            assignment.get("location"),
                "employment_type":    _EMP_TYPE_MAP.get(raw_type, raw_type),
                "employment_status":  employment.get("status", "Active"),
                "manager_id":         rec.get("manager_employee_id"),
                "base_salary": pd.NA, "salary_currency": pd.NA,
                "pay_frequency": pd.NA, "bonus_target_pct": pd.NA,
                "benefits_enrolled": False, "plan_type": pd.NA,
                "coverage_level": pd.NA, "enrollment_date": pd.NA,
                "company_origin": "AcquiredCo",
                "source_system": "acquiredco_api",
            })
        except Exception as exc:
            _dead_letter("acquiredco_api", rec, f"Parse error page {page}: {exc}")

    df = pd.DataFrame(rows)
    valid = df["source_employee_id"].notna() & df["first_name"].notna() & df["last_name"].notna()
    for _, row in df[~valid].iterrows():
        _dead_letter("acquiredco_api", row.to_dict(), "Missing identifier or name")
    return df[valid].reset_index(drop=True)


def load_payroll(filepath: str | Path) -> pd.DataFrame:
    path = Path(filepath)
    if not path.exists():
        logger.error("File not found: %s", path)
        return _empty_df()

    raw: Optional[pd.DataFrame] = None
    for reader, kwargs in [
        (pd.read_excel, {"dtype": str}),
        (pd.read_csv, {"sep": "\t", "dtype": str, "encoding": "utf-8"}),
        (pd.read_csv, {"sep": ",",  "dtype": str, "encoding": "utf-8"}),
    ]:
        try:
            cand = reader(path, **kwargs)
            if len(cand.columns) >= 3:
                raw = cand
                logger.info("Payroll: %d rows via %s", len(raw), reader.__name__)
                break
        except Exception:
            continue

    if raw is None or raw.empty:
        logger.error("Payroll: all read attempts failed")
        return _empty_df()

    if "employee_id" not in raw.columns:
        logger.error("Payroll: missing employee_id column")
        return _empty_df()

    valid = raw["employee_id"].notna()
    for _, row in raw[~valid].iterrows():
        _dead_letter("payroll", row.to_dict(), "Missing employee_id")
    raw = raw[valid].reset_index(drop=True)

    def _origin(s: Any) -> str:
        if pd.isna(s):
            return "Unknown"
        t = str(s).lower()
        return "GlobalTech" if "global" in t else ("AcquiredCo" if "acquired" in t else str(s))

    src = raw.get("source", pd.Series(["Unknown"] * len(raw)))
    return pd.DataFrame({
        "source_employee_id": raw["employee_id"],
        "first_name": pd.NA, "last_name": pd.NA, "email": pd.NA,
        "department": pd.NA, "job_title": pd.NA, "hire_date": pd.NA,
        "country": pd.NA, "employment_type": pd.NA, "employment_status": pd.NA,
        "manager_id": pd.NA,
        "base_salary":     raw.get("base_salary", pd.NA),
        "salary_currency": raw.get("currency", pd.NA),
        "pay_frequency":   raw.get("pay_frequency", pd.NA),
        "bonus_target_pct": raw.get("bonus_target_pct", pd.NA),
        "benefits_enrolled": False, "plan_type": pd.NA,
        "coverage_level": pd.NA, "enrollment_date": pd.NA,
        "company_origin": src.apply(_origin),
        "source_system": "payroll",
    })


def load_benefits(filepath: str | Path) -> pd.DataFrame:
    path = Path(filepath)
    if not path.exists():
        logger.error("File not found: %s", path)
        return _empty_df()
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        logger.error("XML parse error: %s", exc)
        return _empty_df()

    rows, bad = [], 0
    for elem in root:
        try:
            emp_id = elem.findtext("employee_id")
            if not emp_id:
                bad += 1
                _dead_letter("benefits", ET.tostring(elem, encoding="unicode")[:200], "Missing employee_id")
                continue
            rows.append({
                "source_employee_id": emp_id,
                "first_name": pd.NA, "last_name": pd.NA, "email": pd.NA,
                "department": pd.NA, "job_title": pd.NA, "hire_date": pd.NA,
                "country": pd.NA, "employment_type": pd.NA, "employment_status": pd.NA,
                "manager_id": pd.NA, "base_salary": pd.NA, "salary_currency": pd.NA,
                "pay_frequency": pd.NA, "bonus_target_pct": pd.NA,
                "benefits_enrolled": True,
                "plan_type":      elem.findtext("plan_type"),
                "coverage_level": elem.findtext("coverage_level"),
                "enrollment_date": elem.findtext("enrollment_date"),
                "company_origin": "GlobalTech",
                "source_system": "benefits",
            })
        except Exception as exc:
            bad += 1
            _dead_letter("benefits", {}, str(exc))

    logger.info("Benefits XML: %d records ingested (%d dead-letter)", len(rows), bad)
    return pd.DataFrame(rows).reset_index(drop=True)


def align_schemas(*dfs: pd.DataFrame) -> pd.DataFrame:
    aligned = []
    for df in dfs:
        df = df.copy()
        for col in STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        aligned.append(df[STANDARD_COLUMNS])
    combined = pd.concat(aligned, ignore_index=True)
    logger.info("Schema alignment: %d total rows", len(combined))
    return combined


def export_dead_letters(output_path: str | Path) -> None:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(_DEAD_LETTERS, fh, indent=2, default=str)
    logger.info("Dead letters: %d records -> %s", len(_DEAD_LETTERS), out)
