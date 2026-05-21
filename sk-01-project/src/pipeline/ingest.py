from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

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

def _drop_invalid(df: pd.DataFrame, source: str, cols: list[str], reason: str) -> pd.DataFrame:
    if df.empty:
        return df
    valid = df[cols].notna().all(axis=1)
    for _, row in df[~valid].iterrows():
        _dead_letter(source, row.to_dict(), reason)
    return df[valid].reset_index(drop=True)


def load_globaltech_hris(filepath: str | Path) -> pd.DataFrame:
    path = Path(filepath)
    if not path.exists():
        logger.error("File not found: %s", path); return _empty_df()
    try:
        raw = pd.read_csv(path, dtype=str, encoding="utf-8", na_values=["", "N/A", "NULL"])
        logger.info("GlobalTech HRIS: %d raw rows", len(raw))
    except Exception as exc:
        logger.error("Read failed: %s", exc); return _empty_df()
    if missing := {"employee_id", "first_name", "last_name"} - set(raw.columns):
        logger.error("Missing required columns: %s", missing); return _empty_df()
    raw = _drop_invalid(raw, "globaltech_hris", ["employee_id", "first_name", "last_name"], "Missing employee_id or name")
    df = raw.rename(columns={"employee_id": "source_employee_id"})
    df["employment_status"] = "Active"
    df["benefits_enrolled"] = False
    df["company_origin"] = "GlobalTech"
    df["source_system"] = "globaltech_hris"
    return df.reindex(columns=STANDARD_COLUMNS)


def load_acquiredco_api(filepath: str | Path, page_size: int = 200) -> pd.DataFrame:
    path = Path(filepath)
    if not path.exists():
        logger.error("File not found: %s", path); return _empty_df()
    try:
        employees = json.loads(path.read_text(encoding="utf-8")).get("employees", [])
    except Exception as exc:
        logger.error("JSON parse failed: %s", exc); return _empty_df()
    logger.info("AcquiredCo API: %d total records (page_size=%d)", len(employees), page_size)
    pages = []
    for i in range(0, max(len(employees), 1), page_size):
        chunk = employees[i : i + page_size]
        if not chunk:
            break
        p = i // page_size + 1
        logger.info("  Page %d: %d records", p, len(chunk))
        pages.append(_parse_acquiredco_page(chunk, p))
    if not pages:
        return _empty_df()
    df = pd.concat(pages, ignore_index=True)
    logger.info("AcquiredCo API: %d records ingested across %d pages", len(df), len(pages))
    return df


def _parse_acquiredco_page(records: list[dict], page: int) -> pd.DataFrame:
    rows = []
    for rec in records:
        try:
            n, c, a, e = (rec.get(k, {}) for k in ("name", "contact", "assignment", "employment"))
            raw_type = str(e.get("type", "")).upper()
            rows.append({
                "source_employee_id": rec.get("employee_identifier"),
                "first_name": n.get("first"), "last_name": n.get("last"),
                "email": c.get("email"), "department": a.get("department"),
                "job_title": a.get("role"), "hire_date": a.get("hire_timestamp"),
                "country": a.get("location"),
                "employment_type": _EMP_TYPE_MAP.get(raw_type, raw_type),
                "employment_status": e.get("status", "Active"),
                "manager_id": rec.get("manager_employee_id"),
                "benefits_enrolled": False, "company_origin": "AcquiredCo", "source_system": "acquiredco_api",
            })
        except Exception as exc:
            _dead_letter("acquiredco_api", rec, f"Parse error page {page}: {exc}")
    df = pd.DataFrame(rows)
    return _drop_invalid(df, "acquiredco_api", ["source_employee_id", "first_name", "last_name"], "Missing identifier or name")


def load_payroll(filepath: str | Path) -> pd.DataFrame:
    path = Path(filepath)
    if not path.exists():
        logger.error("File not found: %s", path); return _empty_df()
    raw = None
    for reader, kwargs in [
        (pd.read_excel, {"dtype": str}),
        (pd.read_csv, {"sep": "\t", "dtype": str, "encoding": "utf-8"}),
        (pd.read_csv, {"sep": ",",  "dtype": str, "encoding": "utf-8"}),
    ]:
        try:
            cand = reader(path, **kwargs)
            if len(cand.columns) >= 3:
                raw = cand; logger.info("Payroll: %d rows via %s", len(raw), reader.__name__); break
        except Exception:
            continue
    if raw is None or raw.empty:
        logger.error("Payroll: all read attempts failed"); return _empty_df()
    if "employee_id" not in raw.columns:
        logger.error("Payroll: missing employee_id column"); return _empty_df()
    raw = _drop_invalid(raw, "payroll", ["employee_id"], "Missing employee_id")
    def _origin(s: Any) -> str:
        t = str(s).lower() if not pd.isna(s) else ""
        return "GlobalTech" if "global" in t else ("AcquiredCo" if "acquired" in t else (str(s) if not pd.isna(s) else "Unknown"))
    src = raw.get("source", pd.Series(["Unknown"] * len(raw)))
    return pd.DataFrame({
        "source_employee_id": raw["employee_id"], "benefits_enrolled": False,
        "base_salary": raw.get("base_salary", pd.NA), "salary_currency": raw.get("currency", pd.NA),
        "pay_frequency": raw.get("pay_frequency", pd.NA), "bonus_target_pct": raw.get("bonus_target_pct", pd.NA),
        "company_origin": src.apply(_origin), "source_system": "payroll",
    }).reindex(columns=STANDARD_COLUMNS)


def load_benefits(filepath: str | Path) -> pd.DataFrame:
    path = Path(filepath)
    if not path.exists():
        logger.error("File not found: %s", path); return _empty_df()
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        logger.error("XML parse error: %s", exc); return _empty_df()
    rows, bad = [], 0
    for elem in root:
        try:
            emp_id = elem.findtext("employee_id")
            if not emp_id:
                bad += 1; _dead_letter("benefits", ET.tostring(elem, encoding="unicode")[:200], "Missing employee_id"); continue
            rows.append({
                "source_employee_id": emp_id, "benefits_enrolled": True,
                "plan_type": elem.findtext("plan_type"), "coverage_level": elem.findtext("coverage_level"),
                "enrollment_date": elem.findtext("enrollment_date"),
                "company_origin": "GlobalTech", "source_system": "benefits",
            })
        except Exception as exc:
            bad += 1; _dead_letter("benefits", {}, str(exc))
    logger.info("Benefits XML: %d records ingested (%d dead-letter)", len(rows), bad)
    return pd.DataFrame(rows).reindex(columns=STANDARD_COLUMNS).reset_index(drop=True)


def align_schemas(*dfs: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([df.reindex(columns=STANDARD_COLUMNS) for df in dfs], ignore_index=True)
    logger.info("Schema alignment: %d total rows", len(combined))
    return combined


def export_dead_letters(output_path: str | Path) -> None:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(_DEAD_LETTERS, indent=2, default=str), encoding="utf-8")
    logger.info("Dead letters: %d records -> %s", len(_DEAD_LETTERS), out)
