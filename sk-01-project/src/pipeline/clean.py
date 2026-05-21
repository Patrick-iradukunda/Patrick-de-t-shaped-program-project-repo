from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger("pipeline.clean")

EXCHANGE_RATES_TO_USD: dict[str, float] = {"USD": 1.00, "EUR": 1.09, "GBP": 1.27}

PAY_FREQUENCY_MULTIPLIER: dict[str, float] = {
    "annual": 1, "yearly": 1,
    "monthly": 12,
    "bi-weekly": 26, "biweekly": 26, "bi_weekly": 26,
    "weekly": 52,
    "semi-monthly": 24,
}

DEPT_TAXONOMY: dict[str, str] = {
    "ENG-01": "Engineering", "ENG-02": "Engineering",
    "MKT-01": "Marketing",  "MKT-02": "Marketing",  "MKT-03": "Marketing",
    "HRS-01": "Human Resources",
    "FIN-01": "Finance",    "FIN-02": "Finance",
    "OPS-01": "Operations", "OPS-02": "Operations",
    "SLS-01": "Sales",      "SLS-02": "Sales",
    "PDT-01": "Product",    "PDT-02": "Product",
    "DAT-01": "Data Science", "DAT-02": "Data Science",
    "STR-01": "Strategy",
    "MFG-01": "Manufacturing",
    "LGL-01": "Legal",
    "IT-01": "IT", "IT-02": "IT",
    "Engineering": "Engineering", "Eng": "Engineering",
    "Marketing": "Marketing",     "Mktg": "Marketing",
    "Human Resources": "Human Resources", "HR": "Human Resources",
    "Finance": "Finance", "Accounting": "Finance",
    "Operations": "Operations", "Ops": "Operations",
    "Sales": "Sales",
    "Product": "Product", "Product Management": "Product",
    "Data Science": "Data Science", "Data Analytics": "Data Science", "Analytics": "Data Science",
    "Strategy": "Strategy",
    "Manufacturing": "Manufacturing",
    "Legal": "Legal",
    "IT": "IT", "Information Technology": "IT",
    "Customer Success": "Customer Success", "Customer Support": "Customer Success",
    "Design": "Design", "UX": "Design",
    "Research": "Research", "R&D": "Research",
    "Business Development": "Business Development",
    "Communications": "Communications",
    "DevOps": "DevOps",
    "Quality Assurance": "Quality Assurance",
    "Supply Chain": "Supply Chain",
}

EMPLOYMENT_TYPE_MAP: dict[str, str] = {
    "full-time": "Full-Time", "fulltime": "Full-Time", "full time": "Full-Time", "ft": "Full-Time",
    "part-time": "Part-Time", "parttime": "Part-Time", "part time": "Part-Time", "pt": "Part-Time",
    "contractor": "Contractor", "contract": "Contractor", "independent": "Contractor",
}

_LOWERCASE_PARTICLES = {"van", "der", "de", "la", "le", "von", "di", "da", "del", "dos", "das"}
_UNMAPPED_DEPARTMENTS: set[str] = set()


def _clean_name_field(raw: object) -> Optional[str]:
    s = str(raw).strip()
    if not s or s in ("nan", "None"):
        return pd.NA
    normalised = unicodedata.normalize("NFC", s)
    parts = re.split(r"([-\s]+)", normalised)
    result = []
    for part in parts:
        if re.match(r"[-\s]+", part):
            result.append(part)
        elif part.lower() in _LOWERCASE_PARTICLES:
            result.append(part.lower())
        elif re.match(r"o'", part, re.IGNORECASE):
            result.append("O'" + part[2:].capitalize())
        elif re.match(r"mc", part, re.IGNORECASE):
            result.append("Mc" + part[2:].capitalize())
        elif re.match(r"mac", part, re.IGNORECASE):
            result.append("Mac" + part[3:].capitalize())
        else:
            result.append(part.capitalize())
    return "".join(result)


def normalize_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("first_name", "last_name"):
        if col in df.columns:
            df[col] = df[col].apply(_clean_name_field)
    return df


def _build_namespaced_id(raw: object, prefix: str) -> Optional[str]:
    numeric = re.sub(r"[^0-9]", "", str(raw))
    return f"{prefix}-{int(numeric):06d}" if numeric else pd.NA


def resolve_employee_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def _to_id(row: pd.Series, col: str) -> Optional[str]:
        raw = str(row.get(col, "")).strip()
        if not raw or raw in ("nan", "None"):
            return pd.NA
        prefix = "GT" if row.get("company_origin") == "GlobalTech" else "AC"
        return _build_namespaced_id(raw, prefix)

    df["employee_id"] = df.apply(lambda r: _to_id(r, "source_employee_id"), axis=1)
    df["manager_id"]  = df.apply(lambda r: _to_id(r, "manager_id"), axis=1)
    logger.info("Employee ID resolution: %d IDs generated", df["employee_id"].notna().sum())
    return df


def _parse_salary_string(raw: object) -> Optional[float]:
    s = str(raw).strip()
    if not s or s in ("nan", "None"):
        return pd.NA
    cleaned = re.sub(r"[^\d.]", "", s)
    try:
        return float(cleaned) if cleaned else pd.NA
    except ValueError:
        return pd.NA


def normalize_currency(df: pd.DataFrame, rates: dict[str, float] = EXCHANGE_RATES_TO_USD) -> pd.DataFrame:
    df = df.copy()
    df["base_salary_numeric"] = df["base_salary"].apply(_parse_salary_string)
    freq = df["pay_frequency"].astype(str).str.strip().str.lower().str.replace(r"\s+", "-", regex=True)
    df["_mult"] = freq.map(PAY_FREQUENCY_MULTIPLIER).fillna(1.0)
    df["base_salary_annual"] = df["base_salary_numeric"] * df["_mult"]
    currency = df["salary_currency"].astype(str).str.strip().str.upper()
    unmapped = set(currency.unique()) - set(rates) - {"NAN", "NONE", "<NA>", ""}
    if unmapped:
        logger.warning("Unknown currencies (rate=1.0 applied): %s", unmapped)
    df["_rate"] = currency.map(rates).fillna(1.0)
    df["salary_usd_annual"] = pd.to_numeric(df["base_salary_annual"] * df["_rate"], errors="coerce").round(2)
    df.drop(columns=["_mult", "_rate"], inplace=True)

    out_of_range = df["salary_usd_annual"].notna() & (
        (df["salary_usd_annual"] < 15_000) | (df["salary_usd_annual"] > 2_000_000)
    )
    if out_of_range.any():
        logger.warning(
            "Salary clamping: %d records have salary_usd_annual outside [15000, 2000000] — set to NA",
            int(out_of_range.sum()),
        )
        df.loc[out_of_range, "salary_usd_annual"] = pd.NA

    logger.info("Currency: %d records have salary_usd_annual", df["salary_usd_annual"].notna().sum())
    return df


def _map_dept(raw: object) -> Optional[str]:
    s = str(raw).strip() if raw is not None else ""
    if not s or s in ("nan", "None", "<NA>"):
        return pd.NA
    if s in DEPT_TAXONOMY:
        return DEPT_TAXONOMY[s]
    for k, v in DEPT_TAXONOMY.items():
        if k.lower() == s.lower():
            return v
    return pd.NA


def map_departments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["department_canonical"] = df["department"].astype(str).str.strip().apply(_map_dept)
    unmapped_mask = df["department_canonical"].isna() & df["department"].notna()
    new_unmapped = set(df.loc[unmapped_mask, "department"].unique()) - _UNMAPPED_DEPARTMENTS
    if new_unmapped:
        logger.warning("Unmapped departments (flagged for review): %s", sorted(new_unmapped))
        _UNMAPPED_DEPARTMENTS.update(new_unmapped)
    df.loc[unmapped_mask, "department_canonical"] = df.loc[unmapped_mask, "department"]
    return df


def export_unmapped_departments(output_path: str | Path) -> None:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"raw_department": sorted(_UNMAPPED_DEPARTMENTS)}).to_csv(out, index=False)
    logger.info("Unmapped departments: %d values -> %s", len(_UNMAPPED_DEPARTMENTS), out)


def _parse_date_flexible(raw: object) -> pd.Timestamp:
    s = str(raw).strip()
    if not s or s in ("nan", "None", "NaT"):
        return pd.NaT
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                "%m/%d/%Y", "%d-%b-%Y", "%d/%m/%Y", "%B %d, %Y"):
        try:
            return pd.Timestamp(pd.to_datetime(s, format=fmt))
        except Exception:
            continue
    try:
        return pd.Timestamp(pd.to_datetime(s, infer_datetime_format=True))
    except Exception:
        return pd.NaT


def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    today = pd.Timestamp.today().normalize()
    lower = pd.Timestamp("1970-01-01")
    for col in ("hire_date", "enrollment_date"):
        if col not in df.columns:
            continue
        df[col] = df[col].apply(_parse_date_flexible)
        flag = f"{col}_flag"
        df[flag] = "ok"
        df.loc[df[col] < lower, flag] = "before_1970"
        df.loc[df[col] > today, flag] = "future_date"
        df.loc[df[col].isna(), flag]  = "unparseable"
        flagged = (df[flag] != "ok").sum()
        if flagged:
            logger.warning("Date flag: %d %s values out of range", flagged, col)
    return df


def normalize_employment_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["employment_type"] = (
        df["employment_type"].astype(str).str.strip().str.lower().map(EMPLOYMENT_TYPE_MAP)
    )
    return df


def run_all(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning pipeline on %d rows", len(df))
    df = normalize_names(df)
    df = resolve_employee_ids(df)
    df = normalize_employment_type(df)
    df = normalize_currency(df)
    df = map_departments(df)
    df = standardize_dates(df)
    logger.info("Cleaning complete: %d rows", len(df))
    return df
