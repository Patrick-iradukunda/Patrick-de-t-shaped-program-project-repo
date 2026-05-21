from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd

logger = logging.getLogger("pipeline.clean")

EXCHANGE_RATES_TO_USD: dict[str, float] = {"USD": 1.00, "EUR": 1.09, "GBP": 1.27}

PAY_FREQUENCY_MULTIPLIER: dict[str, float] = {
    "annual": 1, "yearly": 1, "monthly": 12, "weekly": 52, "semi-monthly": 24,
    "bi-weekly": 26, "biweekly": 26, "bi_weekly": 26,
}

DEPT_TAXONOMY: dict[str, str] = {
    "ENG-01": "Engineering", "ENG-02": "Engineering", "Engineering": "Engineering", "Eng": "Engineering",
    "MKT-01": "Marketing", "MKT-02": "Marketing", "MKT-03": "Marketing", "Marketing": "Marketing", "Mktg": "Marketing",
    "HRS-01": "Human Resources", "Human Resources": "Human Resources", "HR": "Human Resources",
    "FIN-01": "Finance", "FIN-02": "Finance", "Finance": "Finance", "Accounting": "Finance",
    "OPS-01": "Operations", "OPS-02": "Operations", "Operations": "Operations", "Ops": "Operations",
    "SLS-01": "Sales", "SLS-02": "Sales", "Sales": "Sales",
    "PDT-01": "Product", "PDT-02": "Product", "Product": "Product", "Product Management": "Product",
    "DAT-01": "Data Science", "DAT-02": "Data Science",
    "Data Science": "Data Science", "Data Analytics": "Data Science", "Analytics": "Data Science",
    "STR-01": "Strategy", "Strategy": "Strategy",
    "MFG-01": "Manufacturing", "Manufacturing": "Manufacturing",
    "LGL-01": "Legal", "Legal": "Legal",
    "IT-01": "IT", "IT-02": "IT", "IT": "IT", "Information Technology": "IT",
    "Customer Success": "Customer Success", "Customer Support": "Customer Success",
    "Design": "Design", "UX": "Design", "Research": "Research", "R&D": "Research",
    "Business Development": "Business Development", "Communications": "Communications",
    "DevOps": "DevOps", "Quality Assurance": "Quality Assurance", "Supply Chain": "Supply Chain",
}

EMPLOYMENT_TYPE_MAP: dict[str, str] = {
    "full-time": "Full-Time", "fulltime": "Full-Time", "full time": "Full-Time", "ft": "Full-Time",
    "part-time": "Part-Time", "parttime": "Part-Time", "part time": "Part-Time", "pt": "Part-Time",
    "contractor": "Contractor", "contract": "Contractor", "independent": "Contractor",
}

_PARTICLES = {"van", "der", "de", "la", "le", "von", "di", "da", "del", "dos", "das"}
_UNMAPPED: set[str] = set()


def _clean_name(raw: object) -> object:
    s = unicodedata.normalize("NFC", str(raw).strip())
    if not s or s in ("nan", "None"):
        return pd.NA
    out = []
    for p in re.split(r"([-\s]+)", s):
        if re.match(r"[-\s]+", p):       out.append(p)
        elif p.lower() in _PARTICLES:    out.append(p.lower())
        elif re.match(r"o'",  p, re.I):  out.append("O'" + p[2:].capitalize())
        elif re.match(r"mc",  p, re.I):  out.append("Mc" + p[2:].capitalize())
        elif re.match(r"mac", p, re.I):  out.append("Mac" + p[3:].capitalize())
        else:                            out.append(p.capitalize())
    return "".join(out)


def normalize_names(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(**{c: df[c].apply(_clean_name) for c in ("first_name", "last_name") if c in df.columns})


def _namespaced_id(raw: object, prefix: str) -> object:
    n = re.sub(r"[^0-9]", "", str(raw))
    return f"{prefix}-{int(n):06d}" if n else pd.NA


def resolve_employee_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    def _to_id(row: pd.Series, col: str) -> object:
        v = str(row.get(col, "")).strip()
        if not v or v in ("nan", "None"):
            return pd.NA
        return _namespaced_id(v, "GT" if row.get("company_origin") == "GlobalTech" else "AC")
    df["employee_id"] = df.apply(lambda r: _to_id(r, "source_employee_id"), axis=1)
    df["manager_id"]  = df.apply(lambda r: _to_id(r, "manager_id"), axis=1)
    logger.info("Employee ID resolution: %d IDs generated", df["employee_id"].notna().sum())
    return df


def normalize_currency(df: pd.DataFrame, rates: dict[str, float] = EXCHANGE_RATES_TO_USD) -> pd.DataFrame:
    df = df.copy()
    df["base_salary_numeric"] = pd.to_numeric(
        df["base_salary"].astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce"
    )
    freq = df["pay_frequency"].astype(str).str.strip().str.lower().str.replace(r"\s+", "-", regex=True)
    mult = freq.map(PAY_FREQUENCY_MULTIPLIER).fillna(1.0)
    currency = df["salary_currency"].astype(str).str.strip().str.upper()
    unmapped = set(currency.unique()) - set(rates) - {"NAN", "NONE", "<NA>", ""}
    if unmapped:
        logger.warning("Unknown currencies (rate=1.0 applied): %s", unmapped)
    df["base_salary_annual"] = df["base_salary_numeric"] * mult
    df["salary_usd_annual"] = (df["base_salary_annual"] * currency.map(rates).fillna(1.0)).round(2)
    oor = df["salary_usd_annual"].notna() & ((df["salary_usd_annual"] < 15_000) | (df["salary_usd_annual"] > 2_000_000))
    if oor.any():
        logger.warning("Salary clamping: %d out-of-range values -> NA", int(oor.sum()))
        df.loc[oor, "salary_usd_annual"] = pd.NA
    logger.info("Currency: %d records have salary_usd_annual", df["salary_usd_annual"].notna().sum())
    return df


def _map_dept(raw: object) -> object:
    s = str(raw).strip() if raw is not None else ""
    if not s or s in ("nan", "None", "<NA>"):
        return pd.NA
    return DEPT_TAXONOMY.get(s) or next(
        (v for k, v in DEPT_TAXONOMY.items() if k.lower() == s.lower()), pd.NA
    )


def map_departments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["department_canonical"] = df["department"].astype(str).str.strip().apply(_map_dept)
    unmapped_mask = df["department_canonical"].isna() & df["department"].notna()
    new_unmapped = set(df.loc[unmapped_mask, "department"].unique()) - _UNMAPPED
    if new_unmapped:
        logger.warning("Unmapped departments (flagged for review): %s", sorted(new_unmapped))
        _UNMAPPED.update(new_unmapped)
    df.loc[unmapped_mask, "department_canonical"] = df.loc[unmapped_mask, "department"]
    return df


def export_unmapped_departments(output_path: str | Path) -> None:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"raw_department": sorted(_UNMAPPED)}).to_csv(out, index=False)
    logger.info("Unmapped departments: %d values -> %s", len(_UNMAPPED), out)


def _parse_date(raw: object) -> pd.Timestamp:
    s = str(raw).strip()
    if not s or s in ("nan", "None", "NaT"):
        return pd.NaT
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                "%m/%d/%Y", "%d-%b-%Y", "%d/%m/%Y", "%B %d, %Y"):
        try:
            return pd.Timestamp(pd.to_datetime(s, format=fmt))
        except Exception:
            pass
    try:
        return pd.Timestamp(pd.to_datetime(s, infer_datetime_format=True))
    except Exception:
        return pd.NaT


def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    today, lower = pd.Timestamp.today().normalize(), pd.Timestamp("1970-01-01")
    for col in ("hire_date", "enrollment_date"):
        if col not in df.columns:
            continue
        df[col] = df[col].apply(_parse_date)
        df[f"{col}_flag"] = "ok"
        df.loc[df[col] < lower, f"{col}_flag"] = "before_1970"
        df.loc[df[col] > today, f"{col}_flag"] = "future_date"
        df.loc[df[col].isna(), f"{col}_flag"] = "unparseable"
        flagged = (df[f"{col}_flag"] != "ok").sum()
        if flagged:
            logger.warning("Date flag: %d %s values out of range", flagged, col)
    return df


def normalize_employment_type(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(employment_type=df["employment_type"].astype(str).str.strip().str.lower().map(EMPLOYMENT_TYPE_MAP))


def run_all(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning pipeline on %d rows", len(df))
    for fn in (normalize_names, resolve_employee_ids, normalize_employment_type,
               normalize_currency, map_departments, standardize_dates):
        df = fn(df)
    logger.info("Cleaning complete: %d rows", len(df))
    return df
