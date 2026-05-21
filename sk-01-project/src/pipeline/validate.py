from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

logger = logging.getLogger("pipeline.validate")

GATE_MAX_FAILURES = 2


class PipelineGateError(RuntimeError):
    pass


@dataclass
class _Result:
    check: str
    description: str
    total: int
    passed: int
    failed: int
    pass_rate: float
    status: str
    severity: str = "CRITICAL"


class DataQualityValidator:
    def __init__(self, df: pd.DataFrame, fail_threshold: float = 0.95) -> None:
        self.df = df.copy()
        self.fail_threshold = fail_threshold
        self._results: list[_Result] = []

    def _run(self, name: str, desc: str, mask: pd.Series, severity: str = "CRITICAL") -> _Result:
        total  = len(mask)
        passed = int(mask.sum())
        rate   = passed / total if total > 0 else 0.0
        status = "PASS" if rate >= self.fail_threshold else ("WARN" if rate >= 0.80 else "FAIL")
        r = _Result(name, desc, total, passed, total - passed, round(rate, 4), status, severity)
        self._results.append(r)
        fn = logger.info if status == "PASS" else (logger.warning if status == "WARN" else logger.error)
        fn("[%s] %s — %.1f%% pass", status, name, rate * 100)
        return r

    def check_not_null(self, col: str) -> _Result:
        mask = self.df[col].notna() if col in self.df.columns else pd.Series([], dtype=bool)
        return self._run(f"NOT_NULL_{col.upper()}", f"'{col}' must not be null", mask)

    def check_unique(self, col: str) -> _Result:
        if col not in self.df.columns:
            return self._run(f"UNIQUE_{col.upper()}", f"'{col}' missing", pd.Series([], dtype=bool))
        counts = self.df[col].value_counts(dropna=True)
        mask = self.df[col].apply(lambda v: pd.isna(v) or counts.get(v, 0) == 1)
        return self._run(f"UNIQUE_{col.upper()}", f"'{col}' must be unique", mask)

    def check_values_in_set(self, col: str, allowed: set, severity: str = "CRITICAL") -> _Result:
        if col not in self.df.columns:
            return self._run(f"IN_SET_{col.upper()}", f"'{col}' missing", pd.Series([], dtype=bool), severity)
        mask = self.df[col].isna() | self.df[col].isin(allowed)
        return self._run(f"IN_SET_{col.upper()}", f"'{col}' in {sorted(allowed)}", mask, severity)

    def check_regex(self, col: str, pattern: str, description: str = "") -> _Result:
        if col not in self.df.columns:
            return self._run(f"REGEX_{col.upper()}", description, pd.Series([], dtype=bool))
        mask = self.df[col].isna() | self.df[col].astype(str).str.match(re.compile(pattern))
        return self._run(f"REGEX_{col.upper()}", description or f"'{col}' matches /{pattern}/", mask)

    def check_numeric_range(self, col: str, lo: float, hi: float, severity: str = "CRITICAL") -> _Result:
        if col not in self.df.columns:
            return self._run(f"RANGE_{col.upper()}", f"'{col}' missing", pd.Series([], dtype=bool), severity)
        num = pd.to_numeric(self.df[col], errors="coerce")
        mask = num.isna() | ((num >= lo) & (num <= hi))
        return self._run(f"RANGE_{col.upper()}", f"'{col}' between {lo:,.0f} and {hi:,.0f}", mask, severity)

    def check_date_range(self, col: str, lo: str, hi: str, severity: str = "CRITICAL") -> _Result:
        if col not in self.df.columns:
            return self._run(f"DATE_{col.upper()}", f"'{col}' missing", pd.Series([], dtype=bool), severity)
        dates = pd.to_datetime(self.df[col], errors="coerce")
        mask = dates.isna() | ((dates >= pd.Timestamp(lo)) & (dates <= pd.Timestamp(hi)))
        return self._run(f"DATE_{col.upper()}", f"'{col}' between {lo} and {hi}", mask, severity)

    def check_referential_integrity(self, fk: str, pk: str, severity: str = "CRITICAL") -> _Result:
        name = f"REF_{fk.upper()}_FK"
        desc = f"Non-null '{fk}' must exist as '{pk}'"
        if fk not in self.df.columns or pk not in self.df.columns:
            return self._run(name, desc, pd.Series([], dtype=bool), severity)
        valid = set(self.df[pk].dropna().unique())
        mask = self.df[fk].isna() | self.df[fk].isin(valid)
        return self._run(name, desc, mask, severity)

    def run_all(self) -> pd.DataFrame:
        today = pd.Timestamp.today().strftime("%Y-%m-%d")

        for col in ("employee_id", "first_name", "last_name", "email", "department_canonical", "country"):
            self.check_not_null(col)

        self.check_unique("email")
        self.check_unique("employee_id")
        self.check_values_in_set("employment_type", {"Full-Time", "Part-Time", "Contractor"})
        self.check_values_in_set("salary_currency", {"USD", "EUR", "GBP"}, severity="WARNING")
        self.check_regex("email", r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", "Valid email format")
        self.check_regex("employee_id", r"^(GT|AC)-\d{6}$", "employee_id matches GT/AC-XXXXXX")
        self.check_numeric_range("salary_usd_annual", 15_000, 2_000_000)
        self.check_date_range("hire_date", "1970-01-01", today)
        self.check_referential_integrity("manager_id", "employee_id")

        report = self._to_df()
        failed_critical = report[(report["status"] == "FAIL") & (report["severity"] == "CRITICAL")]
        if len(failed_critical) > GATE_MAX_FAILURES:
            msg = (
                f"PIPELINE GATE: {len(failed_critical)} critical checks failed "
                f"(max={GATE_MAX_FAILURES}). Checks: {list(failed_critical['check'])}"
            )
            logger.critical(msg)
            raise PipelineGateError(msg)

        return report

    def _to_df(self) -> pd.DataFrame:
        return pd.DataFrame([
            {"check": r.check, "description": r.description, "total": r.total,
             "passed": r.passed, "failed": r.failed, "pass_rate": r.pass_rate,
             "status": r.status, "severity": r.severity}
            for r in self._results
        ])

    def export(self, report: pd.DataFrame, output_dir: str | Path) -> None:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        report.to_csv(out / "validation_report.csv", index=False)
        (out / "validation_report.html").write_text(_build_html(report), encoding="utf-8")
        logger.info("Validation report written to %s", out)


def _build_html(report: pd.DataFrame) -> str:
    STATUS_BG = {"PASS": "#d4edda", "WARN": "#fff3cd", "FAIL": "#f8d7da"}
    rows_html = ""
    for _, row in report.iterrows():
        bg = STATUS_BG.get(row["status"], "#ffffff")
        rows_html += (
            f"<tr style='background:{bg}'>"
            f"<td>{row['check']}</td>"
            f"<td>{row['description']}</td>"
            f"<td>{int(row['total']):,}</td>"
            f"<td>{int(row['passed']):,}</td>"
            f"<td>{int(row['failed']):,}</td>"
            f"<td>{float(row['pass_rate']):.1%}</td>"
            f"<td><b>{row['status']}</b></td>"
            f"<td>{row['severity']}</td>"
            "</tr>"
        )
    css = (
        "body{font-family:Arial,sans-serif;margin:2em}"
        "table{border-collapse:collapse;width:100%}"
        "th,td{border:1px solid #ccc;padding:8px 12px;text-align:left}"
        "th{background:#4a4a8a;color:white}"
    )
    headers = "<tr>" + "".join(
        f"<th>{h}</th>" for h in ["Check", "Description", "Total", "Passed", "Failed", "Pass Rate", "Status", "Severity"]
    ) + "</tr>"
    timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>"
        f"<title>Data Quality Report</title><style>{css}</style></head>"
        f"<body><h1>GlobalTech Corp — HR Data Quality Validation Report</h1>"
        f"<p>Generated: {timestamp}</p>"
        f"<table><thead>{headers}</thead><tbody>{rows_html}</tbody></table>"
        f"</body></html>"
    )
