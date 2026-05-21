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
    check: str; description: str; total: int; passed: int
    failed: int; pass_rate: float; status: str; severity: str = "CRITICAL"


class DataQualityValidator:
    def __init__(self, df: pd.DataFrame, fail_threshold: float = 0.95) -> None:
        self.df = df.copy()
        self.fail_threshold = fail_threshold
        self._results: list[_Result] = []

    def _run(self, name: str, desc: str, mask: pd.Series, severity: str = "CRITICAL") -> _Result:
        total, passed = len(mask), int(mask.sum())
        rate = passed / total if total > 0 else 0.0
        status = "PASS" if rate >= self.fail_threshold else ("WARN" if rate >= 0.80 else "FAIL")
        r = _Result(name, desc, total, passed, total - passed, round(rate, 4), status, severity)
        self._results.append(r)
        (logger.info if status == "PASS" else (logger.warning if status == "WARN" else logger.error))(
            "[%s] %s — %.1f%% pass", status, name, rate * 100
        )
        return r

    def _empty(self, name: str, desc: str, sev: str = "CRITICAL") -> _Result:
        return self._run(name, desc, pd.Series([], dtype=bool), sev)

    def check_not_null(self, col: str) -> _Result:
        mask = self.df[col].notna() if col in self.df.columns else pd.Series([], dtype=bool)
        return self._run(f"NOT_NULL_{col.upper()}", f"'{col}' must not be null", mask)

    def check_unique(self, col: str) -> _Result:
        if col not in self.df.columns:
            return self._empty(f"UNIQUE_{col.upper()}", f"'{col}' missing")
        counts = self.df[col].value_counts(dropna=True)
        return self._run(f"UNIQUE_{col.upper()}", f"'{col}' must be unique",
                         self.df[col].apply(lambda v: pd.isna(v) or counts.get(v, 0) == 1))

    def check_values_in_set(self, col: str, allowed: set, severity: str = "CRITICAL") -> _Result:
        if col not in self.df.columns:
            return self._empty(f"IN_SET_{col.upper()}", f"'{col}' missing", severity)
        return self._run(f"IN_SET_{col.upper()}", f"'{col}' in {sorted(allowed)}",
                         self.df[col].isna() | self.df[col].isin(allowed), severity)

    def check_regex(self, col: str, pattern: str, description: str = "") -> _Result:
        if col not in self.df.columns:
            return self._empty(f"REGEX_{col.upper()}", description)
        mask = self.df[col].isna() | self.df[col].astype(str).str.match(re.compile(pattern))
        return self._run(f"REGEX_{col.upper()}", description or f"'{col}' matches /{pattern}/", mask)

    def check_numeric_range(self, col: str, lo: float, hi: float, severity: str = "CRITICAL") -> _Result:
        if col not in self.df.columns:
            return self._empty(f"RANGE_{col.upper()}", f"'{col}' missing", severity)
        num = pd.to_numeric(self.df[col], errors="coerce")
        return self._run(f"RANGE_{col.upper()}", f"'{col}' between {lo:,.0f} and {hi:,.0f}",
                         num.isna() | ((num >= lo) & (num <= hi)), severity)

    def check_date_range(self, col: str, lo: str, hi: str, severity: str = "CRITICAL") -> _Result:
        if col not in self.df.columns:
            return self._empty(f"DATE_{col.upper()}", f"'{col}' missing", severity)
        dates = pd.to_datetime(self.df[col], errors="coerce")
        return self._run(f"DATE_{col.upper()}", f"'{col}' between {lo} and {hi}",
                         dates.isna() | ((dates >= pd.Timestamp(lo)) & (dates <= pd.Timestamp(hi))), severity)

    def check_referential_integrity(self, fk: str, pk: str, severity: str = "CRITICAL") -> _Result:
        name, desc = f"REF_{fk.upper()}_FK", f"Non-null '{fk}' must exist as '{pk}'"
        if fk not in self.df.columns or pk not in self.df.columns:
            return self._empty(name, desc, severity)
        valid = set(self.df[pk].dropna().unique())
        return self._run(name, desc, self.df[fk].isna() | self.df[fk].isin(valid), severity)

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
        failed = report[(report["status"] == "FAIL") & (report["severity"] == "CRITICAL")]
        if len(failed) > GATE_MAX_FAILURES:
            msg = (f"PIPELINE GATE: {len(failed)} critical checks failed "
                   f"(max={GATE_MAX_FAILURES}). Checks: {list(failed['check'])}")
            logger.critical(msg); raise PipelineGateError(msg)
        return report

    def _to_df(self) -> pd.DataFrame:
        cols = ("check", "description", "total", "passed", "failed", "pass_rate", "status", "severity")
        return pd.DataFrame([{c: getattr(r, c) for c in cols} for r in self._results])

    def export(self, report: pd.DataFrame, output_dir: str | Path) -> None:
        out = Path(output_dir); out.mkdir(parents=True, exist_ok=True)
        report.to_csv(out / "validation_report.csv", index=False)
        (out / "validation_report.html").write_text(_build_html(report), encoding="utf-8")
        logger.info("Validation report written to %s", out)


def _build_html(report: pd.DataFrame) -> str:
    BG = {"PASS": "#d4edda", "WARN": "#fff3cd", "FAIL": "#f8d7da"}
    hdrs = ["Check", "Description", "Total", "Passed", "Failed", "Pass Rate", "Status", "Severity"]
    head = "".join(f"<th>{h}</th>" for h in hdrs)
    body = "".join(
        f"<tr style='background:{BG.get(r['status'],'#fff')}'>"
        f"<td>{r['check']}</td><td>{r['description']}</td>"
        f"<td>{int(r['total']):,}</td><td>{int(r['passed']):,}</td><td>{int(r['failed']):,}</td>"
        f"<td>{float(r['pass_rate']):.1%}</td><td><b>{r['status']}</b></td><td>{r['severity']}</td></tr>"
        for _, r in report.iterrows()
    )
    css = ("body{font-family:Arial,sans-serif;margin:2em}table{border-collapse:collapse;width:100%}"
           "th,td{border:1px solid #ccc;padding:8px 12px;text-align:left}th{background:#4a4a8a;color:white}")
    ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    return (f"<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>"
            f"<title>Data Quality Report</title><style>{css}</style></head>"
            f"<body><h1>GlobalTech Corp — HR Data Quality Validation Report</h1>"
            f"<p>Generated: {ts}</p><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
            f"</body></html>")
