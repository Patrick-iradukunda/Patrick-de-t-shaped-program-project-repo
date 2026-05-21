# SK-01 Capstone — Multi-Source HR Data Integration Pipeline

## Business Context

GlobalTech Corp has completed the acquisition of AcquiredCo and needs a unified employee dataset delivered in **10 business days** to support:

- Day 1 integration planning (who is joining, in which role, at what cost)
- Benefits enrollment eligibility verification
- Payroll system migration to the combined company's platform
- Compliance reporting for the merger (headcount by jurisdiction, salary band distribution)

| Risk if Data is Wrong       | Business Impact                                         |
| --------------------------- | ------------------------------------------------------- |
| Duplicate employee records  | Overpayment; incorrect headcount reported to regulators |
| Currency not converted      | Salary bands and compensation analysis are invalid      |
| Missing benefits enrollment | Employees miss open enrollment window; legal liability  |
| Wrong jurisdiction codes    | Compliance reports filed incorrectly; potential fines   |

---

## Input Sources

| #   | Source System                 | File                           | Format           | Records | Known Issues                               |
| --- | ----------------------------- | ------------------------------ | ---------------- | ------- | ------------------------------------------ |
| 1   | GlobalTech HRIS (Workday)     | `data/globaltech_hris.csv`     | CSV (UTF-8)      | 15,000  | Department codes vary by business unit     |
| 2   | AcquiredCo HRIS (BambooHR)    | `data/acquiredco_api.json`     | JSON (paginated) | 3,200   | Employee IDs overlap with GlobalTech range |
| 3   | Combined Payroll (ADP)        | `data/payroll_data.xlsx`       | Excel / TSV      | 19,000  | Mixed currencies; some duplicates          |
| 4   | Benefits Provider (MedShield) | `data/benefits_enrollment.xml` | XML              | 12,000  | GlobalTech only; not all enrolled          |

## Pipeline Stages

| Step              | Module                       | Description                                                                          |
| ----------------- | ---------------------------- | ------------------------------------------------------------------------------------ |
| 1 — Ingestion     | `src/pipeline/ingest.py`     | Load all 4 sources; simulate API pagination; dead-letter logging                     |
| 2 — Cleaning      | `src/pipeline/clean.py`      | Name normalisation, ID namespacing, currency conversion, dept taxonomy, date parsing |
| 3 — Enrichment    | `main.py`                    | Merge benefits and salary data into HRIS records                                     |
| 4 — Deduplication | `src/pipeline/dedup.py`      | 3-pass dedup + ghost employee detection                                              |
| 5 — Validation    | `src/pipeline/validate.py`   | 15 quality checks; pipeline gate halts on >2 critical failures                       |
| 6 — EDA           | `notebooks/eda_report.ipynb` | 6-chart analytics report saved at 300 DPI                                            |

## Golden Dataset Schema

Full schema of `output/golden_dataset.parquet` (9,140 records, 30 columns):

## Data Quality Validation Results

All 15 checks pass at 100% on the golden dataset:

| Check                         | Type                  | Result |
| ----------------------------- | --------------------- | ------ |
| NOT_NULL_EMPLOYEE_ID          | Not Null              | PASS   |
| NOT_NULL_FIRST_NAME           | Not Null              | PASS   |
| NOT_NULL_LAST_NAME            | Not Null              | PASS   |
| NOT_NULL_EMAIL                | Not Null              | PASS   |
| NOT_NULL_DEPARTMENT_CANONICAL | Not Null              | PASS   |
| NOT_NULL_COUNTRY              | Not Null              | PASS   |
| UNIQUE_EMAIL                  | Unique                | PASS   |
| UNIQUE_EMPLOYEE_ID            | Unique                | PASS   |
| IN_SET_EMPLOYMENT_TYPE        | Values in Set         | PASS   |
| IN_SET_SALARY_CURRENCY        | Values in Set         | PASS   |
| REGEX_EMAIL                   | Regex                 | PASS   |
| REGEX_EMPLOYEE_ID             | Regex                 | PASS   |
| RANGE_SALARY_USD_ANNUAL       | Numeric Range         | PASS   |
| DATE_HIRE_DATE                | Date Range            | PASS   |
| REF_MANAGER_ID_FK             | Referential Integrity | PASS   |

---

## Employee ID Namespacing

| Company    | Raw ID     | Namespaced ID |
| ---------- | ---------- | ------------- |
| GlobalTech | `1042`     | `GT-001042`   |
| AcquiredCo | `ACQ_1042` | `AC-001042`   |

---

## Exchange Rates (Fixed at Pipeline Creation)

| Currency | Rate to USD |
| -------- | ----------- |
| USD      | 1.00        |
| EUR      | 1.09        |
| GBP      | 1.27        |

---

## How to Run

### 1. Create and activate virtual environment

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the pipeline

```bash
python main.py
```

All outputs are written to `output/`. The pipeline log is at `output/pipeline.log`.

### 4. View EDA report

Open `notebooks/eda_report.ipynb` in Jupyter and run all cells:

```bash
jupyter notebook notebooks/eda_report.ipynb
```

The notebook reads the golden dataset and validation report from `output/` and renders all 6 charts plus the validation summary table.

---
