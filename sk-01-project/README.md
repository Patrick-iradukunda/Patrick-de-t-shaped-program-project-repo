# SK-01 Capstone — Multi-Source HR Data Integration Pipeline

GlobalTech Corp acquired AcquiredCo and needs a unified employee dataset in **10 business days** for Day 1 planning, benefits enrollment, payroll migration, and compliance reporting. Wrong data risks overpayment, missed enrollment windows, and regulatory fines.

## Input Sources

| # | Source | File | Format | Records | Known Issues |
|---|--------|------|--------|---------|--------------|
| 1 | GlobalTech HRIS (Workday) | `data/globaltech_hris.csv` | CSV UTF-8 | 15,000 | Dept codes vary by BU |
| 2 | AcquiredCo HRIS (BambooHR) | `data/acquiredco_api.json` | JSON paginated | 3,200 | IDs overlap with GlobalTech |
| 3 | Combined Payroll (ADP) | `data/payroll_data.xlsx` | Excel/TSV | 19,000 | Mixed currencies; duplicates |
| 4 | Benefits (MedShield) | `data/benefits_enrollment.xml` | XML | 12,000 | GlobalTech only; partial enrollment |

## Pipeline Stages

| Step | Module | What it does |
|------|--------|--------------|
| 1 — Ingestion | `src/pipeline/ingest.py` | Load all 4 sources; paginate JSON API; dead-letter logging |
| 2 — Cleaning | `src/pipeline/clean.py` | Name normalisation, ID namespacing, currency conversion, dept taxonomy, date parsing |
| 3 — Enrichment | `main.py` | Merge benefits and salary data into HRIS records |
| 4 — Deduplication | `src/pipeline/dedup.py` | 3-pass dedup (exact ID → email → fuzzy name) + ghost employee detection |
| 5 — Validation | `src/pipeline/validate.py` | 15 quality checks; pipeline gate halts on > 2 critical failures |
| 6 — EDA | `notebooks/eda_report.ipynb` | 6-chart analytics report saved at 300 DPI |

## Outputs

| File | Description |
|------|-------------|
| `output/golden_dataset.parquet` | Unified, deduplicated golden dataset (9,140 records, 30 columns) |
| `output/golden_dataset/company_origin=*/` | Partitioned by company for downstream filtering |
| `output/ghost_employees.csv` | Payroll records with no HRIS match (compliance/fraud risk) |
| `output/probable_match_review.csv` | Fuzzy-matched pairs for HR review |
| `output/reports/validation_report.csv` | 15-check quality report (CSV + HTML) |
| `output/eda_report.png` | Combined 6-panel analytics figure (300 DPI) |
| `output/dead_letters.json` | Records skipped during ingestion with reasons |
| `output/pipeline.log` | Full pipeline execution log |

## Data Quality — All 15 Checks Pass

Not-null: `employee_id`, `first_name`, `last_name`, `email`, `department_canonical`, `country` · Unique: `email`, `employee_id` · Values-in-set: `employment_type` {Full-Time, Part-Time, Contractor}, `salary_currency` {USD, EUR, GBP} · Regex: email format, `employee_id` pattern `GT-/AC-XXXXXX` · Numeric range: `salary_usd_annual` $15k–$2M · Date range: `hire_date` 1970–today · Referential integrity: `manager_id` → `employee_id`

## Key Conventions

**Employee ID namespacing** — resolves the ID overlap between companies:

| Company | Raw ID | Namespaced ID |
|---------|--------|---------------|
| GlobalTech | `1042` | `GT-001042` |
| AcquiredCo | `ACQ_1042` | `AC-001042` |

**Fixed exchange rates:** USD 1.00 · EUR 1.09 · GBP 1.27

**Pay frequency multipliers:** Annual ×1 · Monthly ×12 · Bi-weekly ×26 · Weekly ×52 · Semi-monthly ×24

## How to Run

```bash
# 1. Set up environment
python -m venv .venv && .venv\Scripts\activate   # Windows
# source .venv/bin/activate                      # macOS/Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline (all outputs → output/)
python main.py

# 4. Open the EDA notebook
jupyter notebook notebooks/eda_report.ipynb
```

## Known Limitations

- Exchange rates are fixed at pipeline creation time — not live.
- Fuzzy name matching flags probable matches for HR review rather than auto-merging.
- Benefits data covers GlobalTech employees only; AcquiredCo enrollment data is absent.
- Ghost employee detection compares Payroll against HRIS; a Payroll record missing from HRIS is flagged but not deleted.

## Change Log

| Version | Change |
|---------|--------|
| 1.0 | Initial pipeline implementation — all 6 deliverables complete |
