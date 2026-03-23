# TECHNICAL REPORT — HEALTHTEK PROJECT
## Pharmaceutical Data Pipeline on Microsoft Fabric

---

**Project:** HealthTek Pharma Analytics
**Date:** March 2026
**Technologies:** PySpark · Delta Lake · Microsoft Fabric · Power BI Direct Lake
**Data Sources:** BDPM (ANSM) · OpenMedic AMELI 2024

---

## TABLE OF CONTENTS

1. [Context and Objectives](#1-context-and-objectives)
2. [Overall Architecture](#2-overall-architecture)
3. [Data Sources](#3-data-sources)
4. [Bronze Layer — Raw Ingestion](#4-bronze-layer--raw-ingestion)
5. [Silver Layer — Quality and Enrichment](#5-silver-layer--quality-and-enrichment)
6. [Gold Layer — Analytical KPIs](#6-gold-layer--analytical-kpis)
7. [Power BI Semantic Model](#7-power-bi-semantic-model)
8. [Pipeline Orchestration](#8-pipeline-orchestration)
9. [Catalogue of 20 KPIs](#9-catalogue-of-20-kpis)
10. [Quality Framework](#10-quality-framework)
11. [Summary and Business Value](#11-summary-and-business-value)

---

## 1. Context and Objectives

### 1.1 Context

The French pharmaceutical sector produces a considerable volume of open public data: drug registries, scientific evaluations, prescription and reimbursement data. Although publicly available, these datasets are scattered across multiple agencies (ANSM, HAS, Health Insurance) and require significant consolidation, cleaning, and enrichment before they can power any meaningful decision-making analytics.

The **HealthTek** project addresses this challenge by building an end-to-end data pipeline on **Microsoft Fabric**, from raw file ingestion all the way to an interactive Power BI dashboard.

### 1.2 Objectives

| Objective | Description |
|-----------|-------------|
| **Centralize** | Aggregate all public pharmaceutical data sources into a single Lakehouse |
| **Ensure Quality** | Apply a rigorous 6-pillar quality framework to guarantee data integrity |
| **Add Value** | Compute 20 KPIs across 4 analytical poles for strategic business monitoring |
| **Automate** | Orchestrate updates on 4 schedules (Daily, Weekly, Monthly, Annual) |
| **Visualize** | Expose data through a Direct Lake semantic model connected to Power BI |

---

## 2. Overall Architecture

### 2.1 Medallion Architecture (Bronze / Silver / Gold)

The pipeline follows a **Medallion** three-layer architecture, the modern standard for lakehouses:

```
┌─────────────────────────────────────────────────────────────────┐
│                       EXTERNAL SOURCES                          │
│  BDPM (ANSM)              │        OpenMedic (AMELI)            │
│  8 .txt files             │        1 CSV file (2024)            │
└─────────────┬─────────────┴──────────────┬──────────────────────┘
              │                            │
              ▼                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              BRONZE LAYER  (raw Delta tables)                   │
│  bronze_cis_bdpm  │  bronze_cis_cip  │  bronze_cis_cip_dispo   │
│  bronze_has_smr   │  bronze_has_asmr │  bronze_cis_gener       │
│  bronze_cis_compo │  bronze_cis_cpd  │  bronze_open_medic      │
└──────────────────────────────┬──────────────────────────────────┘
                               │  6 Quality Pillars
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              SILVER LAYER  (cleansed data)                      │
│  silver_cis_bdpm  │  silver_cis_cip  │  silver_dispo_spec      │
│  silver_has_smr   │  silver_has_asmr │  silver_cis_gener       │
│  silver_cis_compo │  silver_cis_cpd  │  silver_open_medic      │
│                   ▼                                             │
│         silver_health_analytics_master  (pivot table)          │
└──────────────────────────────┬──────────────────────────────────┘
                               │  20 KPIs — 4 Poles
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              GOLD LAYER  (computed KPIs)                        │
│                    gold_pharma_kpis                             │
│              (partitioned by ATC1)                              │
└──────────────────────────────┬──────────────────────────────────┘
                               │  SQL semantic views
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              POWER BI SEMANTIC MODEL                            │
│  dim_medicament │ dim_atc │ dim_has │ dim_generique             │
│  dim_statut_disponibilite │ fact_volumes                        │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Technology Stack

| Component | Technology |
|-----------|------------|
| Platform | Microsoft Fabric |
| Processing engine | Apache Spark (PySpark) |
| Storage format | Delta Lake (ACID tables) |
| Language | Python 3 + SQL |
| Visualization | Power BI (Direct Lake mode) |
| BI measures | DAX |
| Orchestration | Fabric Data Pipeline + 4 Triggers |

---

## 3. Data Sources

### 3.1 BDPM — French Public Drug Database (ANSM)

The BDPM is the official registry of authorized drugs in France, maintained by the ANSM. The project uses **8 files** from this database:

| File | Bronze Table | Content | Frequency |
|------|-------------|---------|-----------|
| `CIS_bdpm.txt` | `bronze_cis_bdpm` | Drug specialties (name, manufacturer, status) | Monthly |
| `CIS_CIP_bdpm.txt` | `bronze_cis_cip` | Presentations (CIP13, price, reimbursement rate) | Daily |
| `CIS_CIP_Dispo_Spec.txt` | `bronze_cis_cip_dispo_spec` | Supply shortages and stock tensions | Weekly |
| `CIS_HAS_SMR_bdpm.txt` | `bronze_cis_has_smr` | Medical Benefit (SMR) evaluated by HAS | Monthly |
| `CIS_HAS_ASMR_bdpm.txt` | `bronze_cis_has_asmr` | Improvement of Medical Benefit (ASMR) | Monthly |
| `CIS_GENER_bdpm.txt` | `bronze_cis_gener` | Generic groups (Brand / Generic) | Monthly |
| `CIS_COMPO_bdpm.txt` | `bronze_cis_compo` | Active substance composition + dosages | Monthly |
| `CIS_CPD_bdpm.txt` | `bronze_cis_cpd` | Prescription and dispensing conditions | Monthly |

**Universal key:** The **CIS** code (Drug Specialty Identifier, 8 digits) is the primary key linking all reference datasets.

### 3.2 OpenMedic — AMELI Prescription Data

The **OPEN_MEDIC_2024.CSV** file comes from the French Health Insurance (AMELI). It contains annual prescription and reimbursement data for **2024**, broken down by:

- ATC classification (5 hierarchical levels: ATC1 to ATC5)
- CIP13 presentation code
- Patient age group (numeric code)
- Patient gender
- Region (BEN_REG) and prescriber specialty (PSP_SPE)

**Volume data:**
- `BOITES`: number of packs dispensed
- `REM`: amount reimbursed by Health Insurance (€)
- `BSE`: reimbursement base (€)

---

## 4. Bronze Layer — Raw Ingestion

### 4.1 Principle

The Bronze layer stores data in its **raw state**, without any business transformation. Each source file is ingested as-is into a Delta table with two audit columns:
- `IngestionTimestamp`: ingestion timestamp
- `SourceFile`: source file path

### 4.2 Generic Ingestion Function

The `ingest_to_bronze()` function is reusable for all source files. It handles:
- Explicit schema (no automatic inference)
- File encoding (WINDOWS-1252 by default)
- Separator (tab for BDPM files, semicolon for OpenMedic)
- Write in `overwrite` mode with schema evolution (`overwriteSchema=true`)

### 4.3 Frequency-Based Ingestion

Ingestion is parameterized by the `FREQUENCE` parameter passed by the Fabric pipeline:

| Frequency | Tables ingested |
|-----------|----------------|
| `DAILY` | `bronze_cis_cip` (prices and reimbursements) |
| `WEEKLY` | `bronze_cis_cip_dispo_spec` (ANSM shortages) |
| `MONTHLY` | 6 BDPM tables (specialties, SMR, ASMR, generics, composition, CPD) |
| `ANNUAL` | `bronze_open_medic` (2024 prescription volumes) |
| `ALL` | All tables |

---

## 5. Silver Layer — Quality and Enrichment

### 5.1 The 6-Pillar Quality Framework

The Bronze → Silver transition applies a systematic quality framework through **6 pillars** via the `apply_healthtek_quality()` orchestrator function:

| Pillar | Function | Description |
|--------|----------|-------------|
| **0** | `standardize_columns()` | Column name normalization to PascalCase (CIS, CIP, ATC kept uppercase) |
| **1** | `handle_technical_normalization()` | Special character cleanup: typographic quotes, non-breaking spaces |
| **2** | `apply_default_labels()` | Replace null/empty values with meaningful business defaults |
| **3** | `apply_healthtek_aesthetic()` | Sentence case, HTML tag removal, apostrophe correction |
| **4** | `apply_healthtek_scoring()` | Convert SMR/ASMR labels into ordinal numeric scores |
| **5** | `convert_healthtek_types()` | Strict typing: Integer, Double, Date (multiple formats), percentages |
| **6** | `apply_healthtek_padding()` | ID padding: CIS (8 digits), CIP13 (13 digits), CIP7 (7 digits) |

### 5.2 SMR / ASMR Scoring

Converting HAS assessments into numeric scores enables analytical computations:

**SMR Score (Medical Benefit):**
| Value | Score | Meaning |
|-------|-------|---------|
| Important | 1 | Vital drug |
| Moderate | 2 | Significant medical utility |
| Low | 3 | Limited medical utility |
| Insufficient | 4 | Not reimbursable |

**ASMR Score (Improvement of Medical Benefit):**
| Value | Score | Meaning |
|-------|-------|---------|
| ASMR I | 1 | Major improvement |
| ASMR II | 2 | Important improvement |
| ASMR III | 3 | Moderate improvement |
| ASMR IV | 4 | Minor improvement |
| ASMR V | 5 | No improvement (Me-Too) |

### 5.3 OpenMedic Demographics

The `transform_openmedic_demographics()` function translates numeric codes into analytical labels:

- **Age groups**: 0–19 / 20–59 / 60 and over (with sort order)
- **Gender**: Male (1) / Female (2) / Unknown
- **ATC1 extraction**: derived from the ATC5 code (first character)

### 5.4 Silver Master Table

The **Silver Master** (`silver_health_analytics_master`) is the central pivot table consolidating all dimensions. Its grain is: **(CIP13 × CIS × Age group × Gender)**.

**Joins performed:**
```
fact_om (OpenMedic, grain CIP13 × Age × Gender)
  ├── LEFT JOIN dim_cip       (on CIP13)
  ├── LEFT JOIN fact_cis      (on CIS)
  ├── LEFT JOIN dim_dispo     (on CIS + CIP13)
  ├── LEFT JOIN dim_smr       (on CIS — latest assessment via time window)
  ├── LEFT JOIN dim_asmr      (on CIS — latest assessment via time window)
  ├── LEFT JOIN dim_gener     (on CIS)
  ├── LEFT JOIN dim_compo     (on CIS — substances + dosages concatenated)
  └── LEFT JOIN dim_cpd       (on CIS — prescription conditions concatenated)
```

**Key technical detail — SMR/ASMR time window:** To avoid duplicates caused by historical HAS assessments, only the **most recent opinion** (latest DateAvis) is kept using `Window.partitionBy("CIS").orderBy(F.col("DateAvis").desc())`.

### 5.5 Post-Silver Quality Validation

An automatic validation runs after each Silver table via `validate_silver_table()`:
- Verifies the table is not empty
- Checks the null rate on critical columns:
  - CIS: maximum 1% nulls
  - Other key columns: maximum 5% nulls
- **Pipeline stops** if any threshold is exceeded

---

## 6. Gold Layer — Analytical KPIs

### 6.1 Principle

The Gold layer computes **20 pre-aggregated KPIs** from the Silver Master. The grain of the Gold table remains fine-grained: **(CIS × CIP13 × Age group × Gender)**, with KPIs computed at the CIS level and then joined back to this grain.

### 6.2 Business Flags

Before KPI computation, 11 **boolean flags** (0/1) are created to simplify filtering logic:

| Flag | Condition |
|------|-----------|
| `IsRupture` | CodeStatut contains "rupture" |
| `IsVital` | ValeurSmr_Score = 1 (Important SMR) |
| `IsRuptureCritique` | IsRupture = 1 AND IsVital = 1 |
| `IsGenerique` | TypeGenerique > 0 |
| `IsPrinceps` | TypeGenerique = 0 |
| `IsAsmrProgres` | ValeurAsmr_Score between 1 and 3 |
| `IsMeToo` | ValeurAsmr_Score = 5 (ASMR V) |
| `IsSenior` | Age_Label = "60 ans et +" |
| `IsFemme` | Sexe_Label = "Femme" (Female) |
| `IsHomme` | Sexe_Label = "Homme" (Male) |

### 6.3 Organization by Pole

KPIs are organized into **4 analytical poles** (see Section 9 for the full catalogue).

### 6.4 Partitioning

The Gold table is **partitioned by ATC1** (main therapeutic class, A to Z). This partitioning optimizes Power BI query performance, as most reports filter by therapeutic class.

---

## 7. Power BI Semantic Model

### 7.1 SQL Semantic Views

Notebook `04_NB_Semantic_Views` creates **6 SQL views** exposed to the Direct Lake model:

| View | Key | Role |
|------|-----|------|
| `dim_medicament` | CIS | Full drug description: name, form, manufacturer, market status |
| `dim_atc` | ATC5 | ATC1→ATC5 therapeutic hierarchy, Pareto flag |
| `dim_has` | CIS | HAS assessments: SMR + ASMR with enriched BI labels |
| `dim_generique` | CIS | Generic groups: Brand / Generic / Substitutable |
| `dim_statut_disponibilite` | CodeStatut | Availability reference: available / tension / shortage |
| `fact_volumes` | CIS × CIP13 × Age × Gender | Main fact table with all KPIs |

### 7.2 Star Schema

```
                    dim_medicament
                         │ CIS
                         │
dim_statut_disponibilite ─── fact_volumes ─── dim_has
     CodeStatut          │       │CIS         CIS
                         │       │
                    dim_generique│       dim_atc
                         CIS     │ATC5    ATC5
```

**Relationship rules:**
- Many-to-one cardinality (Fact → Dim)
- Single filter direction (Dim → Fact)
- No bidirectional cross-filter

### 7.3 DAX Measures

The `05_DAX_Measures_PowerBI.dax` file defines **20 DAX measures** organized in 4 folders, plus **6 auxiliary measures** for standard visuals:

- `[Total Reimbursed 2024 (€)]`
- `[Budget Projection 2026 (€)]`
- `[Generic Savings (€)]`
- `[Critical Shortage Rate (%)]`
- `[Average Innovation Score (ASMR)]`
- `[Senior Dependency (%)]`
- etc.

### 7.4 Recommended Power BI Pages

| Page | Key Visuals |
|------|-------------|
| **Executive Overview** | 4 KPI cards, Reimbursement by ATC1 line chart, global slicers |
| **Economic Analysis** | Brand vs Generic table, ATC1 cost bar chart, Price/Volume scatter |
| **Shortage Monitoring** | Manufacturer × Status matrix, shortage timeline, ANSM drillthrough |
| **HAS Innovation** | Me-Too vs Innovation donut, SMR efficiency bar, ASMR vs Volume scatter |
| **Usage & Surveillance** | Age pyramid, Gender ratio, Pareto 80/20 curve |

---

## 8. Pipeline Orchestration

### 8.1 Orchestrator Notebook

Notebook `06_NB_Pipeline_Orchestrator` coordinates the sequential execution of the 4 notebooks via `notebookutils.notebook.run()`:

```
[1/4] Bronze  (timeout: 30 min)
      ↓ (stops on error)
[2/4] Silver + Master  (timeout: 60 min)
      ↓ (stops on error)
[3/4] Gold KPIs  (only if ANNUAL or ALL — timeout: 60 min)
      ↓ (non-blocking on error)
[4/4] Semantic Views  (only if ANNUAL or ALL — timeout: 10 min)
```

**Error handling:**
- Bronze and Silver steps are **blocking**: any error immediately stops the pipeline
- Semantic Views step is **non-blocking**: Gold data remains valid even if view creation fails

### 8.2 Automatic Triggers

4 Fabric triggers are configured based on data source update frequencies:

| Trigger | Schedule | Parameter | Source updated |
|---------|----------|-----------|----------------|
| `trigger_daily_cis_cip` | Every day at 06:00 UTC | `DAILY` | CIP prices and reimbursements |
| `trigger_weekly_dispo` | Every Monday at 05:00 UTC | `WEEKLY` | ANSM shortage data |
| `trigger_monthly_bdpm` | 1st of month at 04:00 UTC | `MONTHLY` | Full BDPM registry |
| `trigger_annual_openmedic` | Manual / Jan 1st at 03:00 UTC | `ANNUAL` | OpenMedic + full Gold recompute |

---

## 9. Catalogue of 20 KPIs

### Pole I — Economic Analysis (KPIs 1–5)

| # | KPI | Formula | Business Use |
|---|-----|---------|-------------|
| 1 | **Actual Expenditure 2024** | `SUM(Rem)` | Actual amount reimbursed by Health Insurance |
| 2 | **Budget Projection 2026** | `SUM(Packs × Price × ReimbRate)` | Forward budget planning |
| 3 | **Generic Savings** | `Σ Brand cost − Σ Generic cost (×Packs×Price)` | Potential substitution savings |
| 4 | **Generic Penetration** | `GenericPacks / TotalPacks` | Generic market share |
| 5 | **Average ATC1 Cost** | `TotalBse / NbDrugs` per class | Average cost per therapeutic family |

### Pole II — Sanitary Monitoring (KPIs 6–10)

| # | KPI | Formula | Business Use |
|---|-----|---------|-------------|
| 6 | **Critical Shortage Rate** | `VitalShortages / TotalVital` | Critical patient risk |
| 7 | **Patients Impacted** | `SUM(Packs)` where IsRupture=1 | Volume of patients exposed to shortages |
| 8 | **Average Shortage Duration** | `AVG(DateRestored − DateStart)` in days | Average duration of supply disruptions |
| 9 | **ATC Tension Index** | `COUNT(CIS)` in shortage per ATC | Pressure per therapeutic class |
| 10 | **Lab Reliability** | `1 − (ShortageCount / TotalCIS)` | Manufacturer reliability score |

### Pole III — HAS Innovation (KPIs 11–15)

| # | KPI | Formula | Business Use |
|---|-----|---------|-------------|
| 11 | **Average Innovation Score** | `Σ(AsmrScore × Packs) / ΣPacks` | Volume-weighted innovation score |
| 12 | **Innovation Access Rate** | `ASMR_I-III Packs / TotalPacks` | Share of innovative drugs by volume |
| 13 | **Innovation Premium** | `Price_ASMR1-3 / Price_ASMR4-5` | Price premium for innovative drugs |
| 14 | **Me-Too Share** | `NbMeToo / TotalCIS` | Share of ASMR V drugs (no improvement) |
| 15 | **Therapeutic Efficiency** | `ΣPacks / ΣSMRScore` | Volumes dispensed per SMR level |

### Pole IV — Usage & Surveillance (KPIs 16–20)

| # | KPI | Formula | Business Use |
|---|-----|---------|-------------|
| 16 | **Senior Dependency** | `Packs_60+ / TotalPacks` | Share of consumption by seniors |
| 17 | **Gender Ratio** | `FemalePacks / MalePacks` | Gender balance in prescriptions |
| 18 | **Pareto 80/20** | Cumulative rank of ATC5 by volume | Identify the 20% molecules = 80% volume |
| 19 | **Average Patient Cost** | `Bse / Packs` | Net cost per pack dispensed (€) |
| 20 | **Market Dynamics** | CIS in BDPM but absent from OpenMedic | Newly authorized drugs not yet sold |

---

## 10. Quality Framework

### 10.1 Identifier Normalization

ID padding ensures join integrity across all tables:
- **CIS**: 8 digits, left-padded with zeros (e.g., `60027` → `00060027`)
- **CIP13**: 13 digits (European barcode)
- **CIP7**: 7 digits (legacy code)
- **ATC5**: Uppercased and trimmed

### 10.2 Deduplication Strategy

The deduplication strategy is tailored to the actual grain of each table:
- `silver_cis_cip`: deduplicated on CIP13
- `silver_open_medic`: deduplicated on **(CIP13, Age, Gender)** — the real grain of the source file
- `silver_dispo_spec`: deduplicated on (CIS, CIP7)
- BDPM tables: deduplicated on CIS

### 10.3 Default Values for Missing Data

| Column | Default Value |
|--------|--------------|
| StatutBdm | "Non Renseigné" (Not Specified) |
| CodeStatut | "Disponible" (Available) |
| Titulaire | "Inconnu" (Unknown) |
| LibelleStatut | "Disponible" (Available) |

---

## 11. Summary and Business Value

### 11.1 What the Project Delivers

HealthTek delivers a **complete pharmaceutical intelligence system** covering 4 key dimensions:

1. **Economic**: Health Insurance budget tracking, identification of generic substitution savings levers, 2026 projections
2. **Sanitary**: Near real-time monitoring of supply shortages, identification of threatened vital drugs, shortage traceability
3. **Innovation**: Objective drug scoring through HAS assessments (SMR/ASMR), identification of me-too drugs vs genuine therapeutic advances
4. **Usage**: Demographic prescription analysis, high-impact molecule detection (Pareto), market dynamics monitoring

### 11.2 Technical Strengths

- **Scalable architecture**: the pipeline handles millions of rows thanks to Spark and Delta Lake
- **Certified quality**: 6 quality pillars + automatic validation prevent corrupted data from being published
- **Differential updates**: the FREQUENCE parameter avoids recomputing the entire pipeline on every update
- **Optimized semantic model**: ATC1 partitioning, Direct Lake, star schema for maximum Power BI performance
- **Maintainability**: reusable functions, structured logging, explicit error handling

### 11.3 Limitations and Roadmap

| Current Limitation | Potential Improvement |
|-------------------|-----------------------|
| OpenMedic data is annual only | Integrate monthly SNIIRAM data for finer granularity |
| No multi-year historical data | Add a Date dimension for longitudinal tracking |
| ATC labels manually reconstructed | Join with the full WHO ATC reference |
| 2026 projection based on linear growth | Integrate an ML predictive model in Fabric |
| No alerting on critical shortages | Automatic Teams/email notification on IsRuptureCritique |

---

*Technical report generated from the analysis of HealthTek notebooks — March 2026*
