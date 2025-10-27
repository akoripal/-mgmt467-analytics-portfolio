# 🧩 Lab 1 — Data Quality (DQ) Pipeline  
**Course:** MGMT 467 — AI-Assisted Big Data Analytics  
**Author:** Anurag Koripalli  
**Date:** October 2025  

---

## 🎯 Objective  
Build a complete, auditable data-quality pipeline using the **Netflix Kaggle dataset**, moving data from **Kaggle ➜ Google Cloud Storage ➜ BigQuery**, and applying **data quality (DQ) diagnostics** to detect and handle missingness, duplicates, and outliers.

---

## 🧠 Learning Goals  
1. Explain why data is staged in GCS before BigQuery.  
2. Build an **idempotent**, traceable pipeline for reproducible analytics.  
3. Identify and quantify **missing data, duplicates, and outliers**.  
4. Apply cleaning methods (deduplication, winsorization) and justify their business/ML impact.  
5. Document all SQL logic and reflections for transparency.

---

## ⚙️ Environment Setup  
- **Platform:** Google Colab  
- **Cloud Services:** Google Cloud Storage, BigQuery  
- **Libraries:** `google-cloud-bigquery`, `pandas`, `os`  
- **Authentication:** `gcloud auth login`  
- **Project ID:** `mgmt467project`  
- **Region:** `us-central1`  
- **Dataset:** `mgmt467project.netflix`  
- **Bucket:** `gs://mgmt467project-netflix`

---

## 🧩 Pipeline Overview  

| Step | Description | Output |
|------|--------------|--------|
| 1️⃣ | Import Kaggle dataset and stage to Google Cloud Storage | Raw CSV files in GCS |
| 2️⃣ | Load GCS data into BigQuery | `netflix.*` tables |
| 3️⃣ | Verify row counts across all tables | `users`, `movies`, `watch_history`, etc. |
| 4️⃣ | Diagnose missingness and duplication | Percentages by field |
| 5️⃣ | Create deduplicated table `watch_history_dedup` | Removes repeated sessions |
| 6️⃣ | Compute IQR and Winsorize outliers | Robust table `watch_history_robust` |
| 7️⃣ | Flag anomalies (binge sessions, unrealistic ages, invalid durations) | DQ summary flags |
| 8️⃣ | Export all queries to `dq_checks.sql` | Centralized audit log |

---

## 🧮 Key SQL Components  
| Category | Query Example |
|-----------|----------------|
| **Missingness %** | Calculates null percentages for `country`, `subscription_plan`, and `age`. |
| **Duplicates** | Groups by `user_id, movie_id, watch_date, device_type` to find duplicates. |
| **Deduplication** | Uses `ROW_NUMBER()` to keep the highest-progress record. |
| **IQR Outlier Detection** | Calculates Q1, Q3, IQR, and bounds for `watch_duration_minutes`. |
| **Winsorization** | Caps values below 1st percentile and above 99th percentile. |
| **Anomaly Flags** | Identifies binge sessions (> 8 hrs), extreme ages (< 10 / > 100), and invalid movie durations (< 15 min / > 480 min). |

All SQL queries are documented in [`lab4_dq_checks.sql`].

---

## 📊 Verification Results  

| Stage | Table | Row Count | Notes |
|-------|--------|-----------|-------|
| Before Deduplication | `netflix.watch_history` | 210,000 | Raw data |
| After Deduplication | `netflix.watch_history_dedup` | 100,000 | Cleaned |
| After Winsorization | `netflix.watch_history_robust` | 100,000 | Outliers capped |

### Flag Summary  
| Flag | % of Rows | Interpretation |
|------|------------|----------------|
| `flag_binge` | 0.64 % | Possible binge sessions or logging errors |
| `flag_age_extreme` | 1.74 % | Suspiciously low/high user ages |
| `flag_duration_anomaly` | 2.21 % | Movies outside valid duration bounds |

---

## 💬 Reflections  

- **Data Integrity:** Staging in GCS ensures controlled data lineage before analytics.  
- **Deduplication Impact:** Reduced 210 K → 100 K rows, removing redundant sessions without data loss.  
- **Outlier Handling:** Winsorization preserved data scale while capping extreme watch durations, stabilizing model inputs.  
- **Business Relevance:** Detecting binge behavior and extreme user ages aids quality checks for recommender systems.  

---

## 📦 Files in This Folder  
| File | Description |
|------|--------------|
| `MGMT467_PromptPlusExamples_Colab_Kaggle_GCS_BQ_DQ_prof__AK.ipynb` | Complete C_

