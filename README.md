# Movies Data Pipeline with PySpark on Databricks

This repository contains a **PySpark data processing pipeline** implemented in Databricks, processing large movie datasets (1GB+) to demonstrate distributed processing, transformations, joins, aggregations, SQL queries, caching, and optional ML analysis.

---

## Dataset Description and Source

The project uses three related datasets:

| Dataset | Description | Source |
|---------|-------------|--------|
| `movies_metadata.csv` | Metadata for movies including title, budget, revenue, release dates, genres, popularity, vote ratings | Kaggle: [TMDb Movie Dataset](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata) |
| `credits.csv` | Cast and crew information for each movie | Kaggle: same source |
| `keywords.csv` | Keywords or tags associated with each movie | Kaggle: same source |

All datasets were uploaded to a Databricks **volume** (`/Volumes/workspace/default/movies_data/`) and processed using PySpark.

---

## Pipeline Overview

The notebook implements the following pipeline steps:

1. **Data Ingestion:** Load CSVs into PySpark DataFrames with safe schema inference.
3. **Data Cleaning & Transformation:**  
   - Cast numeric columns safely to double (`budget`, `revenue`, `popularity`, `vote_average`, `runtime`).  
   - Parse release dates safely with `try_to_date()` and extract `release_year`.  
   - Count genres per movie and create derived features.
4. **Filtering:** Apply early filters (`release_year >= 2010`, `vote_average > 7`) for performance optimization.
   
5. **Joins:** Merge `movies`, `credits`, and `keywords` datasets on `id`.
6. **Aggregations:** Group by `release_year` or genre with `avg`, `count` functions.
7. **SQL Queries:**  
   - Top 10 highest-rated movies since 2015.  
   - Average rating by release year.
8. **Performance Optimisation:**  
   - `.explain()` used to check execution plans.  
   - `.cache()` applied for repeated actions.  
   - Column pruning and filter pushdown optimizations applied.
9. **Writing Results:** Save processed results to **Parquet files** for reuse.
10. **Lazy vs Eager Evaluation:** Demonstrate differences between transformations and actions.
11. **MLlib Demo (Optional):** Linear regression predicting movie revenue from budget, popularity, runtime, and vote_average.

---

## Performance Analysis

**Execution Plans (`.explain()` output / Spark UI screenshots)**  

- Spark optimized transformations using **lazy evaluation**, combining filters and joins before execution.  
- Filters (`release_year`, `vote_average`) were pushed down to minimize I/O.  
- Joins were executed efficiently using **broadcast joins** where applicable.
<img width="677" height="361" alt="performance analysis" src="https://github.com/user-attachments/assets/fee6856f-e88d-4c18-9a5c-cbebf1709172" />


**Screenshots included:**
1. Query execution plan using `.explain(extended=True)`  
2. Spark UI Query Details showing stages, shuffles, and optimized execution  
3. Successful pipeline execution confirming all transformations, filters, and Parquet writes  

---

## Key Findings from Data Analysis

- **Movies released after 2010** with high ratings (`vote_average > 7`) tend to have higher budgets and popularity.  
- **Genre Analysis:** Certain genres, like Adventure and Action, consistently have higher average ratings.  
- **Yearly Trends:** Average movie ratings have slightly increased over the years 2010–2023, indicating improved audience reception.  
- **ML Insights:** A simple linear regression model shows that `budget` and `popularity` are moderately predictive of revenue, while `runtime` and `vote_average` are weaker predictors.  

---

## Screenshots

1. **Query Execution Plan**  
 <img width="376" height="396" alt="lazy eval" src="https://github.com/user-attachments/assets/80966068-43d2-4f10-9cf1-c914b67f42be" />

2. **Successful Pipeline Execution**  
  <img width="838" height="398" alt="lazy_eval 2" src="https://github.com/user-attachments/assets/a5e2c338-1a39-4921-84c0-38e872b33b4c" />
  <img width="381" height="334" alt="lazy eval 3" src="https://github.com/user-attachments/assets/082c6c70-f415-417f-884e-a91504991066" />

3. **Query Details View Showing Optimizations**  
<img width="573" height="364" alt="sql q1" src="https://github.com/user-attachments/assets/60c4fca1-08a2-4df5-9134-2fc432b8b483" />
<img width="668" height="365" alt="sql q2" src="https://github.com/user-attachments/assets/8e74d919-2da6-4489-892c-48d9f0ed573a" />
<img width="674" height="349" alt="action" src="https://github.com/user-attachments/assets/632bd02c-a78b-40c4-b444-f0c50bfddd68" />

4. **ML Model**  
<img width="527" height="191" alt="ml model" src="https://github.com/user-attachments/assets/512d695d-949a-42c3-8f0a-f7d1b3cd5e43" />

---

## How to Run

1. Start a Databricks cluster with sufficient memory for 1GB+ datasets.  
2. Upload CSV datasets to a volume (`/Volumes/workspace/default/movies_data/`).  
3. Run the notebook `de_pyspark_ssj40.ipynb` sequentially.  
4. Inspect Spark UI and `.explain()` outputs for query optimization.  
5. Processed results are saved as Parquet files in:  
