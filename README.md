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
2. **Data Cleaning & Transformation:**  
   - Cast numeric columns safely to double (`budget`, `revenue`, `popularity`, `vote_average`, `runtime`).  
   - Parse release dates safely with `try_to_date()` and extract `release_year`.  
   - Count genres per movie and create derived features.
3. **Filtering:** Apply early filters (`release_year >= 2010`, `vote_average > 7`) for performance optimization.
4. **Joins:** Merge `movies`, `credits`, and `keywords` datasets on `id`.
5. **Aggregations:** Group by `release_year` or genre with `avg`, `count` functions.
6. **SQL Queries:**  
   - Top 10 highest-rated movies since 2015.  
   - Average rating by release year.
7. **Performance Optimisation:**  
   - `.explain()` used to check execution plans.  
   - `.cache()` applied for repeated actions.  
   - Column pruning and filter pushdown optimizations applied.
8. **Writing Results:** Save processed results to **Parquet files** for reuse.
9. **Lazy vs Eager Evaluation:** Demonstrate differences between transformations and actions.
10. **MLlib Demo (Optional):** Linear regression predicting movie revenue from budget, popularity, runtime, and vote_average.

---

## Performance Analysis

**Execution Plans (`.explain()` output / Spark UI screenshots)**  

- Spark optimized transformations using **lazy evaluation**, combining filters and joins before execution.  
- Filters (`release_year`, `vote_average`) were pushed down to minimize I/O.  
- Joins were executed efficiently using **broadcast joins** where applicable.  
- Caching the joined dataset reduced computation time for repeated actions.

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
   ![Execution Plan](screenshots/execution_plan.png)  

2. **Successful Pipeline Execution**  
   ![Pipeline Success](screenshots/pipeline_success.png)  

3. **Query Details View Showing Optimizations**  
   ![Query Details](screenshots/query_details.png)  

> *Note: Place the actual screenshots in a `screenshots/` folder in the repo.*

---

## How to Run

1. Start a Databricks cluster with sufficient memory for 1GB+ datasets.  
2. Upload CSV datasets to a volume (`/Volumes/workspace/default/movies_data/`).  
3. Run the notebook `de_pyspark_ssj40.ipynb` sequentially.  
4. Inspect Spark UI and `.explain()` outputs for query optimization.  
5. Processed results are saved as Parquet files in:  
