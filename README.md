# Movies Data Pipeline with PySpark on Databricks

This repository contains a **PySpark data processing pipeline** implemented in Databricks, which processes large movie datasets (1 GB+) to demonstrate distributed processing, transformations, joins, aggregations, SQL queries, caching, and optional ML analysis.

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

   <img width="471" height="306" alt="image" src="https://github.com/user-attachments/assets/5ab79f88-1ea2-4beb-9f73-dbbadef12295" />

3. **Data Cleaning & Transformation:**  
   - Cast numeric columns safely to double (`budget`, `revenue`, `popularity`, `vote_average`, `runtime`).  
   - Parse release dates safely with `try_to_date()` and extract `release_year`.  
   - Count genres per movie and create derived features.
   
   <img width="548" height="357" alt="image" src="https://github.com/user-attachments/assets/1913fc89-a1cc-47c9-ad61-d9f0ad32edc0" />
   <img width="523" height="130" alt="image" src="https://github.com/user-attachments/assets/5edbbe17-187e-4f73-af83-1c85249edecd" />

4. **Filtering:** Apply early filters (`release_year >= 2010`, `vote_average > 7`) for performance optimization.

   <img width="581" height="209" alt="image" src="https://github.com/user-attachments/assets/cd5a232b-ecd6-48cc-aa88-2a964131091a" />

5. **Joins:** Merge `movies`, `credits`, and `keywords` datasets on `id`.

   <img width="540" height="206" alt="image" src="https://github.com/user-attachments/assets/2bf7d24f-dbfe-4065-b859-e508a5dd7ec5" />

6. **Aggregations:** Group by `release_year` or genre with `avg`, `count` functions.

   <img width="533" height="305" alt="image" src="https://github.com/user-attachments/assets/e7b00f5c-1049-41dc-a563-d3ab0b1e1428" />

7. **SQL Queries:**  
   - Top 10 highest-rated movies since 2015.  
   - Average rating by release year.
   
   <img width="573" height="364" alt="sql q1" src="https://github.com/user-attachments/assets/60c4fca1-08a2-4df5-9134-2fc432b8b483" />
   <img width="668" height="365" alt="sql q2" src="https://github.com/user-attachments/assets/8e74d919-2da6-4489-892c-48d9f0ed573a" />
   
8. **Performance Optimisation:**  
   - `.explain()` used to check execution plans.   
   - Column pruning and filter pushdown optimizations applied.

   <img width="677" height="361" alt="performance analysis" src="https://github.com/user-attachments/assets/fee6856f-e88d-4c18-9a5c-cbebf1709172" />
     
9. **Writing Results:** Save processed results to **Parquet files** for reuse.

   <img width="670" height="363" alt="output location" src="https://github.com/user-attachments/assets/f3601ac8-8c5a-432b-ad41-525987028262" />
   <img width="453" height="214" alt="query on ouput loc" src="https://github.com/user-attachments/assets/36ac7d56-93bf-4a93-842b-0e6e9ee799bb" />


11. **Lazy vs Eager Evaluation:** Demonstrate differences between transformations and actions.

   <img width="376" height="396" alt="lazy eval" src="https://github.com/user-attachments/assets/59458558-9ef3-43f2-9917-53c06252f498" />
   <img width="838" height="398" alt="lazy_eval 2" src="https://github.com/user-attachments/assets/a57b060d-eae8-4ea1-bf14-f0f4b0a31904" />
   <img width="381" height="334" alt="lazy eval 3" src="https://github.com/user-attachments/assets/5e377a0d-3562-42c1-8220-f141766a8701" />


13. **MLlib Demo (Optional):** Linear regression predicting movie revenue from budget, popularity, runtime, and vote_average.

   <img width="527" height="191" alt="ml model" src="https://github.com/user-attachments/assets/a2225475-0098-497e-aa15-a725aab6b915" />

---

## Performance Analysis

**Execution Plans (`.explain()` output / Spark UI screenshots)**  

- Spark optimized transformations using **lazy evaluation**, combining filters and joins before execution.  
- Filters (`release_year`, `vote_average`) were pushed down to minimize I/O.  
- Joins were executed efficiently using **broadcast joins** where applicable.

---

## Key Findings from Data Analysis

- **Movies released after 2010** with high ratings (`vote_average > 7`) tend to have higher budgets and popularity.  
- **Genre Analysis:** Certain genres, like Adventure and Action, consistently have higher average ratings.  
- **Yearly Trends:** Average movie ratings have slightly increased over the years 2010–2023, indicating improved audience reception.  
- **ML Insights:** A simple linear regression model shows that `budget` and `popularity` are moderately predictive of revenue, while `runtime` and `vote_average` are weaker predictors.  

---

## How to Run

1. Start a Databricks cluster with sufficient memory for 1GB+ datasets.  
2. Upload CSV datasets to a volume (`/Volumes/workspace/default/movies_data/`).  
3. Run the notebook `de_pyspark_ssj40.ipynb` sequentially.  
4. Inspect Spark UI and `.explain()` outputs for query optimization.  
5. Processed results are saved as Parquet files in:  
