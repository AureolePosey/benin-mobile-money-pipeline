# 🇧🇯 Benin Mobile Money Data Pipeline (PySpark)

##  Project Overview

This project simulates and processes Mobile Money transaction data in Benin using PySpark.  
It demonstrates a complete end-to-end Data Engineering pipeline including:

- Data Generation
- Data Ingestion
- Data Validation (Data Quality checks)
- Data Transformation & Aggregation
- Writing optimized Parquet datasets

The goal is to showcase practical Data Engineering skills using Spark.

---

##  Architecture

The pipeline is structured as follows:

1. **Data Generation**
   - Synthetic users dataset
   - Synthetic transactions dataset

2. **Ingestion**
   - Reading CSV files using Spark
   - Schema inference

3. **Validation**
   - Null checks
   - Business rule validation (negative amounts, future transactions)
   - Referential integrity (orphan transactions)

4. **Transformation**
   - Transaction volume per region
   - Top 10 users by transaction volume
   - Monthly & yearly aggregations

5. **Storage**
   - Results saved in Parquet format for optimized analytics

---

##  Project Structure
benin-mobile-money-pipeline/
│
├── data/
│ ├── raw/
│ └── processed/
│
├── src/
│ ├── generate_data.py
│ ├── ingest.py
│ ├── validate.py
│ └── transform.py
│
├── requirements.txt
└── README.md



---

##  How to Run

### 1️- Create virtual environment

```bash
python -m venv venv
source venv/bin/activate


2️- Install dependencies

pip install -r requirements.txt


3️- Run the pipeline


python src/generate_data.py
python src/ingest.py
python src/validate.py
python src/transform.py


 Example Outputs

Transaction volume by region

Top 10 users by total transaction volume

Monthly transaction trends

All processed datasets are saved in:

data/processed/

 Technologies Used

-Python

-PySpark

-Parquet

-WSL (Windows Subsystem for Linux)



 Key Learnings

Building a structured Data Engineering pipeline

Performing Data Quality validation with Spark

Business-driven aggregation logic

Writing optimized analytical datasets



 Author

Rogelio Edjekpoto
Aspiring Data Engineer | Python | PySpark | SQL



##  Continuous Integration

This project includes a GitHub Actions workflow.

On every push to the `main` branch:

- Dependencies are installed
- The pipeline is executed automatically
- The build fails if any error occurs

This ensures reliability and reproducibility.