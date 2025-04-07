# Data Engineering Pipeline for Premier League Data (2019–2023)

## Overview

This pipeline collects and processes Premier League data (2019–2023 season) by scraping data from the official Premier League website using selenium, storing it in an S3 bucket, and processing it in a PostgreSQL database. The final processed data is visualized in Tableau. The pipeline is orchestrated using Apache Airflow.

---

## Components

- **Data Lake**: S3 Bucket
- **Data Warehouse**: PostgreSQL (Docker)
- **Orchestration**: Apache Airflow
- **Visualization**: Tableau
- **Infrastructure**: Terraform

---

## 1. Data Collection

### Scraping Data
- **Source**: Official Premier League website (2019–2023 season).
- **Data**: Match scores, player stats, team details, etc.
- **Technology**: Python (Selenium).

### Storage in S3 (Data Lake)
- **File Format**: CSV.
- **Organization**: Data stored by season (e.g., `2019/`, `2020/`).

---

## 2. Data Processing

### Loading Data into PostgreSQL (Data Warehouse)
- **Database**: PostgreSQL in a Docker container.
- **Schema**: Tables for match data.
- **Data Operations**: Data cleaning, validation, aggregation.

### Data Cleaning
- Validate and format data.

---

## 3. Orchestration with Apache Airflow

### Airflow Setup
- **Task Scheduling**: Scraping, data loading, and ingestion.
- **DAGs**: Directed Acyclic Graphs define task flow.
- **Error Handling**: Retry logic.

---

## 4. Data Visualization

### Integration with Tableau
- **Connection**: Tableau connects to PostgreSQL for visualization.
- **Dashboards**: Interactive views of match results season comparisons.

### Dashboard Link
- You can view the Premier League dashboard here:  
  [Premier League Dashboard](https://public.tableau.com/app/profile/ezenwa.victory.chibuikem/viz/Premier_League_Dashboard_increased_width/Dashboard1)

---

## 5. Pipeline Flow

1. **Scraping**: Airflow runs scraper to fetch data and store it in S3.
2. **Data Ingestion**: Data is loaded into PostgreSQL for cleaning and transformation.
3. **Data Visualization**: Tableau connects to PostgreSQL and generates visual reports.
