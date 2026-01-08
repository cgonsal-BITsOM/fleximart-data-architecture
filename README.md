# fleximart-data-architecture
Assignment 2 - AI Data Architecture Design and Implementation
# FlexiMart Data Architecture Project

**Student Name: Charles Gonsalves
**Student ID: bitsom_ba_25071752
**Email:** cgonsal@gmail.com
**Date:** Jan 4, 2026

## Project Overview
This is assignment 2 for "Certification Program in Business Analytics with Gen & Agentic AI" from BITSoM. 

As a Data Engineer at FlexiMart, an e-commerce company, this projct is for a complete data pipeline from raw CSV files to a fully functional analytics system: ETL Pipeline, Database Documentation, Business Queries, NoSQL Analysis, Data Warehouse. This was done with Postgres database and local MongoDB container.





## Repository Structure
├── part1-database-etl/
│   ├── etl_pipeline.py
│   ├── schema_documentation.md
│   ├── business_queries.sql
│   └── data_quality_report.txt
├── part2-nosql/
│   ├── nosql_analysis.md
│   ├── mongodb_operations.js
│   └── products_catalog.json
├── part3-datawarehouse/
│   ├── star_schema_design.md
│   ├── warehouse_schema.sql
│   ├── warehouse_data.sql
│   └── analytics_queries.sql
└── README.md

## Technologies Used

- Python 3.x, pandas, mysql-connector-python
- PostgreSQL 18
- MongoDB 6.0.27
- Docker version 29.1.3 

## Setup Instructions
1. Ensure above are installed in the env. 
2. Verify Installation
python3 --version
docker --version
docker compose version
3. Create Python virtual environment
python3 -m venv venv
source venv/bin/activate   # macOS/Linux
pip install pandas mysql-connector-python
4. Start Docker
docker compose up -d
docker ps
5. Database connectivity
PostgreSQL

Host: localhost
Port: 5432
Credentials: defined in docker-compose.yml

MongoDB

Host: localhost
Port: 27017


### Database Setup

```bash
# Create databases from Postgress CLI
createdb -U postgres fleximart;
createdb -U postgres fleximart_dw;

# Run Part 1 - ETL Pipeline
python part1-database-etl/etl_pipeline.py

# Run Part 1 - Business Queries
psql -U postgres -d fleximart -f part1-database-etl/business_queries.sql

# Run Part 3 - Data Warehouse
psql -U postgres -d fleximart_dw -f part3-datawarehouse/warehouse_schema.sql;
psql -U postgres -d fleximart_dw -f part3-datawarehouse/warehouse_data.sql;
psql -U postgres -d fleximart_dw -f part3-datawarehouse/analytics_queries.sql;


### MongoDB Setup

mongosh < part2-nosql/mongodb_operations.js

## Key Learnings

- NoSQL, Docker , and GITHub 
- Importance of NoSQL for Analytics and AI. 


## Challenges Faced

1. Setting up environment to run locally was challenge due to limited capacity on the laptop and complexity involved in using those technologies. Fortunately, I was able to resolve the issues by using public domain knowledge and AI enabled google search.
2. GITHub - Not used to GITHub. Took guidance from mentor to resolve GITHub issues

