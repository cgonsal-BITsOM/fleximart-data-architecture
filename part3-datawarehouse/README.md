## Part 3 overview
FlexiMart requires a centralized data warehouse to analyze historical sales patterns, enable trend analysis, and support decision-ready analytics. This folder contains the foundational assets that implement that requirement using a star schema–based dimensional model optimized for analytical reporting.

star_schema_design.md
Documents the logical star schema design, including fact grain, dimension attributes, and dimensional modeling decisions. Explains why the model is structured this way and how it supports drill‑down, roll‑up, and historical analysis.

warehouse_schema.sql
Contains the DDL scripts to create the FlexiMart data warehouse schema in the target database.
Implements the star schema with fact and dimension tables, primary keys, and foreign key relationships.

warehouse_data.sql
Provides seed data for dimensions and fact tables to enable immediate testing and analysis.
Populates realistic dates, products, customers, and sales transactions aligned to the schema design.

analytics_queries.sql
Contains business‑focused analytical SQL queries built on top of the star schema.
Demonstrates core BI patterns such as time‑based drill‑down, top‑N product analysis, and customer value segmentation.