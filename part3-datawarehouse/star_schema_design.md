Task 3.1: Star Schema Design Documentation (10 marks) - 1 hour
Create star_schema_design.md with these sections:

# Section 1: Schema Overview 

Describe your star schema in text format (not diagram):


## Star Schema (Text Format)
1. FACT TABLE: fact_sales

Grain: One row per product per order line item
Business Process: Sales transactions

Measures (Numeric Facts)

quantity_sold (INT, NOT NULL): Number of units sold
unit_price (DECIMAL(10,2), NOT NULL): Price per unit at time of sale
discount_amount (DECIMAL(10,2), NOT NULL): Discount applied (monetary)
total_amount (DECIMAL(12,2), NOT NULL): Final amount = quantity_sold * unit_price - discount_amount

Foreign Keys

date_key → dim_date(date_key)
product_key → dim_product(product_key)
customer_key → dim_customer(customer_key)



2. DIMENSION TABLE: dim_date (Conformed)
Purpose: Date dimension for time-based analysis
Type: Conformed dimension used across subject areas
Attributes

date_key (PK, INT, format: YYYYMMDD)
full_date (DATE)
day_of_week (VARCHAR): Monday, Tuesday, etc.
month (TINYINT): 1–12
month_name (VARCHAR): January, February, etc.
quarter (VARCHAR): Q1, Q2, Q3, Q4
year (INT): e.g., 2023, 2024
is_weekend (BOOLEAN)


3. DIMENSION TABLE: dim_product (Type 2 SCD)
Purpose: Product attributes at point-in-time; supports historical changes
Attributes

product_key (PK, surrogate INT)
product_id (business natural key)
product_name
brand
category
subcategory
unit_of_measure (e.g., Each, Pack)
active_flag (BOOLEAN): Is this product active?
effective_start_date: The date when specific  "version" of the product became valid.
effective_end_date: The date when specific  "version" of the product became invalid 
current_flag (BOOLEAN): Is this the most current version?


DIMENSION TABLE: dim_customer (Type 2 SCD)
Purpose: Customer profile and segmenting
Attributes

customer_key (PK, surrogate INT)
customer_id (business natural key)
customer_name
email (optional)
phone (optional)
city, state, country
postal_code
customer_segment (e.g., Retail, Wholesale, VIP)
loyalty_tier (optional)
effective_start_date: The date when specific  "version" of the customer became valid.
effective_end_date: The date when specific  "version" of the customer became invalid
current_flag (BOOLEAN): Is this the most current version?

====


FACT TABLE: fact_sales
Grain: One row per product per order line item
Business Process: Sales transactions

Measures (Numeric Facts):
- quantity_sold: Number of units sold
- unit_price: Price per unit at time of sale
- discount_amount: Discount applied
- total_amount: Final amount (quantity × unit_price - discount)

Foreign Keys:
- date_key → dim_date
- product_key → dim_product
- customer_key → dim_customer

DIMENSION TABLE: dim_date
Purpose: Date dimension for time-based analysis
Type: Conformed dimension
Attributes:
- date_key (PK): Surrogate key (integer, format: YYYYMMDD)
- full_date: Actual date
- day_of_week: Monday, Tuesday, etc.
- month: 1-12
- month_name: January, February, etc.
- quarter: Q1, Q2, Q3, Q4
- year: 2023, 2024, etc.
- is_weekend: Boolean

[Continue for dim_product and dim_customer]

Section 2: Design Decisions (3 marks - 150 words)

Why line‑item granularity: It preserves full transactional detail (quantity, price, discount) and enables accurate aggregation, advanced analytics, and flexible reporting without losing fidelity.


Why surrogate keys: They provide stable, system‑independent identifiers that support SCD2 history, avoid issues with changing or reused business IDs, and improve join performance compared to natural keys.


How the design supports drill‑down/roll‑up: Dimension hierarchies (date, product, customer) combined with an atomic fact table allow seamless navigation from high‑level KPIs to detailed order lines and back, enabling flexible and consistent analysis.

Section 3: Sample Data Flow (3 marks)

Show an example of how one transaction flows from source to data warehouse:

A. Source Transaction:
Order #101, Customer "John Doe", Product "Laptop", Qty: 2, Price: 50000

B. Sales order becomes a record in OLTP database, order table

{
  "order_id": 101,
  "order_date": "2024-01-15",
  "customer_name": "John Doe",
  "product_name": "Laptop",
  "quantity": 2,
  "unit_price": 50000
}

C. Transform OLTP record for ETL process
dim_date: Order date becomes date_key 20240115
dim_product: Product name matches to product_id ("LAP123"), returns product_key =5 
dim_customer Customer “John Doe” matches to customer_id ('C00045'), returns customer_key = 12


D. Entries Data Warehouse:
fact_sales: {
  date_key: 20240115,
  product_key: 5,
  customer_key: 12,
  quantity_sold: 2,
  unit_price: 50000,
  total_amount: 100000
}

dim_date: {date_key: 20240115, full_date: '2024-01-15', month: 1, quarter: 'Q1'...}
dim_product: {product_key: 5, product_name: 'Laptop', category: 'Electronics'...}
dim_customer: {customer_key: 12, customer_name: 'John Doe', city: 'Mumbai'...}


## Section 2: Design Decisions (150 words)
1. Why you chose this granularity (transaction line-item level)
- One line per product per order tracks all details associated with transaction and allows for analytical flexibility for all sort of aggregation (daily, weekly, monthly etc, top products, etc). This approach will avoid any constraints later on for analysis.
2. Why surrogate keys instead of natural keys
- While natural keys can change over time , can be reused and coule be inconistent across different systems, surrogate keys are more reliable and stable. Surrogate key are integers , and therefore provide better query performance for joins. They are also an universal identifier across mutliple systems. 
3. How this design supports drill-down and roll-up operations
- Star schema faciliates natural drill‑down/roll‑up for any dashboard or analytics path. The dimensional design supports drill‑down and roll‑up through built‑in hierarchies: date (year → quarter → month → day), product (category → subcategory → product), and customer (region → segment → customer). Combined with the line‑item fact table, this enables flexible navigation from high‑level KPIs down to the individual order line while maintaining accuracy and consistency across analytics.

## Section 3: Sample Data Flow
Source Transaction:
Order#101, Customer "John Doe", Product "Laptop", Qty: 2, Price: 50000
a) Becomes a transaction in OLTP database

{
  "order_id": 101,
  "order_date": "2024-01-15",
  "customer_name": "John Doe",
  "product_name": "Laptop",
  "quantity": 2,
  "unit_price": 50000
}
b) ETL - Dimenson lookup / surrogate key
dim_date :
2024-01-15 → date_key = 20240115
dim_product: 
Product “Laptop” → matched by natural key (e.g., product_id='LAP123')--> Returns product key=5
dim_customer: 
Customer “John Doe” → matched by natural key (e.g., customer_id='C00045')
Returns customer_key = 12

c. Becomes in Data Warehouse:
fact_sales: {
  date_key: 20240115,
  product_key: 5,
  customer_key: 12,
  quantity_sold: 2,
  unit_price: 50000,
  total_amount: 100000
}

