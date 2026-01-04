import os
import sys
import uuid
import time
import logging
import re
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from logging.handlers import RotatingFileHandler
from datetime import datetime
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch


# ==========================================================
# Config, Logging, and Report Model
# ==========================================================
@dataclass
class Config:
    customers_file: str = os.getenv("CUSTOMERS_FILE", "customers_raw.csv")
    products_file: str = os.getenv("PRODUCTS_FILE", "products_raw.csv")
    sales_file: str = os.getenv("SALES_FILE", "sales_raw.csv")
    report_file: str = os.getenv("REPORT_FILE", "data_quality_report.txt")
    log_file: str = os.getenv("LOG_FILE", "etl_pipeline.log")
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_name: str = os.getenv("DB_NAME", "fleximart")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_pass: str = os.getenv("DB_PASS", "your_password")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    country_code: str = "+91-"  # phone normalization


def setup_logger(cfg: Config, run_id: str):
    logger = logging.getLogger("fleximart_etl")
    logger.setLevel(getattr(logging, cfg.log_level, logging.INFO))
    logger.propagate = False
    logger.handlers = []
    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s run=%(run_id)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(getattr(logging, cfg.log_level, logging.INFO))
    ch.setFormatter(fmt)
    ch.addFilter(lambda record: setattr(record, "run_id", run_id) or True)
    logger.addHandler(ch)
    fh = RotatingFileHandler(cfg.log_file, maxBytes=5 * 1024 * 1024, backupCount=5)
    fh.setLevel(getattr(logging, cfg.log_level, logging.INFO))
    fh.setFormatter(fmt)
    fh.addFilter(lambda record: setattr(record, "run_id", run_id) or True)
    logger.addHandler(fh)
    return logger


# Detailed report per file
class Report:
    def __init__(self):
        self.data = {
            "customers_raw.csv": {
                "processed": 0,
                "duplicates_removed": 0,
                "missing_handled": 0,
                "loaded": 0,
                "table": "customers",
            },
            "products_raw.csv": {
                "processed": 0,
                "duplicates_removed": 0,
                "missing_handled": 0,
                "loaded": 0,
                "table": "products",
            },
            "sales_raw.csv": {
                "processed": 0,
                "duplicates_removed": 0,
                "missing_handled": 0,
                "loaded": 0,
                "tables": ["orders", "order_items"],
                "loaded_breakdown": {"orders": 0, "order_items": 0},
            },
        }

    def write(self, cfg: Config, run_id: str):
        with open(cfg.report_file, "w", encoding="utf-8") as f:
            f.write("data_quality_report.txt - ETL Run Summary\n")
            f.write(f"Run ID: {run_id}\n")

            c = self.data["customers_raw.csv"]
            f.write("[customers_raw.csv] -> customers\n")
            f.write(f" Records processed: {c['processed']}\n")
            f.write(f" Duplicates removed: {c['duplicates_removed']}\n")
            f.write(f" Missing values handled: {c['missing_handled']}\n")
            f.write(f" Records loaded successfully (customers): {c['loaded']}\n")

            p = self.data["products_raw.csv"]
            f.write("[products_raw.csv] -> products\n")
            f.write(f" Records processed: {p['processed']}\n")
            f.write(f" Duplicates removed: {p['duplicates_removed']}\n")
            f.write(f" Missing values handled: {p['missing_handled']}\n")
            f.write(f" Records loaded successfully (products): {p['loaded']}\n")

            s = self.data["sales_raw.csv"]
            f.write("[sales_raw.csv] -> orders, order_items\n")
            f.write(f" Records processed: {s['processed']}\n")
            f.write(f" Duplicates removed: {s['duplicates_removed']}\n")
            f.write(f" Missing values handled: {s['missing_handled']}\n")
            f.write(
                f" Records loaded successfully (orders): {s['loaded_breakdown']['orders']}\n"
            )
            f.write(
                f" Records loaded successfully (order_items): {s['loaded_breakdown']['order_items']}\n"
            )
            f.write(f" Total records loaded from sales: {s['loaded']}\n")


# ==========================================================
# BLOCK 2 — DB, Schema Bootstrap, and Utility Functions
# Purpose: Initialize DB schema and define cleaning utilities.
# Outcome: Ensures tables exist and inputs are standardized.
# ==========================================================
DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS public.customers (
        customer_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        phone VARCHAR(20),
        city VARCHAR(50),
        registration_date DATE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.products (
        product_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        product_name VARCHAR(100) NOT NULL,
        category VARCHAR(50) NOT NULL,
        price NUMERIC(10,2) NOT NULL,
        stock_quantity INT DEFAULT 0
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.orders (
        order_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        customer_id BIGINT NOT NULL,
        order_date DATE NOT NULL,
        total_amount NUMERIC(10,2) NOT NULL,
        status VARCHAR(20) DEFAULT 'Pending',
        CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
            REFERENCES public.customers(customer_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS public.order_items (
        order_item_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        order_id BIGINT NOT NULL,
        product_id BIGINT NOT NULL,
        quantity INT NOT NULL,
        unit_price NUMERIC(10,2) NOT NULL,
        subtotal NUMERIC(10,2) NOT NULL,
        CONSTRAINT fk_items_order FOREIGN KEY (order_id)
            REFERENCES public.orders(order_id),
        CONSTRAINT fk_items_product FOREIGN KEY (product_id)
            REFERENCES public.products(product_id)
    );
    """,
]
# Opens a PostgreSQL connection using Config values.
# Logs a connect event; returns a live psycopg2 connection.


def get_connection(cfg: Config, logger):
    conn = psycopg2.connect(
        host=cfg.db_host,
        database=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_pass,
        port=cfg.db_port,
    )
    logger.info("Connected to PostgreSQL", extra={"stage": "connect"})
    return conn
# Sets search_path and executes DDL to ensure required tables exist.
# Commits changes; safe to call multiple times.


def ensure_schema(conn, logger):
    cur = conn.cursor()
    cur.execute("SET search_path TO public;")
    for ddl in DDL_STATEMENTS:
        cur.execute(ddl)
    conn.commit()
    cur.close()
    logger.info("Schema verified/created", extra={"stage": "bootstrap"})


# ---- Cleaning utilities ----
# Strips non-digits and normalizes to last 10 digits with country code.
# Returns None if input lacks 10 digits.


def clean_phone(num: object, country_code: str) -> Optional[str]:
    if pd.isna(num):
        return None
    digits = "".join(filter(str.isdigit, str(num)))
    return country_code + digits[-10:] if len(digits) >= 10 else None
# Extracts numeric digits from mixed input and converts to int.
# Returns NaN when no digits present.


def clean_id(val: object) -> float:
    if pd.isna(val):
        return np.nan
    digits = "".join(filter(str.isdigit, str(val)))
    return int(digits) if digits else np.nan
# Normalizes category text to 'Titlecase' with whitespace trimmed.
# Defaults to 'Unknown' for missing values.


def clean_category(cat: object) -> str:
    if pd.isna(cat):
        return "Unknown"
    return str(cat).strip().lower().capitalize()
# Heuristically infers day-first vs month-first from a date string.
# Returns True/False or None when ambiguous.


def _infer_dayfirst_from_string(s: str) -> Optional[bool]:
    s = str(s).strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d{4})\D+(\d{1,2})\D+(\d{1,2})\s*$", s)
    if m:
        return None
    tokens = re.split(r"\D+", s)
    if len(tokens) >= 2:
        try:
            a = int(tokens[0])
            b = int(tokens[1])
            if a > 12 and b <= 12:
                return True
            if b > 12 and a <= 12:
                return False
        except Exception:
            return None
    return None
# Parses varied date formats into ISO 'YYYY-MM-DD' strings.
# Returns None for invalid/empty dates.


def clean_date(d: object) -> Optional[str]:
    if pd.isna(d):
        return None
    s = str(d).strip()
    if not s:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    explicit_formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%m.%d.%Y",
    ]
    for fmt in explicit_formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    hint = _infer_dayfirst_from_string(s)
    try:
        dt = pd.to_datetime(
            s,
            dayfirst=True if hint is True else (False if hint is False else None),
            errors="coerce",
            infer_datetime_format=True,
        )
    except Exception:
        dt = pd.NaT
    if pd.isna(dt):
        return None
    return pd.Timestamp(dt).strftime("%Y-%m-%d")
# Vectorized wrapper to clean a pandas Series of dates.
# Applies clean_date to each element.


def clean_date_series(series: pd.Series) -> pd.Series:
    return series.apply(clean_date)
# Lowercases and trims email, removing embedded whitespace.
# Returns None for empty inputs.


def canonicalize_email(email: object) -> Optional[str]:
    if pd.isna(email):
        return None
    s = str(email).strip().lower()
    s = re.sub(r"\s+", "", s)
    return s if s else None
# Creates stable placeholder emails when none exist.
# Uses customer_id or random UUID suffix for uniqueness.


def synthesize_email(row: pd.Series) -> str:
    cid = row.get("customer_id")
    if pd.notna(cid):
        suffix = str(int(cid))
    else:
        suffix = uuid.uuid4().hex[:8]
    return f"unknown+{suffix}@example.com"
# Validates presence of required columns in a DataFrame.
# Logs and raises ValueError if any are missing.


def assert_required_columns(
    df: pd.DataFrame, required: List[str], df_name: str, logger
):
    missing = [c for c in required if c not in df.columns]
    if missing:
        msg = f"{df_name}: missing required columns: {missing}"
        logger.error(msg, extra={"stage": "extract"})
        raise ValueError(msg)
# Builds an email→customer_id map from the customers table.
# Emails are canonicalized to match transform logic.


def fetch_customer_id_by_email(conn) -> Dict[str, int]:
    cur = conn.cursor()
    cur.execute("SELECT email, customer_id FROM customers")
    rows = cur.fetchall()
    cur.close()
    return {canonicalize_email(email): cid for (email, cid) in rows}


# ==========================================================
# BLOCK 3 — ETL Class (Extract & Transform)
# Purpose: Extract CSVs and transform them into clean, modeled DataFrames.
# Outcome: Validated inputs, de-duplicated records, normalized fields.
# ==========================================================
class FlexiMartETL:
    def __init__(self, cfg: Config, logger):
        self.cfg = cfg
        self.logger = logger
        self.report = Report()
        # working dataframes
        self.customers = pd.DataFrame()
        self.products = pd.DataFrame()
        self.sales = pd.DataFrame()
        # outputs
        self.orders = pd.DataFrame()
        self.order_items = pd.DataFrame()

    # -------- Extract --------
    def extract(self):
        self.logger.info("Extract: loading CSVs", extra={"stage": "extract"})
        self.customers = pd.read_csv(self.cfg.customers_file)
        self.products = pd.read_csv(self.cfg.products_file)
        self.sales = pd.read_csv(self.cfg.sales_file)
        self.report.data["customers_raw.csv"]["processed"] = len(self.customers)
        self.report.data["products_raw.csv"]["processed"] = len(self.products)
        self.report.data["sales_raw.csv"]["processed"] = len(self.sales)
        assert_required_columns(
            self.customers,
            [
                "first_name",
                "last_name",
                "email",
                "phone",
                "city",
                "registration_date",
                "customer_id",
            ],
            "customers_raw.csv",
            self.logger,
        )
        assert_required_columns(
            self.products,
            ["product_name", "category", "price", "stock_quantity"],
            "products_raw.csv",
            self.logger,
        )
        assert_required_columns(
            self.sales,
            [
                "transaction_id",
                "transaction_date",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price",
            ],
            "sales_raw.csv",
            self.logger,
        )

    # -------- Transform Customers --------
    def transform_customers(self):
        self.logger.info("Transform: customers", extra={"stage": "transform"})
        self.customers["customer_id"] = self.customers["customer_id"].apply(clean_id)
        self.customers["email"] = self.customers["email"].apply(canonicalize_email)
        missing_email_before = int(self.customers["email"].isna().sum())
        self.customers["email"] = self.customers.apply(
            lambda r: r["email"] if r["email"] is not None else synthesize_email(r),
            axis=1,
        )
        self.report.data["customers_raw.csv"]["missing_handled"] += missing_email_before
        self.customers["phone"] = self.customers["phone"].apply(
            lambda x: clean_phone(x, self.cfg.country_code)
        )
        self.customers["registration_date"] = clean_date_series(
            self.customers["registration_date"]
        )
        dup_before = len(self.customers)
        self.customers.drop_duplicates(subset=["email"], keep="first", inplace=True)
        self.report.data["customers_raw.csv"]["duplicates_removed"] += int(
            dup_before - len(self.customers)
        )

    # -------- Transform Products --------
    def transform_products(self):
        self.logger.info("Transform: products", extra={"stage": "transform"})
        self.products["price"] = (
            pd.to_numeric(self.products["price"], errors="coerce")
            .fillna(0.0)
            .astype(float)
        )
        self.products["stock_quantity"] = (
            pd.to_numeric(self.products["stock_quantity"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        self.products["category"] = self.products["category"].apply(clean_category)
        dup_before = len(self.products)
        self.products.drop_duplicates(inplace=True)
        self.report.data["products_raw.csv"]["duplicates_removed"] += int(
            dup_before - len(self.products)
        )
        if "product_id" in self.products.columns:
            self.products["product_id"] = self.products["product_id"].apply(clean_id)

    # -------- Transform Sales (early) --------
    def transform_sales_initial(self):
        self.logger.info("Transform: sales (initial)", extra={"stage": "transform"})
        self.sales["customer_id"] = self.sales["customer_id"].apply(clean_id)
        self.sales["product_id"] = self.sales["product_id"].apply(clean_id)
        self.sales["transaction_id"] = self.sales["transaction_id"].apply(clean_id)
        before = len(self.sales)
        self.sales.dropna(subset=["customer_id", "product_id"], inplace=True)
        self.report.data["sales_raw.csv"]["missing_handled"] += int(
            before - len(self.sales)
        )
        dup_before = len(self.sales)
        self.sales.drop_duplicates(inplace=True)
        self.report.data["sales_raw.csv"]["duplicates_removed"] += int(
            dup_before - len(self.sales)
        )
        self.sales["transaction_date"] = clean_date_series(
            self.sales["transaction_date"]
        )
        self.sales["quantity"] = (
            pd.to_numeric(self.sales["quantity"], errors="coerce").fillna(0).astype(int)
        )
        self.sales["unit_price"] = (
            pd.to_numeric(self.sales["unit_price"], errors="coerce")
            .fillna(0.0)
            .astype(float)
        )
        self.sales["subtotal"] = self.sales["quantity"].astype(float) * self.sales[
            "unit_price"
        ].astype(float)
        # attach email from customers for later DB remap
        customer_keys = self.customers[["customer_id", "email"]].copy()
        self.sales = self.sales.merge(customer_keys, on="customer_id", how="left")
        self.sales["email"] = self.sales["email"].apply(canonicalize_email)
        # Build orders/items AFTER remap (done in load)


# ==========================================================
# BLOCK 4 — Loaders (DB operations with FK-safe logic)
# Purpose: Load data into PostgreSQL with FK-safe ordering and batching.
# Outcome: Upserts customers/products and inserts orders/items reliably.
# ==========================================================
class Loader:
    def __init__(self, cfg: Config, logger, report: Report):
        self.cfg = cfg
        self.logger = logger
        self.report = report
        self.conn = None
        self.cur = None

    def open(self):
        self.conn = get_connection(self.cfg, self.logger)
        ensure_schema(self.conn, self.logger)
        self.cur = self.conn.cursor()

    def close(self):
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.commit()
                self.conn.close()
        except Exception:
            self.logger.exception("Error closing DB resources", extra={"stage": "load"})

    def upsert_customers(self, df: pd.DataFrame) -> None:
        self.logger.info("Load: customers", extra={"stage": "load"})
        with_id = df[pd.notna(df["customer_id"])]
        without_id = df[pd.isna(df["customer_id"])]
        sql_with = """
            INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, registration_date)
            OVERRIDING SYSTEM VALUE
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            """
        sql_without = """
            INSERT INTO customers (first_name, last_name, email, phone, city, registration_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            """
        rows_with = [
            (
                int(row.customer_id),
                row.first_name,
                row.last_name,
                row.email,
                row.phone,
                row.city,
                row.registration_date,
            )
            for _, row in with_id.iterrows()
        ]
        rows_without = [
            (
                row.first_name,
                row.last_name,
                row.email,
                row.phone,
                row.city,
                row.registration_date,
            )
            for _, row in without_id.iterrows()
        ]
        if rows_with:
            execute_batch(self.cur, sql_with, rows_with, page_size=self.cfg.batch_size)
        if rows_without:
            execute_batch(
                self.cur, sql_without, rows_without, page_size=self.cfg.batch_size
            )
        self.conn.commit()
        self.report.data["customers_raw.csv"]["loaded"] = int(
            len(rows_with) + len(rows_without)
        )

    def remap_sales_customer_ids(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        # build/refresh mapping
        email_to_db_cid = fetch_customer_id_by_email(self.conn)
        # synthesize stub for missing emails
        missing_mask = sales_df["email"].isna()
        if missing_mask.any():
            stub_emails = []
            for idx in sales_df.index[missing_mask]:
                new_email = f"unknown+{uuid.uuid4().hex[:8]}@example.com"
                sales_df.at[idx, "email"] = new_email
                stub_emails.append(new_email)
            for em in set(stub_emails):
                try:
                    self.cur.execute(
                        """
                        INSERT INTO customers (first_name, last_name, email, phone, city, registration_date)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (email) DO NOTHING
                        """,
                        ("Guest", "Customer", em, None, None, None),
                    )
                except Exception:
                    self.logger.exception(
                        "Failed to insert stub customer", extra={"stage": "load"}
                    )
            self.conn.commit()
            email_to_db_cid = fetch_customer_id_by_email(self.conn)
        sales_df["customer_id"] = sales_df["email"].map(email_to_db_cid)
        rows_dropped = int(sales_df["customer_id"].isna().sum())
        if rows_dropped:
            self.logger.error(
                f"Sales rows dropped after customer remap: {rows_dropped}",
                extra={"stage": "load"},
            )
        sales_df = sales_df.dropna(subset=["customer_id"])  # enforce FK integrity
        self.report.data["sales_raw.csv"]["missing_handled"] += rows_dropped
        return sales_df

    def load_products(self, df: pd.DataFrame) -> None:
        self.logger.info("Load: products", extra={"stage": "load"})
        # Correct handling of optional product_id column to avoid KeyError
        has_pid = "product_id" in df.columns
        if has_pid:
            with_id = df[pd.notna(df["product_id"])]
            without_id = df[pd.isna(df["product_id"])]
        else:
            with_id = df.iloc[0:0]  # empty frame when id column absent
            without_id = df
        sql_with = """
            INSERT INTO products (product_id, product_name, category, price, stock_quantity)
            OVERRIDING SYSTEM VALUE
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """
        sql_without = """
            INSERT INTO products (product_name, category, price, stock_quantity)
            VALUES (%s, %s, %s, %s)
            """
        rows_with = [
            (
                int(row.product_id) if not pd.isna(row.product_id) else None,
                row.product_name,
                row.category,
                float(row.price),
                int(row.stock_quantity),
            )
            for _, row in with_id.iterrows()
        ]
        rows_without = [
            (
                row.product_name,
                row.category,
                float(row.price),
                int(row.stock_quantity),
            )
            for _, row in without_id.iterrows()
        ]
        if rows_with:
            execute_batch(self.cur, sql_with, rows_with, page_size=self.cfg.batch_size)
        if rows_without:
            execute_batch(
                self.cur, sql_without, rows_without, page_size=self.cfg.batch_size
            )
        self.conn.commit()
        self.report.data["products_raw.csv"]["loaded"] = int(
            len(rows_with) + len(rows_without)
        )

    def load_orders_and_items(self, sales_df: pd.DataFrame) -> Tuple[int, int]:
        self.logger.info("Load: orders & order_items", extra={"stage": "load"})
        # Build orders/items from remapped sales
        orders = (
            sales_df.groupby("transaction_id")
            .agg(
                {"customer_id": "first", "transaction_date": "first", "subtotal": "sum"}
            )
            .reset_index()
        )
        orders.rename(
            columns={
                "transaction_id": "order_id",
                "transaction_date": "order_date",
                "subtotal": "total_amount",
            },
            inplace=True,
        )
        order_items = sales_df.rename(columns={"transaction_id": "order_id"})[
            ["order_id", "product_id", "quantity", "unit_price", "subtotal"]
        ]
        # Insert orders (explicit order_id)
        sql_orders = """
            INSERT INTO orders (order_id, customer_id, order_date, total_amount, status)
            OVERRIDING SYSTEM VALUE
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """
        order_rows = [
            (
                int(r.order_id),
                int(r.customer_id),
                r.order_date,
                float(r.total_amount),
                "Completed",
            )
            for _, r in orders.iterrows()
        ]
        if order_rows:
            execute_batch(
                self.cur, sql_orders, order_rows, page_size=self.cfg.batch_size
            )
        self.conn.commit()
        loaded_orders = len(order_rows)
        # Ensure order_ids exist
        self.cur.execute("SELECT order_id FROM orders")
        existing_orders = {x[0] for x in self.cur.fetchall()}
        order_items = order_items[order_items["order_id"].isin(existing_orders)]
        sql_items = """
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal)
            VALUES (%s, %s, %s, %s, %s)
            """
        item_rows = [
            (
                int(r.order_id),
                int(r.product_id),
                int(r.quantity),
                float(r.unit_price),
                float(r.subtotal),
            )
            for _, r in order_items.iterrows()
        ]
        if item_rows:
            execute_batch(self.cur, sql_items, item_rows, page_size=self.cfg.batch_size)
        self.conn.commit()
        loaded_items = len(item_rows)
        self.report.data["sales_raw.csv"]["loaded_breakdown"]["orders"] = int(
            loaded_orders
        )
        self.report.data["sales_raw.csv"]["loaded_breakdown"]["order_items"] = int(
            loaded_items
        )
        self.report.data["sales_raw.csv"]["loaded"] = int(loaded_orders + loaded_items)
        return loaded_orders, loaded_items


# ==========================================================
# BLOCK 5 — Orchestration & Reporting
# Purpose: Orchestrate the end-to-end ETL run and produce the QA report.
# Outcome: One-click execution with timing, logging, and a written summary.
# ==========================================================


def run():
    cfg = Config()
    run_id = uuid.uuid4().hex[:8]
    logger = setup_logger(cfg, run_id)
    start_ts = time.time()
    logger.info("ETL run started", extra={"stage": "start"})
    etl = FlexiMartETL(cfg, logger)
    loader = Loader(cfg, logger, etl.report)
    try:
        # Extract & Transform
        etl.extract()
        etl.transform_customers()
        etl.transform_products()
        etl.transform_sales_initial()
        # Load
        loader.open()
        loader.upsert_customers(etl.customers)
        loader.load_products(etl.products)
        # Remap sales to DB customer ids BEFORE building orders/items
        remapped_sales = loader.remap_sales_customer_ids(etl.sales)
        loader.load_orders_and_items(remapped_sales)
        # Write report
        etl.report.write(cfg, run_id)
        elapsed = round(time.time() - start_ts, 2)
        logger.info(
            f"ETL run finished successfully in {elapsed}s", extra={"stage": "end"}
        )
        print("ETL Pipeline Completed Successfully.")
    except Exception:
        elapsed = round(time.time() - start_ts, 2)
        logger.exception(f"ETL run failed after {elapsed}s", extra={"stage": "error"})
        raise
    finally:
        loader.close()


if __name__ == "__main__":
    run()