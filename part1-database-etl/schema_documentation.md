## Entity-Relationship Description (Text Format)

## Task 1.2: Database Schema Documentation 

ENTITY: customers
Purpose: Stores customer information.
Attributes:
•	customer_id: Unique identifier for each customer (Primary Key)
•	first_name: Customer’s first name, (required) 
•	last_name: Customer’s last name, (required) 
•	email: Customer’s unique email address (enforced unique constraint), if no email id, then generate a fictious one @example.com
•	phone: Normalized phone number. Optional. Duplicates allowed (Two customers can have same phone number)
•	city: Customer’s city of residence, optional , yyyy-dd-mm format
•	registration_date: Date when the customer registered
Relationships:
•	One customer can place MANY orders (1:M relationship with orders table via customer_id).

ENTITY: products
Purpose: Stores catalog information for products sold by the business.
Attributes:
•	product_id: Unique identifier for each product (Primary Key)
•	product_name: Name of the product, (required) 
•	category: Standardized product category, (required) 
•	price: Unit price of the product, (required) 
•	stock_quantity: Available inventory count, default 0
Relationships:
•	One product can appear in MANY order_items (1:M relationship with order_items table via product_id).

ENTITY: orders
Purpose: Stores order level (header level) transaction information.
Attributes:
•	order_id: Unique identifier for each order (Primary Key)
•	customer_id: Foreign Key referencing customers.customer_id, (required)
•	order_date: Date when the order was placed, (required)
•	total_amount: Total computed order amount, (required)
•	status: Lifecycle status of the order (e.g., Pending, Completed), Default=Pending
Relationships:
•	Many orders could belong to ONE customer (M:1 with customers)
•	One order can have MANY order_items (1:M with order_items table via order_id)

ENTITY: order_items
Purpose: Stores the line level details of each order.
Attributes:
•	order_item_id: Unique identifier for each order item (Primary Key)
•	order_id: Foreign Key referencing orders.order_id
•	product_id: Foreign Key referencing products.product_id
•	quantity: Number of units of the product in this line
•	unit_price: Price per unit at the moment of purchase
•	subtotal: Calculated line amount (quantity × unit_price)
Relationships:
•	Many order_items belong to ONE order (M:1 with orders)
•	Many order_items reference ONE product (M:1 with products)

## Why the design is in 3NF (≈220 words)

Normalization restructures tables to reduce redundancy (save storage, improve maintainability)  ,  improve integrity (eliminate update anomalies) and improve query performance. The 3NF removes transitive dependencies (non-key fields shouldn’t depend on other non-key fields). 

All the 4 tables in this schema are in 3F because every non‑key attribute depends only on the primary key and there are no transitive dependencies or repeating groups.

ENTITY 1: customers
Primary Key: customer_id
•	All attributes (first_name, last_name, email, phone, city, registration_date) depend only on customer_id.
•	No derived attributes.
•	No attribute depends on another attribute (e.g., city doesn’t determine anything else).
•	No multi‑valued attributes; everything is atomic.

ENTITY 2: products
Primary Key: product_id
•	product_name, category, price, and stock_quantity all depend directly on product_id.
•	No attribute depends on another attribute (e.g., category does not determine price).
•	No computed or derived attribute

ENTITY 3: orders
Primary Key: order_id
•	customer_id, order_date, status all depend only on order_id.
•	total_amount is a computed value (sum of order_items subtotals).

ENTITY 4: order_items
Primary Key: order_item_id
•	All attributes depend directly on order_item_id: order_id, product_id, quantity, unit_price, subtotal.
•	Subtotal is a computed value. (quantity × unit_price)

By placing customer data in Customers, product data in Products, order headers in Orders, and line-item details in OrderItems, the schema avoids redundancies such as repeating product names across multiple orders. As a result, the design achieves Third Normal Form (3NF)

## Avoiding anomalies

- Update anomaly: Product price changes occur once in Products, not scattered across orders; customer email updates happen once in Customers.
- Insert anomaly: Can insert a new product without an order, or a new customer without immediate purchases (independent entities).
- Delete anomaly: Deleting an order or line item won’t erase product definitions or customer records; facts are isolated to their lifecycle tables.

## Sample data representation

# Customers
customer_id	first_name	last_name	email	phone	city	registration_date
1	Rahul	Sharma	rahul.sharma@gmail.com	+91-9876543210	Bangalore 2023-01-15
2	Priya	Patel	priya.patel@yahoo.com	+91-9988776655	Mumbai	2023-02-20
3	Amit	Kumar	unknown+3@example.com	+91-9765432109	Delhi	2023-03-10
# Products
product_id	product_name	    category	    price	    stock_quantity
1	        Samsung Galaxy S21	Electronics	    45999.00	150
2	        Nike Running Shoes	Fashion	        3499.00	    80
3	        Apple MacBook Pro	Electronics	    0.00	    45
# orders
order_id	customer_id	order_date	total_amount	status
1	1	2024-01-15	45999.00	Completed
2	2	2024-01-16	5998.00	Completed
3	3	2024-01-15	52999.00	Completed
# order_items
order_item_id	order_id	product_id	quantity	unit_price	subtotal
1	1	1	1	45999.00	45999.00
2	2	4	2	2999.00	5998.00
3	3	7	1	52999.00	52999.00


