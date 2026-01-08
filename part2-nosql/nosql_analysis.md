# Section A: Limitations of RDBMS 

FlexiMart’s current relational schema is optimized for structured, uniform data, but is not optimized for diverse product types with diverse product attributes.
1.	Different attributes per product — A single products table forces all items to share the same columns. Laptops, shoes, and groceries each require unique attributes; adding them leads to excessive nulls or complex side tables, increasing joins and reducing performance and agility.
2.	Frequent schema changes — Introducing each new product type will require altering the schema, adding columns, modifying constraints, and updating ETL and application logic. This database operation is expensive for both database and applications and will require frequent downtime.
3.	Storing nested customer reviews — Relational databases are optimized for normalized data where review would be stored in separate table. Separate review tables makes retrieval heavier and preventing product centric, nested data structures that modern applications expect.

# Section B: NoSQL Benefits

MongoDB’s document model provides the flexibility needed for a rapidly expanding catalog.
1.	Flexible schema — MongoDB stores data in JSON-like documents, allowing each product record to have a different structure. E.g.  laptops record includes technical specs while shoes store size and color without forcing a rigid global schema. No schema migrations are required as new product types emerge.
2.	Embedded documents — MongoDB allows embedding related data (like product reviews) directly within the main product document, creating a nested, hierarchical structure. This structure allows for natural nesting, faster reads, and fewer joins
3.	Horizontal scalability — MongoDB’s sharding architecture distributes data across nodes, supporting high volume product catalogs and intensive read/write workloads. As the number of products and reviews grows, FlexiMart can simply add more servers to the cluster to handle the increased load and data volume, rather than having to upgrade a single, more powerful server

# Section C: Trade-offs 

Two disadvantages of using MongoDB instead of MySQL for this product catalog

•  Weaker transactional guarantees — MySQL strictly enforces data integrity rules by design due its rigid, predefined schema. While MongoDB supports ACID transactions, its dynamic schema makes enforcing complex multi table transactional workloads more challenging. This matters if product updates must be tightly synchronized with inventory or pricing operations. 
•  Potential for data duplication — Because MongoDB encourages embedding, the same attribute or reference may appear across multiple documents, increasing storage consumption and complicating updates if shared data changes. In contrast, MySQL’s normalized structure avoids duplication and enforces referential integrity more strictly.
