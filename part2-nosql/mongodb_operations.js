
// mongodb_operations.csvaware.js
// Purpose: Load products from JSON **or** CSV into 'products' and run operations 1-5.
// Usage: Execute via mongosh with env vars MONGO_URI, MONGO_DB, PRODUCTS_FILE.
// Notes: CSV expects a header row. Common columns: product_id,name,category,price,stock

const fs = require('fs');

const {
  MONGO_URI = 'mongodb://localhost:27017',
  MONGO_DB = 'catalogdb',
  PRODUCTS_FILE = '/data/products_catalog.json'
} = process.env;

// Connect to MongoDB
// mongosh provides global connect()
db = connect(MONGO_URI);
if (MONGO_DB) {
  db = db.getSiblingDB(MONGO_DB);
}

function isCSV(path) {
  return path.toLowerCase().endsWith('.csv');
}

function parseCSV(text) {
  // Simple CSV parser: handles commas, trims spaces, no quoted commas handling.
  // For quoted fields/commas, consider using mongoimport or enhance parser.
  const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
  if (lines.length === 0) return [];
  const headers = lines[0].split(',').map(h => h.trim());
  const docs = [];
  for (let i = 1; i < lines.length; i++) {
    const row = lines[i];
    const cols = row.split(',');
    const doc = {};
    for (let j = 0; j < headers.length; j++) {
      const key = headers[j];
      const raw = (cols[j] || '').trim();
      // Attempt numeric conversion for common fields
      if (['price', 'stock'].includes(key)) {
        const num = Number(raw);
        doc[key] = isNaN(num) ? raw : num;
      } else {
        doc[key] = raw;
      }
    }
    docs.push(doc);
  }
  return docs;
}

// --- Operation 1: Load Data ---
// Import the provided JSON file into collection 'products'

(function loadData() {
  print('## Operation 1: Load Data');
  let docs = [];
  try {
    const text = fs.readFileSync(PRODUCTS_FILE, 'utf8');
    if (isCSV(PRODUCTS_FILE)) {
      docs = parseCSV(text);
    } else {
      docs = EJSON.parse(text, { relaxed: true });
      if (!Array.isArray(docs)) docs = [docs];
    }
    if (!Array.isArray(docs)) docs = [docs];
  } catch (e) {
    print(`Failed to read/parse PRODUCTS_FILE: ${PRODUCTS_FILE}`);
    printjson(e);
    return;
  }
  if (docs.length === 0) {
    print('No documents parsed from PRODUCTS_FILE.');
    return;
  }
  const res = db.products.insertMany(docs, { ordered: false });
  printjson({ insertedCount: res.insertedCount });
  print(`products count: ${db.products.countDocuments()}`);
})();

// --- Operation 2: Basic Query ---
// Find all products in "Electronics" category with price less than 50000
// Return only: name, price, stock

(function basicQuery() {
  print('## Operation 2: Basic Query — Electronics < 50000');
  db.products
    .find(
      { category: 'Electronics', price: { $lt: 50000 } },
      { _id: 0, name: 1, price: 1, stock: 1 }
    )
    .forEach(doc => printjson(doc));
})();

// --- Operation 3: Review Analysis ---
// Find all products that have average rating >= 4.0
// Use aggregation to calculate average from reviews array


(function reviewAnalysis() {
  print('## Operation 3: Review Analysis — avgRating >= 4.0');
  db.products
    .aggregate([
      { $addFields: { avgRating: { $avg: '$reviews.rating' } } },
      { $match: { avgRating: { $gte: 4.0 } } },
      { $project: { _id: 0, name: 1, avgRating: 1 } }
    ])
    .forEach(doc => printjson(doc));
})();

// --- Operation 4: Update Operation ---
// Add a new review to product "ELEC001"
// Review: {user: "U999", rating: 4, comment: "Good value", date: ISODate()}


(function addReview() {
  print('## Operation 4: Update — push new review to ELEC001');
  const updateRes = db.products.updateOne(
    { product_id: 'ELEC001' },
    {
      $push: {
        reviews: {
          user: 'U999',
          rating: 4,
          comment: 'Good value',
          date: new Date()
        }
      }
    }
  );
  printjson(updateRes);
  print('Post-update product:');
  printjson(
    db.products.findOne(
      { product_id: 'ELEC001' },
      { _id: 0, product_id: 1, reviews: 1 }
    )
  );
})();

// --- Operation 5: Complex Aggregation ---
// Calculate average price by category
// Return: category, avg_price, product_count
// Sort by avg_price descending

(function avgPriceByCategory() {
  print('## Operation 5: Aggregation — avg price by category (desc)');
  db.products
    .aggregate([
      {
        $group: {
          _id: '$category',
          avg_price: { $avg: '$price' },
          product_count: { $sum: 1 }
        }
      },
      { $project: { _id: 0, category: '$_id', avg_price: 1, product_count: 1 } },
      { $sort: { avg_price: -1 } }
    ])
    .forEach(doc => printjson(doc));
})();
