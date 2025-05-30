// queries.js - MongoDB CRUD, advanced queries, aggregation, and indexing in one file

const { MongoClient } = require('mongodb');

require('dotenv').config();
const uri = process.env.MONGO_URI;
const client = new MongoClient(uri);
const dbName = 'plp_bookstore';
const collectionName = 'books';



async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // TASK 1: Setup (database and collection are created implicitly when you insert data)
    console.log('Connected to database:', dbName);

    // TASK 2: Basic CRUD Queries

    // Find all books in a specific genre (e.g., Fiction)
    const fictionBooks = await collection.find({ genre: "Fiction" }).toArray();
    console.log('\nBooks in genre "Fiction":');
    fictionBooks.forEach(book => console.log(`- ${book.title}`));

    // Find books published after a certain year (e.g., 2000)
    const booksAfter2000 = await collection.find({ published_year: { $gt: 2000 } }).toArray();
    console.log('\nBooks published after 2000:');
    booksAfter2000.forEach(book => console.log(`- ${book.title} (${book.published_year})`));

    // Find books by a specific author (e.g., George Orwell)
    const orwellBooks = await collection.find({ author: "George Orwell" }).toArray();
    console.log('\nBooks by George Orwell:');
    orwellBooks.forEach(book => console.log(`- ${book.title}`));

    // Update the price of a specific book (e.g., '1984')
    const updateResult = await collection.updateOne(
      { title: "1984" },
      { $set: { price: 12.99 } }
    );
    console.log(`\nUpdated price for "1984": ${updateResult.modifiedCount} document(s) modified`);

    // Delete a book by its title (e.g., 'Moby Dick')
    const deleteResult = await collection.deleteOne({ title: "Moby Dick" });
    console.log(`\nDeleted "Moby Dick": ${deleteResult.deletedCount} document(s) deleted`);


    // TASK 3: Advanced Queries

    // Find books both in stock and published after 2010
    const recentInStockBooks = await collection.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).toArray();
    console.log('\nBooks in stock and published after 2010:');
    recentInStockBooks.forEach(book => console.log(`- ${book.title} (${book.published_year})`));

    // Projection: return only title, author, and price fields
    const projectedBooks = await collection.find({}, {
      projection: { title: 1, author: 1, price: 1, _id: 0 }
    }).toArray();
    console.log('\nBooks with projection (title, author, price):');
    console.log(projectedBooks);

    // Sorting: display books by price ascending
    const booksSortedByPriceAsc = await collection.find().sort({ price: 1 }).toArray();
    console.log('\nBooks sorted by price (ascending):');
    booksSortedByPriceAsc.forEach(book => console.log(`- ${book.title}: $${book.price}`));

    // Sorting: display books by price descending
    const booksSortedByPriceDesc = await collection.find().sort({ price: -1 }).toArray();
    console.log('\nBooks sorted by price (descending):');
    booksSortedByPriceDesc.forEach(book => console.log(`- ${book.title}: $${book.price}`));

    // Pagination: 5 books per page, show page 2 (skip 5)
    const pageSize = 5;
    const pageNumber = 2;
    const paginatedBooks = await collection.find()
      .skip(pageSize * (pageNumber - 1))
      .limit(pageSize)
      .toArray();
    console.log(`\nBooks page ${pageNumber} (5 per page):`);
    paginatedBooks.forEach(book => console.log(`- ${book.title}`));


    // TASK 4: Aggregation Pipelines

    // 1. Calculate average price of books by genre
    const avgPriceByGenre = await collection.aggregate([
      { $group: { _id: "$genre", averagePrice: { $avg: "$price" } } },
      { $sort: { averagePrice: -1 } }
    ]).toArray();
    console.log('\nAverage price of books by genre:');
    avgPriceByGenre.forEach(genre => console.log(`- ${genre._id}: $${genre.averagePrice.toFixed(2)}`));

    // 2. Find author with the most books
    const authorWithMostBooks = await collection.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log('\nAuthor with the most books:');
    authorWithMostBooks.forEach(author => console.log(`- ${author._id}: ${author.count} books`));

    // 3. Group books by publication decade and count them
    const booksByDecade = await collection.aggregate([
      {
        $group: {
          _id: {
            $concat: [
              { $toString: { $subtract: [ "$published_year", { $mod: [ "$published_year", 10 ] } ] } },
              "s"
            ]
          },
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log('\nBooks grouped by publication decade:');
    booksByDecade.forEach(decade => console.log(`- ${decade._id}: ${decade.count}`));


    // TASK 5: Indexing

    // Create an index on title field
    const indexTitle = await collection.createIndex({ title: 1 });
    console.log('\nCreated index on title:', indexTitle);

    // Create a compound index on author and published_year
    const compoundIndex = await collection.createIndex({ author: 1, published_year: -1 });
    console.log('Created compound index on author and published_year:', compoundIndex);

    // Use explain() to show index usage for a query filtering on author and published_year
    const explainResult = await collection.find({ author: "George Orwell", published_year: { $gt: 1940 } })
      .explain("executionStats");
    console.log('\nExplain plan for query using indexes:');
    console.dir(explainResult.executionStats, { depth: null });

  } catch (err) {
    console.error('Error running queries:', err);
  } finally {
    await client.close();
  }
}

runQueries();
