package notaql.performance_tests.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;

import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;

import notaql.engines.mongodb.MongoApi;
import notaql.incremental_tests.Loglevel;
import notaql.performance_tests.PerformanceTest;

/**
 * Methods and variables to be used for Mongodb-tests.
 * 
 * The database will be created in the first run and dropped in the last run. Because of this the statistics of these two runs
 * should be discarded.
 */
public abstract class PerformanceTestMongodbFullrecomputation extends PerformanceTest {
	// Configuration
	protected static final String DATABASE_NAME = TEST_PREFIX + "db";
	protected static final String COLLECTION_INPUT_NAME = "input";
	private static final String COLLECTION_OUTPUT_NAME = "output";
	
	
	// Class variables
	protected static MongoApi mongoApi;
	protected static DBCollection input;
	protected static DBCollection output;
	
	
	// Object variables
	private Map<DBCollection, BulkWriteOperation> bulks = new HashMap<DBCollection, BulkWriteOperation>();
	private List<ObjectId> insertedDocuments;
	
	
	/**
	 * Sets up the needed tables for the OUTPUT of the transformation.
	 * 
	 * After each test these collections will be truncated. This should be faster compared
	 * to removing and re-creating the tables each time.
	 * 
	 * 
	 * Also sets up the MongoDB connection for the MongoAPI.
	 */
	@Override
	protected void beforeClass() throws Exception {
		super.beforeClass();
		
		mongoApi = new MongoApi();
		input = getCollection(COLLECTION_INPUT_NAME);
		output = getCollection(COLLECTION_OUTPUT_NAME);
	}
	
	
	/**
	 * Drops the tables which were created by beforeClass().
	 * 
	 * Also closes the HBase connections.
	 */
	@Override
	protected void afterClass() throws Exception {
		super.afterClass();
		
		drop(DATABASE_NAME);
		mongoApi.close();
	}
	
	
	/**
	 * Truncates the tables.
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		
		if (input == null)
			throw new IllegalStateException("tableInput was not initialized");
		
		truncate(input);
		truncate(output);
		
		insertedDocuments = null;
	}
	
	
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody 
	 * @return
	 */
	protected static String generateQuery(String queryBody) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("IN-ENGINE: mongodb(database_name <- '" + DATABASE_NAME + "', collection_name <- '" + input.getName() + "'),");
		sb.append("OUT-ENGINE: mongodb(database_name <- '" + DATABASE_NAME + "', collection_name <- '" + output.getName() + "'),");
		sb.append(queryBody);
		
		return sb.toString();
	}
	
	
	/**
	 * Creates a new collection with the given name if it doesn't exist.  
	 * 
	 * @param collectionName
	 * @return the collection
	 * @throws IOException
	 */
	protected DBCollection getCollection(String collectionName) throws IOException {
		log("getCollection('" + collectionName + "')", Loglevel.DEBUG);
		
		return mongoApi.getCollection(DATABASE_NAME, collectionName, true);
	}
	
	
	/**
	 * Drops the given database.  
	 * 
	 * @param databaseName
	 * @throws IOException
	 */
	protected void drop(String databaseName) throws IOException {
		log("drop('" + databaseName + "')", Loglevel.DEBUG);

		mongoApi.dropDatabase(databaseName);
	}
	
	
	/**
	 * Truncates the given collection.
	 * 
	 * @param collection
	 */
	public void truncate(DBCollection collection) {
		log("truncate('" + collection.getName() + "')", Loglevel.DEBUG);
		
		mongoApi.truncate(collection);
	}
	
	
	/**
	 * Puts *one* value to the given table.
	 * 
	 * @param collection
	 * @param objectId
	 * @param key
	 * @param value
	 * @param upsert
	 */
	protected void put(DBCollection collection, ObjectId objectId, String key, Object value, boolean upsert) {
		// To many puts... commented out
		// log("put('" + collection.getName() + "', '" + objectId + "', '" + key + "', '" + value + "')", Loglevel.DEBUG);
		
		if (!bulks.containsKey(collection))
			bulks.put(collection, collection.initializeUnorderedBulkOperation());
		
		mongoApi.put(collection, "_id", objectId, key, value, false, upsert, bulks.get(collection));
	}
	
	
	/**
	 * Commits a bulk write operation
	 * 
	 * @param collection
	 * @return the result or null
	 */
	private void commit(DBCollection collection) {
		if (bulks.containsKey(collection)) {
			bulks.get(collection).execute();
			bulks.remove(collection);
		}
	}
	
	
	/**
	 * Inserts new testdata into the database.
	 * 
	 * The testdata contains salary informations.
	 * 
	 * @param databaseId
	 * @param quantity
	 */
	protected final String DATA_SALARY_COLUMN_NAME = "salary";
	protected void addTestDataSalary(DBCollection collection, int quantity, int offset) {
		// Check input
		if (quantity == 0)
			return;
		
		
		// Create the list for the data
		if (insertedDocuments == null)
			insertedDocuments = new ArrayList<ObjectId>(quantity);
		
		
		// Insert new data
		for (int i=offset; i<quantity+offset; i++) {
			ObjectId generatedId = ObjectId.get();
			put(collection, generatedId, DATA_SALARY_COLUMN_NAME, (i%2)+1, true);
			insertedDocuments.add(generatedId);
		}
		
		commit(collection);
	}
	
	
	/**
	 * Changes testdata in the database.
	 * 
	 * The testdata contains salary informations.
	 * 
	 * @param databaseId
	 * @param changePercentage > 0 (nothing changed) && < 1 (everything is new)
	 */
	protected void changeTestDataSalary(DBCollection collection, double changePercentage) {
		// Check input
		if (changePercentage == 0)
			return;
		else if (changePercentage > 1 || changePercentage < 0)
			throw new IllegalArgumentException("changePercentage out of range");
			
		
		// Change the previous data
		int changeDataBound = (int) (changePercentage * DATA_SIZE);
		
		if (changeDataBound == 0)
			log("No data changed (DATA_SIZE = " + DATA_SIZE + ", changePercentage = " + changePercentage + ")", Loglevel.WARN);
		else
			log("Changing " + changeDataBound + " values", Loglevel.INFO);

		for (int i=0; i<changeDataBound; i++) {
			put(collection, insertedDocuments.get(i), DATA_SALARY_COLUMN_NAME, (i%2)+2, true);
		}
		
		commit(collection);
	}
}
