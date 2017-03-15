package notaql.incremental_tests.mongodb;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.mongodb.DBCollection;

import notaql.engines.mongodb.MongoApi;
import notaql.incremental_tests.Loglevel;
import notaql.incremental_tests.Test;

/**
 * Methods and variables to be used for Redis-tests.
 */
public abstract class TestMongodb extends Test {
	// Configuration
	protected static final String DATABASE_NAME = TEST_PREFIX + "db";
	protected static final String COLLECTION_INPUT_NAME = "input";
	private static final String COLLECTION_OUTPUT_NAME = "output";
	
	
	// Class variables
	protected static MongoApi mongoApi;
	protected static DBCollection input; 
	protected static DBCollection output; 
	
	
	/**
	 * Sets up the needed tables for the OUTPUT of the transformation.
	 * 
	 * After each test these collections will be truncated. This should be faster compared
	 * to removing and re-creating the tables each time.
	 * 
	 * 
	 * Also sets up the MongoDB connection for the MongoAPI.
	 */
	@BeforeClass
	public static void beforeClass() throws Exception {
		Test.beforeClass();
		
		mongoApi = new MongoApi();
		input = getCollection(COLLECTION_INPUT_NAME);
		output = getCollection(COLLECTION_OUTPUT_NAME);
	}
	
	
	/**
	 * Drops the tables which were created by beforeClass().
	 * 
	 * Also closes the HBase connections.
	 */
	@AfterClass
	public static void afterClass() throws Exception {
		Test.afterClass();
		
		drop(DATABASE_NAME);
		mongoApi.close();
	}
	
	
	/**
	 * Truncates the tables.
	 */
	@After
	public void tearDown() throws Exception {
		log("tearDown()", Loglevel.DEBUG);
		
		if (input == null)
			throw new IllegalStateException("tableInput was not initialized");
		
		truncate(input);
		truncate(output);
	}
	
	
	/**
	 * Creates a new collection with the given name if it doesn't exist.  
	 * 
	 * @param collectionName
	 * @return the collection
	 * @throws IOException
	 */
	protected static DBCollection getCollection(String collectionName) throws IOException {
		logStatic(TestMongodb.class, "getCollection('" + collectionName + "')", Loglevel.DEBUG);
		
		return mongoApi.getCollection(DATABASE_NAME, collectionName);
	}
	
	
	/**
	 * Drops the given database.  
	 * 
	 * @param databaseName
	 * @throws IOException
	 */
	protected static void drop(String databaseName) throws IOException {
		logStatic(TestMongodb.class, "drop('" + databaseName + "')", Loglevel.DEBUG);

		mongoApi.dropDatabase(databaseName);
	}
	
	
	/**
	 * Deletes an object from the given collection.
	 * 
	 * @param collection
	 * @param objectId
	 */
	protected void delete(DBCollection collection, String objectId) {
		log("delete('" + collection.getName() + "', '" + objectId + "')", Loglevel.DEBUG);
		
		mongoApi.delete(collection, objectId);
	}
	
	
	/**
	 * Gets data from the given collection by object-id and key.
	 * 
	 * @param collection
	 * @param objectId
	 * @param key
	 * @return the data
	 */
	protected Object getObject(DBCollection collection, String objectId, String key) {
		log("getObject('" + collection.getName() + "', '" + objectId + "', '" + key + "')", Loglevel.DEBUG);
		
		return mongoApi.get(collection, objectId, key);
	}
	
	
	/**
	 * Gets data from the given collection by object-id and key.
	 * 
	 * @param collection
	 * @param objectId
	 * @param key
	 * @return the data
	 */
	protected String getString(DBCollection collection, String objectId, String key) {
		return (String) getObject(collection, objectId, key);
	}
	
	
	/**
	 * Gets data from the given collection by object-id and key.
	 * 
	 * @param collection
	 * @param objectId
	 * @param key
	 * @return the data
	 */
	protected int getInteger(DBCollection collection, String objectId, String key) {
		Object object = getObject(collection, objectId, key);
		
		if (object == null)
			throw new NullPointerException("object is null");
		else if (object instanceof Integer)
			return (Integer) getObject(collection, objectId, key);
		else if (object instanceof Double)
			return ((Double) object).intValue();
		else
			throw new IllegalStateException("object is a " + object.getClass().getSimpleName());
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
	 * Puts a value to the given table.
	 * 
	 * @param collection
	 * @param objectId
	 * @param key
	 * @param value
	 */
	protected void put(DBCollection collection, String objectId, String key, Object value) {
		log("put('" + collection.getName() + "', '" + objectId + "', '" + key + "', '" + value + "')", Loglevel.DEBUG);
		
		mongoApi.put(collection, objectId, key, value);
	}
}
