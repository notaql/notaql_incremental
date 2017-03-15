package notaql.incremental_tests.redis;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import notaql.engines.redis.RedisApi;
import notaql.incremental_tests.Loglevel;
import notaql.incremental_tests.Test;

/**
 * Methods and variables to be used for Redis-tests.
 */
public abstract class TestRedis extends Test {
	// Configuration
	protected static final int input = 11;
	protected static final int output = 12;
	
	
	// Class variables
	protected static RedisApi redisApi;
	
	
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
		
		redisApi = new RedisApi();
	}
	
	
	/**
	 * Drops the tables which were created by beforeClass().
	 * 
	 * Also closes the HBase connections.
	 */
	@AfterClass
	public static void afterClass() throws Exception {
		Test.afterClass();
		
		redisApi.close();
	}
	
	
	/**
	 * Truncates the tables.
	 */
	@After
	public void tearDown() throws Exception {
		log("tearDown()", Loglevel.DEBUG);
		
		drop(input);
		drop(output);
	}
	
	
	/**
	 * Drops the given database.  
	 * 
	 * @param databaseId
	 */
	protected static void drop(int databaseId) {
		logStatic(TestRedis.class, "drop('" + databaseId + "')", Loglevel.DEBUG);

		redisApi.dropDatabase(databaseId);
	}
	
	
	/**
	 * Deletes an object from the given database.
	 * 
	 * @param databaseId
	 * @param key
	 */
	protected void delete(int databaseId, String key) {
		log("delete('" + databaseId + "', '" + key + "')", Loglevel.DEBUG);
		
		redisApi.delete(databaseId, key);
	}
	
	
	/**
	 * Gets data from the given database by object-id and key.
	 * 
	 * @param databaseId
	 * @param key
	 * @param hashKey
	 * @return the data
	 */
	protected String getString(int databaseId, String key, String hashKey) {
		log("getString('" + databaseId + "', '" + key + "', '" + hashKey + "')", Loglevel.DEBUG);
		
		return redisApi.get(databaseId, key, hashKey);
	}
	
	
	/**
	 * Gets data from the given database by object-id and key.
	 * 
	 * @param databaseId
	 * @param key
	 * @param hashKey
	 * @return the data
	 */
	protected int getInteger(int databaseId, String key, String hashKey) {
		log("getInteger('" + databaseId + "', '" + key + "', '" + hashKey + "')", Loglevel.DEBUG);
		
		return redisApi.getNumber(databaseId, key, hashKey).intValue();
	}
	
	
	/**
	 * Truncates the given database.
	 * 
	 * @param databaseId
	 */
	@Deprecated() // Use drop()
	public void truncate(int databaseId) {
		log("truncate('" + databaseId + "')", Loglevel.DEBUG);
		
		drop(databaseId);
	}
	
	
	/**
	 * Puts a value to the given database.
	 * 
	 * @param databaseId
	 * @param key
	 * @param hashKey
	 * @param value
	 */
	protected void put(int databaseId, String key, String hashKey, Object value) {
		log("put('" + databaseId + "', '" + key + "', '" + hashKey + "', '" + value + "' (Type: " + value.getClass().getSimpleName() + "))", Loglevel.DEBUG);
		
		redisApi.put(databaseId, key, hashKey, value);
	}
}
