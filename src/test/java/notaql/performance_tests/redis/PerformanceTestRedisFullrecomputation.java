package notaql.performance_tests.redis;

import java.util.Random;

import notaql.engines.redis.RedisApi;
import notaql.incremental_tests.Loglevel;
import notaql.performance_tests.PerformanceTest;

/**
 * Methods and variables to be used for Redis-tests.
 */
public abstract class PerformanceTestRedisFullrecomputation extends PerformanceTest {
	// Configuration
	protected static final int input = 11;
	protected static final int output = 12;
	
	
	// Class variables
	protected RedisApi redisApi;
	private Random random;
	
	
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
		
		redisApi = new RedisApi();
	}
	
	
	/**
	 * Drops the tables which were created by beforeClass().
	 * 
	 * Also closes the HBase connections.
	 */
	@Override
	protected void afterClass() throws Exception {
		super.afterClass();
		
		redisApi.close();
	}
	
	
	/**
	 * Truncates the tables.
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		
		drop(input);
		drop(output);
	}
	
	
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody 
	 * @return
	 */
	protected static String generateQuery(String queryBody) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("IN-ENGINE: redis(database_id <- '" + input + "'),");
		sb.append("OUT-ENGINE: redis(database_id <- '" + output + "'),");
		sb.append(queryBody);
		
		return sb.toString();
	}
	
	
	/**
	 * Drops the given database.  
	 * 
	 * @param databaseId
	 */
	protected void drop(int databaseId) {
		log("drop('" + databaseId + "')", Loglevel.DEBUG);

		redisApi.dropDatabase(databaseId);
	}
	
	
	/**
	 * Puts a value to the given database.
	 * 
	 * @param databaseId
	 * @param key
	 * @param hashKey
	 * @param value
	 */
	protected void put(int databaseId, String key, Object value) {
		// To many puts... commented out
		// log("put('" + databaseId + "', '" + key + "', '" + value + "' (Type: " + value.getClass().getSimpleName() + "))", Loglevel.DEBUG);
		
		redisApi.put(databaseId, key, value);
	}
	
	
	/**
	 * Inserts new testdata into the database.
	 * 
	 * The testdata contains salary informations.
	 * 
	 * @param databaseId
	 * @param quantity
	 * @param offset
	 */
	protected void addTestDataSalary(int databaseId, int quantity, int offset) {
		// Check input
		if (quantity == 0)
			return;
		
		
		// Insert new data
		for (int i=offset; i<quantity+offset; i++)
			put(databaseId, String.valueOf(i), (i%2)+1);
	}
	
	
	/**
	 * Changes testdata in the database.
	 * 
	 * The testdata contains salary informations.
	 * 
	 * @param databaseId
	 * @param changePercentage > 0 (nothing changed) && < 1 (everything is new)
	 */
	protected void changeTestDataSalary(int databaseId, double changePercentage) {
		// Check input
		if (changePercentage == 0)
			return;
		else if (changePercentage > 1 || changePercentage < 0)
			throw new IllegalArgumentException("changePercentage out of range");
		
		
		if (random == null)
			random = new Random();
			
		
		// Change the previous data
		int changeDataBound = (int) (changePercentage * DATA_SIZE);
		
		if (changeDataBound == 0)
			log("No data changed (DATA_SIZE = " + DATA_SIZE + ", changePercentage = " + changePercentage + ")", Loglevel.WARN);
		else
			log("Changing " + changeDataBound + " values", Loglevel.INFO);
		
		for (int i=0; i<changeDataBound; i++) {
			int randomId = i;
			put(databaseId, String.valueOf(randomId), (randomId%2)+2);
		}
	}
}
