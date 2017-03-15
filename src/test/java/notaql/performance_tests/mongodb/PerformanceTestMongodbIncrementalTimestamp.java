package notaql.performance_tests.mongodb;

/**
 * Methods and variables to be used for Mongodb-tests.
 * 
 * The database will be created in the first run and dropped in the last run. Because of this the statistics of these two runs
 * should be discarded.
 */
public abstract class PerformanceTestMongodbIncrementalTimestamp extends PerformanceTestMongodbFullrecomputation {
	// Class variables
	private static long timestampPreviousExecution;
	
	
	/* (non-Javadoc)
	 * @see notaql.performance_tests.mongodb.PerformanceTestMongodbFullrecomputation#tearDown()
	 */
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		
		timestampPreviousExecution = 0;
	}
	
	
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody
	 * @param isTimestampQuery 
	 * @return
	 */
	protected static String generateQuery(String queryBody, boolean isTimestampQuery) {
		StringBuilder sb = new StringBuilder();
		
		if (isTimestampQuery)
			sb.append("IN-ENGINE: mongodb(database_name <- '" + DATABASE_NAME + "', collection_name <- '" + input.getName() + "', timestamp <- '" + timestampPreviousExecution + "'),");
		else
			sb.append("IN-ENGINE: mongodb(database_name <- '" + DATABASE_NAME + "', collection_name <- '" + input.getName() + "'),");
		
		sb.append("OUT-ENGINE: mongodb(database_name <- '" + DATABASE_NAME + "', collection_name <- '" + output.getName() + "'),");
		sb.append(queryBody);

		
		timestampPreviousExecution = System.currentTimeMillis();
		
		return sb.toString();
	}
}
