package notaql.performance_tests.redis;

import java.io.IOException;

import notaql.incremental_tests.Loglevel;

public abstract class PerformanceTestRedisIncrementalSnapshot extends PerformanceTestRedisFullrecomputation {
	// Configuration
	private static final int snapshot = 13;
	
	
	/**
	 * Truncates the tables.
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		
		drop(snapshot);
	}
	
	
	/**
	 * Creates a copy of the input-table.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void createSnapshot() throws IOException, InterruptedException {
		log("createSnapshot()", Loglevel.DEBUG);
		
		redisApi.copyDatabase(input, snapshot);
	}
	
	
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody
	 * @param isSnapshotQuery if set to true the query will be executed as snapshot-based incremental query. 
	 * @return
	 */
	protected static String generateQuery(String queryBody, boolean isSnapshotQuery) {
		StringBuilder sb = new StringBuilder();
		
		if (isSnapshotQuery)
			sb.append("IN-ENGINE: redis(database_id <- '" + input + "'),");
		else
			sb.append("IN-ENGINE: redis(database_id <- '" + input + "', snapshot_database_id <- '" + snapshot + "'),");
		
		sb.append("OUT-ENGINE: redis(database_id <- '" + output + "'),");
		sb.append(queryBody);
		
		return sb.toString();
	}
}
