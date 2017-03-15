package notaql.performance_tests.mongodb;

import java.io.IOException;

import com.mongodb.DBCollection;

import notaql.incremental_tests.Loglevel;

/**
 * Methods and variables to be used for Mongodb-tests.
 * 
 * The database will be created in the first run and dropped in the last run. Because of this the statistics of these two runs
 * should be discarded.
 */
public abstract class PerformanceTestMongodbIncrementalSnapshot extends PerformanceTestMongodbFullrecomputation {
	// Configuration
	private static final String COLLECTION_SNAPSHOT_NAME = "snapshot";
	
	
	// Class variables
	private static DBCollection snapshot;
	
	
	/**
	 * Truncates the tables.
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		
		if (snapshot != null)
			truncate(snapshot);
	}
	
	
	/**
	 * Creates a copy of the input-table.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void createSnapshot() throws IOException, InterruptedException {
		log("createSnapshot()", Loglevel.DEBUG);
		
		snapshot = mongoApi.copyCollection(input, COLLECTION_SNAPSHOT_NAME);
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
			sb.append("IN-ENGINE: mongodb(database_name <- '" + DATABASE_NAME + "', collection_name <- '" + input.getName() + "', snapshot_collection_name <- '" + snapshot.getName() + "'),");
		else
			sb.append("IN-ENGINE: mongodb(database_name <- '" + DATABASE_NAME + "', collection_name <- '" + input.getName() + "'),");
		
		sb.append("OUT-ENGINE: mongodb(database_name <- '" + DATABASE_NAME + "', collection_name <- '" + output.getName() + "'),");
		sb.append(queryBody);
		
		return sb.toString();
	}
}
