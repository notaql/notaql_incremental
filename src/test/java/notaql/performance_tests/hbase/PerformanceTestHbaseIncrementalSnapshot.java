package notaql.performance_tests.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;

import notaql.incremental_tests.Loglevel;

public abstract class PerformanceTestHbaseIncrementalSnapshot extends PerformanceTestHbaseFullrecomputation {
	// Configuration
	private static final String TABLE_SNAPSHOT_ID = TEST_PREFIX + "snapshot";
	
	
	// Object variables
	protected static HTable snapshot;

	
	/**
	 * Truncates the tables.
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		
		if (snapshot != null)
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
		
		snapshot = hbaseApi.copyTable(TABLE_IN_ID, TABLE_SNAPSHOT_ID, TABLE_IN_ID + "_snapshot");
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
			sb.append("IN-ENGINE: hbase(table_id <- '" + input.getName() + "', snapshot_table_id <- '" + snapshot.getName() + "'),");
		else
			sb.append("IN-ENGINE: hbase(table_id <- '" + input.getName() + "'),");
		
		sb.append("OUT-ENGINE: hbase(table_id <- '" + output.getName() + "'),");
		sb.append(queryBody);
		
		return sb.toString();
	}
}
