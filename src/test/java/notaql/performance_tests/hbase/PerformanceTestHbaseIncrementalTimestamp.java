package notaql.performance_tests.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;

public abstract class PerformanceTestHbaseIncrementalTimestamp extends PerformanceTestHbaseFullrecomputation {
	// Configuration
	private static final int NUMBER_OF_MAX_VERSIONS = 2;
	
	
	// Class variables
	protected static long timestampPreviousExecution = System.currentTimeMillis();

	
	/**
	 * May be overwritten (e.g. for supporting timestamps).
	 * 
	 * @return the input table
	 * @throws IOException
	 */
	protected HTable createInputTable() throws IOException {
		return getTable(TABLE_IN_ID, NUMBER_OF_MAX_VERSIONS);
	}
	
	
	/**
	 * Truncates the tables.
	 */
	@Override
	protected void tearDown() throws Exception {
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
			sb.append("IN-ENGINE: hbase(table_id <- '" + input.getName() + "', timestamp <- '" + timestampPreviousExecution + "'),");
		else
			sb.append("IN-ENGINE: hbase(table_id <- '" + input.getName() + "'),");
		
		sb.append("OUT-ENGINE: hbase(table_id <- '" + output.getName() + "'),");
		sb.append(queryBody);
		

		timestampPreviousExecution = System.currentTimeMillis();		
		return sb.toString();
	}
}
