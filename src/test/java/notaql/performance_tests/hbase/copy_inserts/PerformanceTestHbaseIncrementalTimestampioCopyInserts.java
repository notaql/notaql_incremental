package notaql.performance_tests.hbase.copy_inserts;

import notaql.engines.Engine;
import notaql.performance_tests.hbase.PerformanceTestHbaseIncrementalTimestamp;

/**
 * Copys values (with additionaly inserted data)
 */
public class PerformanceTestHbaseIncrementalTimestampioCopyInserts extends PerformanceTestHbaseIncrementalTimestamp {
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody
	 * @param isTimestampQuery
	 * @return
	 */
	public static String generateQuery(String queryBody, boolean isTimestampQuery) {
		StringBuilder sb = new StringBuilder();
		
		if (isTimestampQuery)
			sb.append("IN-ENGINE: hbase(table_id <- '" + input.getName() + "', timestamp <- '" + timestampPreviousExecution + "', " + Engine.PARAMETER_NAME_USER_EXPECTS_UPDATES + " <- 'false', " + Engine.PARAMETER_NAME_USER_EXPECTS_DELETES + " <- 'false'),");
		else
			sb.append("IN-ENGINE: hbase(table_id <- '" + input.getName() + "'),");
		
		sb.append("OUT-ENGINE: hbase(table_id <- '" + output.getName() + "'),");
		sb.append(queryBody);
		

		timestampPreviousExecution = System.currentTimeMillis();		
		return sb.toString();
	}
	
	
	/* (non-Javadoc)
	 * @see notaql.performance_tests.PerformanceTest#test()
	 */
	@Override
	public void test() throws Exception {
		// Prepare the 1. execution
		logCurrentTimestampToFile("Start Test");
		final String query = "OUT._r <- IN._r, OUT.$(IN._c) <- IN._v";
		addTestDataSalary(input, DATA_SIZE, 0);
		logCurrentTimestampToFile("Data added 1");
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		logCurrentTimestampToFile("Execution 1");
		
		
		// Prepare the 2. execution
		int newRows = (int) (DATA_SIZE*DATA_CHANGE_PERCENTAGE);
		addTestDataSalary(input, newRows, DATA_SIZE);
		logCurrentTimestampToFile("Data added 2");
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		logCurrentTimestampToFile("Execution 2");
	}
	
	
	public static void main(String[] args) throws Exception {
		(new PerformanceTestHbaseIncrementalTimestampioCopyInserts()).execute();
	}
}
