package notaql.performance_tests.redis.sum_inserts;

import notaql.performance_tests.redis.PerformanceTestRedisIncrementalSnapshot;

/**
 * Tests a simple SUM() with additionaly inserted data
 */
public class PerformanceTestRedisIncrementalSnapshotSumInserts extends PerformanceTestRedisIncrementalSnapshot {
	/* (non-Javadoc)
	 * @see notaql.performance_tests.PerformanceTest#test()
	 */
	@Override
	public void test() throws Exception {
		// Prepare the 1. execution
		logCurrentTimestampToFile("Start Test");
		final String query = "OUT._k <- 'result', OUT._v <- SUM(IN._v)";
		addTestDataSalary(input, DATA_SIZE, 0);
		logCurrentTimestampToFile("Data added 1");
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		logCurrentTimestampToFile("Execution 1");
		
		
		// Prepare the 2. execution
		createSnapshot();
		logCurrentTimestampToFile("Snapshot");

		int newRows = (int) (DATA_SIZE*DATA_CHANGE_PERCENTAGE);
		addTestDataSalary(input, newRows, DATA_SIZE);
		logCurrentTimestampToFile("Data added 2");
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		logCurrentTimestampToFile("Execution 2");
	}
	
	
	public static void main(String[] args) throws Exception {
		(new PerformanceTestRedisIncrementalSnapshotSumInserts()).execute();
	}
}
