package notaql.performance_tests.hbase.sum_updates;

import notaql.performance_tests.hbase.PerformanceTestHbaseIncrementalSnapshot;

/**
 * Tests a simple SUM() with additionaly changed data
 */
public class PerformanceTestHbaseIncrementalSnapshotSumUpdates extends PerformanceTestHbaseIncrementalSnapshot {
	/* (non-Javadoc)
	 * @see notaql.performance_tests.PerformanceTest#test()
	 */
	@Override
	public void test() throws Exception {
		// Prepare the 1. execution
		logCurrentTimestampToFile("Start Test");
		final String query = "OUT._r <- 'result', OUT.sum <- SUM(IN." + DATA_SALARY_COLUMN_NAME + ")";
		addTestDataSalary(input, DATA_SIZE, 0);
		logCurrentTimestampToFile("Data added");
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		logCurrentTimestampToFile("Execution 1");
		
		
		// Prepare the 2. execution
		createSnapshot();
		logCurrentTimestampToFile("Snapshot");

		changeTestDataSalary(input, DATA_CHANGE_PERCENTAGE);
		logCurrentTimestampToFile("Data changed");
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		logCurrentTimestampToFile("Execution 2");
	}
	
	
	public static void main(String[] args) throws Exception {
		(new PerformanceTestHbaseIncrementalSnapshotSumUpdates()).execute();
	}
}
