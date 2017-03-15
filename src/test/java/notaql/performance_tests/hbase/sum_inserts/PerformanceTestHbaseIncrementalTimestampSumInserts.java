package notaql.performance_tests.hbase.sum_inserts;

import notaql.performance_tests.hbase.PerformanceTestHbaseIncrementalTimestamp;

/**
 * Tests a simple SUM() with additionaly inserted data
 */
public class PerformanceTestHbaseIncrementalTimestampSumInserts extends PerformanceTestHbaseIncrementalTimestamp {
	/* (non-Javadoc)
	 * @see notaql.performance_tests.PerformanceTest#test()
	 */
	@Override
	public void test() throws Exception {
		// Prepare the 1. execution
		logCurrentTimestampToFile("Start Test");
		final String query = "OUT._r <- 'result', OUT.sum <- SUM(IN." + DATA_SALARY_COLUMN_NAME + ")";
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
		(new PerformanceTestHbaseIncrementalTimestampSumInserts()).execute();
	}
}
