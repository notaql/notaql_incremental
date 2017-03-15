package notaql.performance_tests.hbase.sum_inserts;

import notaql.performance_tests.hbase.PerformanceTestHbaseIncrementalTrigger;

/**
 * Tests a simple SUM() with additionaly inserted data
 */
public class PerformanceTestHbaseIncrementalTriggerSumInserts extends PerformanceTestHbaseIncrementalTrigger {
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
		notaqlEvaluate(generateQuery(query));
		logCurrentTimestampToFile("Execution 1");
		

		notaqlEvaluateTrigger(generateQueryTrigger(query));
		logCurrentTimestampToFile("Trigger installed");
		
		
		// Prepare the 2. execution
		int newRows = (int) (DATA_SIZE*DATA_CHANGE_PERCENTAGE);
		addTestDataSalary(input, newRows, DATA_SIZE);
		logCurrentTimestampToFile("Execution 2");
	}
	
	
	public static void main(String[] args) throws Exception {
		(new PerformanceTestHbaseIncrementalTriggerSumInserts()).execute();
	}
}
