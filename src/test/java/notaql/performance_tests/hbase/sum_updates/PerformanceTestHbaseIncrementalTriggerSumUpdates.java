package notaql.performance_tests.hbase.sum_updates;

import notaql.performance_tests.hbase.PerformanceTestHbaseIncrementalTrigger;

/**
 * Tests a simple SUM() with additionaly changed data
 */
public class PerformanceTestHbaseIncrementalTriggerSumUpdates extends PerformanceTestHbaseIncrementalTrigger {
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
		notaqlEvaluate(generateQuery(query));
		logCurrentTimestampToFile("Execution 1");
		

		notaqlEvaluateTrigger(generateQueryTrigger(query));
		logCurrentTimestampToFile("Trigger installed");
		
		
		// Prepare the 2. execution
		changeTestDataSalary(input, DATA_CHANGE_PERCENTAGE);
		logCurrentTimestampToFile("Execution 2");
	}
	
	
	public static void main(String[] args) throws Exception {
		(new PerformanceTestHbaseIncrementalTriggerSumUpdates()).execute();
	}
}
