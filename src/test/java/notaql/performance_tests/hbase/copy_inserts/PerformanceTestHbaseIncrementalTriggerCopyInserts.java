package notaql.performance_tests.hbase.copy_inserts;

import notaql.performance_tests.hbase.PerformanceTestHbaseIncrementalTrigger;

/**
 * Copys values (with additionaly inserted data)
 */
public class PerformanceTestHbaseIncrementalTriggerCopyInserts extends PerformanceTestHbaseIncrementalTrigger {
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
		(new PerformanceTestHbaseIncrementalTriggerCopyInserts()).execute();
	}
}
