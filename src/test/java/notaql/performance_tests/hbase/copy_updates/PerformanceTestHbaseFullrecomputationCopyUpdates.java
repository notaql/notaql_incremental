package notaql.performance_tests.hbase.copy_updates;

import notaql.performance_tests.hbase.PerformanceTestHbaseFullrecomputation;

/**
 * Copys values (with additionaly changed data)
 */
public class PerformanceTestHbaseFullrecomputationCopyUpdates extends PerformanceTestHbaseFullrecomputation {
	/* (non-Javadoc)
	 * @see notaql.performance_tests.PerformanceTest#test()
	 */
	@Override
	public void test() throws Exception {
		// Prepare the 1. execution
		logCurrentTimestampToFile("Start Test");
		final String query = "OUT._r <- IN._r, OUT.$(IN._c) <- IN._v";
		addTestDataSalary(input, DATA_SIZE, 0);
		logCurrentTimestampToFile("Data added");
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query));
		logCurrentTimestampToFile("Execution 1");
		
		
		// Prepare the 2. execution
		changeTestDataSalary(input, DATA_CHANGE_PERCENTAGE);
		logCurrentTimestampToFile("Data changed");
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query));
		logCurrentTimestampToFile("Execution 2");
	}
	
	
	public static void main(String[] args) throws Exception {
		(new PerformanceTestHbaseFullrecomputationCopyUpdates()).execute();
	}
}
