package notaql.performance_tests.mongodb.copy_updates;

import notaql.performance_tests.mongodb.PerformanceTestMongodbIncrementalSnapshot;

/**
 * Copys values (with additionaly changed data)
 */
public class PerformanceTestMongodbIncrementalSnapshotCopyUpdates extends PerformanceTestMongodbIncrementalSnapshot {
	/* (non-Javadoc)
	 * @see notaql.performance_tests.PerformanceTest#test()
	 */
	@Override
	public void test() throws Exception {
		// Prepare the 1. execution
		logCurrentTimestampToFile("Start Test");
		final String query = "OUT._id <- IN._id, OUT.$(IN.*.name()) <- IN.@";
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
		(new PerformanceTestMongodbIncrementalSnapshotCopyUpdates()).execute();
	}
}
