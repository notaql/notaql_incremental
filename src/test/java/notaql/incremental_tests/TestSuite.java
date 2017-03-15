package notaql.incremental_tests;

import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import notaql.incremental_tests.hbase.TestHbaseIncrementalSnapshot;
import notaql.incremental_tests.hbase.TestHbaseIncrementalTimestamp;
import notaql.incremental_tests.hbase.TestHbaseIncrementalTrigger;
import notaql.incremental_tests.mongodb.TestMongodbIncrementalSnapshot;
import notaql.incremental_tests.mongodb.TestMongodbIncrementalTimestamp;
import notaql.incremental_tests.redis.TestRedisIncrementalSnapshot;

/**
 * Collection of tests for testing the incremental capabilities of NotaQl.
 * 
 * IMPORTANT: Export the TestSuite as Runnable JAR, but chose "Copy required libraries into a sub-folder next to the generated JAR"; Spark has problems otherwise.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	TestHbaseIncrementalSnapshot.class,
	TestHbaseIncrementalTimestamp.class,
	TestHbaseIncrementalTrigger.class,
	TestMongodbIncrementalSnapshot.class,
	TestMongodbIncrementalTimestamp.class,
	TestRedisIncrementalSnapshot.class
})
public class TestSuite {
	public static void main(String[] args) throws Exception {                    
		JUnitCore.main(TestSuite.class.getName());
	}
}