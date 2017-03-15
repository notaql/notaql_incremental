package notaql.incremental_tests.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import notaql.datamodel.ValueUtils;
import notaql.engines.Engine;
import notaql.engines.hbase.exceptions.TableHasNoVersionsException;
import notaql.incremental_tests.Loglevel;

/**
 * Collection of tests for testing the incremental capabilities of NotaQl with HBase using timestamps.
 * 
 * IMPORTANT: The pc which executes the test and the HBase server have to have synchronized clocks (e.g. use "sudo ntpd -gq").
 */
public class TestHbaseIncrementalTimestamp extends TestHbase {
	// Configuration
	private static final int NUMBER_OF_MAX_VERSIONS = 2;
	
	
	// Class variables
	private static long timestampPreviousExecution = System.currentTimeMillis();
	
	
	/**
	 * Sets up the needed tables for the INPUT of the transformation.
	 * 
	 * After each test these tables will be truncated. This should be faster compared
	 * to removing and re-creating the tables each time.
	 */
	@BeforeClass
	public static void beforeClass() throws Exception {
		TestHbase.beforeClass();
		
		input = getTable(TABLE_IN_ID, NUMBER_OF_MAX_VERSIONS);
	}
	
	
	/**
	 * Truncates the tables.
	 * 
	 * Clears the timestamp of the first execution.
	 */
	@After
	public void tearDown() throws Exception {
		super.tearDown();
		
		timestampPreviousExecution = 0;
	}
	
	
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody
	 * @param isTimestampQuery
	 * @return
	 */
	private static String generateQuery(String queryBody, boolean isTimestampQuery) {
		return generateQuery(queryBody, isTimestampQuery, input);
	}
	
	
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody
	 * @param isTimestampQuery
	 * @param inputTable 
	 * @return
	 */
	private static String generateQuery(String queryBody, boolean isTimestampQuery, HTable inputTable) {
		return generateQuery(queryBody, isTimestampQuery, input, false);
	}
	
	
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody
	 * @param isTimestampQuery
	 * @param inputTable 
	 * @param insertsOnly
	 * @return
	 */
	private static String generateQuery(String queryBody, boolean isTimestampQuery, HTable inputTable, boolean insertsOnly) {
		StringBuilder sb = new StringBuilder();
		
		String timestampComponent = "";
		String insertsOnlyComponent = "";
		
		if (isTimestampQuery)
			timestampComponent = ", timestamp <- '" + timestampPreviousExecution + "'";
		
		if (insertsOnly)
			insertsOnlyComponent = ", " + Engine.PARAMETER_NAME_USER_EXPECTS_UPDATES + " <- 'false', " + Engine.PARAMETER_NAME_USER_EXPECTS_DELETES + " <- 'false'";
		
		sb.append("IN-ENGINE: hbase(table_id <- '" + inputTable.getName() + "'");
		sb.append(timestampComponent);
		sb.append(insertsOnlyComponent);
		sb.append("),");
		
		sb.append("OUT-ENGINE: hbase(table_id <- '" + output.getName() + "'),");
		sb.append(queryBody);
		

		timestampPreviousExecution = System.currentTimeMillis();		
		return sb.toString();
	}
	
	
	/* (non-Javadoc)
	 * @see notaql_extensions.tests.hbase.TestHbase#delete(org.apache.hadoop.hbase.client.HTable, java.lang.String)
	 */
	@Override
	protected void delete(HTable table, String rowId) throws IOException {
		log("delete('" + table.getName() + "', '" + rowId + "')", Loglevel.DEBUG);
		
		
		// Get the existing data
		Result result = hbaseApi.get(input, "A");
		
		
		// Overwrite all Numbers from the result
		for (Entry<byte[], NavigableMap<byte[], byte[]>> entryColumnFamily : result.getNoVersionMap().entrySet()) {
			for (Entry<byte[], byte[]> entryColumn : entryColumnFamily.getValue().entrySet()) {
				// Check if the value is a number
				try {
					// The following statement throws an exception if this is not a Number
					Number number = ValueUtils.stringToNumber(Bytes.toString(entryColumn.getValue()));
					
					if (number != null)
						hbaseApi.put(table, rowId, Bytes.toString(entryColumnFamily.getKey()), Bytes.toString(entryColumn.getKey()), 0);
				} catch (IllegalStateException e) {}
			}
		}
	}
	
	
	/**
	 * Tests the connection to HBase.
	 * 
	 * @throws Exception
	 */
	@Test
	public void test000() throws Exception {
		log("TEST000", Loglevel.DEBUG);
		

		// Prepare the table
		put(input, "A", "salary", 10000);
		
		
		// Test the table
		try {
			assertEquals(10000, getInteger(input, "A", "salary"));
		} catch (AssertionError e) {
			// If this test fails the other tests don't make any sense
			log("Connection-test failed", Loglevel.ERROR);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	
	/**
	 * Basic test for timestamp-based incremental computations.
	 * 
	 * - Adds values to the database
	 * - Executes the Script
	 * - Creates a snapshot
	 * - Adds new values
	 * - Re-executes the script and checks the result
	 * 
	 * @throws Exception
	 */
	@Test
	public void test001a() throws Exception {
		log("TEST001a", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "OUT._r <- 'result', OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "salary", 10000);
		put(input, "B", "salary", 20000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertion
		assertEquals(30000, getInteger(output, "result", "sum"));
		assertEquals(2, getInteger(output, "result", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 15000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertion
		assertEquals(35000, getInteger(output, "result", "sum"));
		assertEquals(2, getInteger(output, "result", "count"));
	}
	
	
	/**
	 * Basic test for timestamp-based incremental computations.
	 * 
	 * Tells the engine that there won't be any updates or deletes. The hbase engine evaluator has special
	 * optimizations then. 
	 * 
	 * @throws Exception
	 */
	@Test
	public void test001b() throws Exception {
		log("TEST001b", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "OUT._r <- 'result', OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "salary", 10000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false, input, true));
		
		
		// 1. Assertion
		assertEquals(10000, getInteger(output, "result", "sum"));
		assertEquals(1, getInteger(output, "result", "count"));
		
		
		// Prepare the 2. execution
		put(input, "B", "salary", 20000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true, input, true));
		
		
		// 2. Assertion
		assertEquals(30000, getInteger(output, "result", "sum"));
		assertEquals(2, getInteger(output, "result", "count"));
	}
	
	
	/**
	 * Tests deletes.
	 * 
	 * - Adds values to the database
	 * - Executes the Script
	 * - Creates a snapshot
	 * - Deletes previous values
	 * - Re-executes the script and checks the result
	 * 
	 * @throws Exception
	 */
	@Test
	public void test002() throws Exception {
		log("TEST002", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "OUT._r <- 'result', OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "salary", 10000);
		put(input, "B", "salary", 20000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertion
		assertEquals(30000, getInteger(output, "result", "sum"));
		assertEquals(2, getInteger(output, "result", "count"));
		
		
		// Prepare the 2. execution
		delete(input, "A");
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertion
		assertEquals(20000, getInteger(output, "result", "sum"));
		assertEquals(2, getInteger(output, "result", "count"));
	}
	
	
	/**
	 * An employee gets a raise of his bonus.
	 * 
	 * Possible sources of problems:
	 * - Only the bonus changes but not the salary of the employee. Still the old salary (flagged as preserved) has to be included in the delta-data and by this in the aggregation-function.
	 * - The employer of the employee doesn't change either. Grouping by this value (flagged as preserved) shall not fail. 
	 * 
	 * @throws Exception
	 */
	@Test
	public void test003() throws Exception {
		log("TEST003", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "OUT._r <- IN.employer, OUT.sum <- SUM(IN.salary+IN.bonus), OUT.count <- COUNT(IN._r)";
		put(input, "A", "employer", "Employer1");
		put(input, "A", "bonus", 500);
		put(input, "A", "salary", 10000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertion
		assertEquals(10500, getInteger(output, "Employer1", "sum"));
		assertEquals(1, getInteger(output, "Employer1", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "bonus", 1500);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertion
		assertEquals(11500, getInteger(output, "Employer1", "sum"));
		assertEquals(1, getInteger(output, "Employer1", "count"));
	}
	
	
	/**
	 * An employee gets hired by a different employer.
	 * 
	 * Possible sources of problems:
	 * - The old values (flagged as preserved) have to be aggregated to another row and have to be substracted from the previous row (=> preserved has to be changed into a insert/delete combination). 
	 * 
	 * @throws Exception
	 */
	@Test
	public void test004() throws Exception {
		log("TEST004", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "OUT._r <- IN.employer, OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "employer", "Employer1");
		put(input, "A", "salary", 10000);
		
		put(input, "B", "employer", "Employer1");
		put(input, "B", "salary", 15000);
		
		put(input, "C", "employer", "Employer2");
		put(input, "C", "salary", 20000);
		
		put(input, "D", "employer", "Employer2");
		put(input, "D", "salary", 25000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertions
		assertEquals(25000, getInteger(output, "Employer1", "sum"));
		assertEquals(2, getInteger(output, "Employer1", "count"));
		
		assertEquals(45000, getInteger(output, "Employer2", "sum"));
		assertEquals(2, getInteger(output, "Employer2", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "employer", "Employer2");
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertions
		assertEquals(15000, getInteger(output, "Employer1", "sum"));
		assertEquals(1, getInteger(output, "Employer1", "count"));
		
		assertEquals(55000, getInteger(output, "Employer2", "sum"));
		assertEquals(3, getInteger(output, "Employer2", "count"));
	}
	
	
	/**
	 * Combination of test003 and test004: An employee gets hired by a different employer and also gets a raise in his salary.
	 * 
	 * Possible sources of problems:
	 * - see test003 and test004
	 * - Additional: The old value has to be substracted, but the new value has to be added.
	 * 
	 * @throws Exception
	 */
	@Test
	public void test005() throws Exception {
		log("TEST005", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "OUT._r <- IN.employer, OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "employer", "Employer1");
		put(input, "A", "salary", 10000);
		
		put(input, "B", "employer", "Employer1");
		put(input, "B", "salary", 15000);
		
		put(input, "C", "employer", "Employer2");
		put(input, "C", "salary", 20000);
		
		put(input, "D", "employer", "Employer2");
		put(input, "D", "salary", 25000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertions
		assertEquals(25000, getInteger(output, "Employer1", "sum"));
		assertEquals(2, getInteger(output, "Employer1", "count"));
		
		assertEquals(45000, getInteger(output, "Employer2", "sum"));
		assertEquals(2, getInteger(output, "Employer2", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "employer", "Employer2");
		put(input, "A", "salary", 13000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertions
		assertEquals(15000, getInteger(output, "Employer1", "sum"));
		assertEquals(1, getInteger(output, "Employer1", "count"));
		
		assertEquals(58000, getInteger(output, "Employer2", "sum"));
		assertEquals(3, getInteger(output, "Employer2", "count"));
	}
	
	
	/**
	 * Tests the 1st case of the IN-FILTER:
	 * - 1st value: not filtered
	 * - 2nd value: not filtered
	 * 
	 * @throws Exception
	 */
	@Test
	public void test006() throws Exception {
		log("TEST006", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary > 5000, OUT._r <- 'result', OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "salary", 10000);
		put(input, "B", "salary", 20000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertion
		assertEquals(30000, getInteger(output, "result", "sum"));
		assertEquals(2, getInteger(output, "result", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 15000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertion
		assertEquals(35000, getInteger(output, "result", "sum"));
		assertEquals(2, getInteger(output, "result", "count"));
	}
	
	
	/**
	 * Tests the 2nd case of the IN-FILTER:
	 * - 1st value: filtered
	 * - 2nd value: not filtered
	 * 
	 * @throws Exception
	 */
	@Test
	public void test007() throws Exception {
		log("TEST007", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary > 11000, OUT._r <- 'result', OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "salary", 10000);
		put(input, "B", "salary", 20000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertion
		assertEquals(20000, getInteger(output, "result", "sum"));
		assertEquals(1, getInteger(output, "result", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 15000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertion
		assertEquals(35000, getInteger(output, "result", "sum"));
		assertEquals(2, getInteger(output, "result", "count"));
	}
	
	
	/**
	 * Tests the 3rd case of the IN-FILTER:
	 * - 1st value: not filtered
	 * - 2nd value: filtered
	 * 
	 * @throws Exception
	 */
	@Test
	public void test008() throws Exception {
		log("TEST008", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary < 25000, OUT._r <- 'result', OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "salary", 10000);
		put(input, "B", "salary", 20000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertion
		assertEquals(30000, getInteger(output, "result", "sum"));
		assertEquals(2, getInteger(output, "result", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 50000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertion
		assertEquals(20000, getInteger(output, "result", "sum"));
		assertEquals(1, getInteger(output, "result", "count"));
	}
	
	
	/**
	 * Tests the 4rd case of the IN-FILTER:
	 * - 1st value: filtered
	 * - 2nd value: filtered
	 * 
	 * @throws Exception
	 */
	@Test
	public void test009() throws Exception {
		log("TEST009", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary > 16000, OUT._r <- 'result', OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "salary", 10000);
		put(input, "B", "salary", 20000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertion
		assertEquals(20000, getInteger(output, "result", "sum"));
		assertEquals(1, getInteger(output, "result", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 15000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertion
		assertEquals(20000, getInteger(output, "result", "sum"));
		assertEquals(1, getInteger(output, "result", "count"));
	}
	
	
	/**
	 * Tests the 2nd case of the IN-FILTER:
	 * - 1st value: filtered
	 * - 2nd value: not filtered
	 * 
	 * Also there are values which were not changed in the second version (and by this have to be transformed from preserved into inserted).
	 * 
	 * @throws Exception
	 */
	@Test
	public void test010() throws Exception {
		log("TEST010", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary > 11000, OUT._r <- 'result', OUT.sum_salary <- SUM(IN.salary), OUT.sum_coworkers <- SUM(IN.coworkers), OUT.count <- COUNT(IN._r)";
		put(input, "A", "salary", 10000);
		put(input, "A", "coworkers", 1);
		
		put(input, "B", "salary", 20000);
		put(input, "B", "coworkers", 10);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertion
		assertEquals(20000, getInteger(output, "result", "sum_salary"));
		assertEquals(10, getInteger(output, "result", "sum_coworkers"));
		assertEquals(1, getInteger(output, "result", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 15000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertion
		assertEquals(35000, getInteger(output, "result", "sum_salary"));
		assertEquals(11, getInteger(output, "result", "sum_coworkers"));
		assertEquals(2, getInteger(output, "result", "count"));
	}
	
	
	/**
	 * Tests the 3rd case of the IN-FILTER:
	 * - 1st value: not filtered
	 * - 2nd value: filtered
	 * 
	 * Also there are values which were not changed in the second version (and by this have to be transformed from preserved into deleted).
	 * 
	 * @throws Exception
	 */
	@Test
	public void test011() throws Exception {
		log("TEST011", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary < 25000, OUT._r <- 'result', OUT.sum_salary <- SUM(IN.salary), OUT.sum_coworkers <- SUM(IN.coworkers), OUT.count <- COUNT(IN._r)";
		put(input, "A", "salary", 10000);
		put(input, "A", "coworkers", 1);
		
		put(input, "B", "salary", 20000);
		put(input, "B", "coworkers", 10);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertion
		assertEquals(30000, getInteger(output, "result", "sum_salary"));
		assertEquals(11, getInteger(output, "result", "sum_coworkers"));
		assertEquals(2, getInteger(output, "result", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 50000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertion
		assertEquals(20000, getInteger(output, "result", "sum_salary"));
		assertEquals(10, getInteger(output, "result", "sum_coworkers"));
		assertEquals(1, getInteger(output, "result", "count"));
	}
	
	
	/**
	 * An employee gets a raise in his salary.
	 * 
	 * Also tests the 2nd case of the IN-FILTER:
	 * - 1st value: filtered
	 * - 2nd value: not filtered
	 * 
	 * Possible sources of problems:
	 * - the value by which the rows where grouped gets a preserved flag
	 * 
	 * @throws Exception
	 */
	@Test
	public void test012() throws Exception {
		log("TEST012", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary > 11000, OUT._r <- IN.employer, OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "employer", "Employer1");
		put(input, "A", "salary", 10000);
		
		put(input, "B", "employer", "Employer1");
		put(input, "B", "salary", 15000);
		
		put(input, "C", "employer", "Employer2");
		put(input, "C", "salary", 20000);
		
		put(input, "D", "employer", "Employer2");
		put(input, "D", "salary", 25000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertions
		assertEquals(15000, getInteger(output, "Employer1", "sum"));
		assertEquals(1, getInteger(output, "Employer1", "count"));
		
		assertEquals(45000, getInteger(output, "Employer2", "sum"));
		assertEquals(2, getInteger(output, "Employer2", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 12000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertions
		assertEquals(27000, getInteger(output, "Employer1", "sum"));
		assertEquals(2, getInteger(output, "Employer1", "count"));
		
		assertEquals(45000, getInteger(output, "Employer2", "sum"));
		assertEquals(2, getInteger(output, "Employer2", "count"));
	}
	
	
	/**
	 * An employee gets a raise in his salary.
	 * 
	 * Also tests the 3rd case of the IN-FILTER:
	 * - 1st value: not filtered
	 * - 2nd value: filtered
	 * 
	 * Possible sources of problems:
	 * - the old value still has to be substracted
	 * 
	 * @throws Exception
	 */
	@Test
	public void test013() throws Exception {
		log("TEST013", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary < 30000, OUT._r <- IN.employer, OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "employer", "Employer1");
		put(input, "A", "salary", 10000);
		
		put(input, "B", "employer", "Employer1");
		put(input, "B", "salary", 15000);
		
		put(input, "C", "employer", "Employer2");
		put(input, "C", "salary", 20000);
		
		put(input, "D", "employer", "Employer2");
		put(input, "D", "salary", 25000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertions
		assertEquals(25000, getInteger(output, "Employer1", "sum"));
		assertEquals(2, getInteger(output, "Employer1", "count"));
		
		assertEquals(45000, getInteger(output, "Employer2", "sum"));
		assertEquals(2, getInteger(output, "Employer2", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 50000);
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertions
		assertEquals(15000, getInteger(output, "Employer1", "sum"));
		assertEquals(1, getInteger(output, "Employer1", "count"));
		
		assertEquals(45000, getInteger(output, "Employer2", "sum"));
		assertEquals(2, getInteger(output, "Employer2", "count"));
	}
	
	
	/**
	 * An employee gets a raise in his salary because he changed the employer.
	 * 
	 * Also tests the 2nd case of the IN-FILTER:
	 * - 1st value: filtered
	 * - 2nd value: not filtered
	 * 
	 * Possible sources of problems:
	 * - the value by which the rows where grouped gets a preserved flag
	 * 
	 * @throws Exception
	 */
	@Test
	public void test014() throws Exception {
		log("TEST014", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary > 11000, OUT._r <- IN.employer, OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "employer", "Employer1");
		put(input, "A", "salary", 10000);
		
		put(input, "B", "employer", "Employer1");
		put(input, "B", "salary", 15000);
		
		put(input, "C", "employer", "Employer2");
		put(input, "C", "salary", 20000);
		
		put(input, "D", "employer", "Employer2");
		put(input, "D", "salary", 25000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertions
		assertEquals(15000, getInteger(output, "Employer1", "sum"));
		assertEquals(1, getInteger(output, "Employer1", "count"));
		
		assertEquals(45000, getInteger(output, "Employer2", "sum"));
		assertEquals(2, getInteger(output, "Employer2", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 12000);
		put(input, "A", "employer", "Employer2");
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertions
		assertEquals(15000, getInteger(output, "Employer1", "sum"));
		assertEquals(1, getInteger(output, "Employer1", "count"));
		
		assertEquals(57000, getInteger(output, "Employer2", "sum"));
		assertEquals(3, getInteger(output, "Employer2", "count"));
	}
	
	
	/**
	 * An employee gets a raise in his salary because he changed the employer.
	 * 
	 * Also tests the 3rd case of the IN-FILTER:
	 * - 1st value: not filtered
	 * - 2nd value: filtered
	 * 
	 * Possible sources of problems:
	 * - the old value still has to be substracted
	 * 
	 * @throws Exception
	 */
	@Test
	public void test015() throws Exception {
		log("TEST015", Loglevel.DEBUG);
		

		// Prepare the 1. execution
		final String query = "IN-FILTER: IN.salary < 30000, OUT._r <- IN.employer, OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._r)";
		put(input, "A", "employer", "Employer1");
		put(input, "A", "salary", 10000);
		
		put(input, "B", "employer", "Employer1");
		put(input, "B", "salary", 15000);
		
		put(input, "C", "employer", "Employer2");
		put(input, "C", "salary", 20000);
		
		put(input, "D", "employer", "Employer2");
		put(input, "D", "salary", 25000);
		
				
		// 1. Execution
		notaqlEvaluate(generateQuery(query, false));
		
		
		// 1. Assertions
		assertEquals(25000, getInteger(output, "Employer1", "sum"));
		assertEquals(2, getInteger(output, "Employer1", "count"));
		
		assertEquals(45000, getInteger(output, "Employer2", "sum"));
		assertEquals(2, getInteger(output, "Employer2", "count"));
		
		
		// Prepare the 2. execution
		put(input, "A", "salary", 50000);
		put(input, "A", "employer", "Employer2");
		
		
		// 2. Execution
		notaqlEvaluate(generateQuery(query, true));
		
		
		// 2. Assertions
		assertEquals(15000, getInteger(output, "Employer1", "sum"));
		assertEquals(1, getInteger(output, "Employer1", "count"));
		
		assertEquals(45000, getInteger(output, "Employer2", "sum"));
		assertEquals(2, getInteger(output, "Employer2", "count"));
	}
	
	
	/**
	 * Tests horizontal aggregations.
	 * 
	 * @throws Exception
	 */
	@Test
	public void test016() throws Exception {
	   log("TEST016", Loglevel.DEBUG);
	   

	   // Prepare the 1. execution
	   final String query = "OUT._r <- IN._r, OUT.sum <- SUM(IN._v), OUT.count <- COUNT(IN._v)";
	   put(input, "A", "v1", 10000);
	   
	         
	   // 1. Execution
	   notaqlEvaluate(generateQuery(query, false));
	   
	   
	   // 1. Assertion
	   assertEquals(10000, getInteger(output, "A", "sum"));
	   assertEquals(1, getInteger(output, "A", "count"));
	   
	   
	   // Prepare the 2. execution
	   put(input, "A", "v2", 15000);
	   
	   
	   // 2. Execution
	   notaqlEvaluate(generateQuery(query, true));
	   
	   
	   // 2. Assertion
	   assertEquals(25000, getInteger(output, "A", "sum"));
	   assertEquals(2, getInteger(output, "A", "count"));
	}
	
	
	/**
	 * Tests inserting values without aggregations
	 * 
	 * @throws Exception
	 */
	@Test
	public void test017() throws Exception {
	   log("TEST017", Loglevel.DEBUG);
	   

	   // Prepare the 1. execution
	   final String query = "OUT._r <- IN._r, OUT.$(IN._c) <- IN._v";
	   put(input, "A", "salary", 10000);
	   
	         
	   // 1. Execution
	   notaqlEvaluate(generateQuery(query, false));
	   
	   
	   // 1. Assertion
	   assertEquals(10000, getInteger(output, "A", "salary"));
	   
	   
	   // Prepare the 2. execution
	   put(input, "A", "coworkers", 1);
	   put(input, "B", "salary", 20000);
	   
	   
	   // 2. Execution
	   notaqlEvaluate(generateQuery(query, true));
	   
	   
	   // 2. Assertion
	   assertEquals(10000, getInteger(output, "A", "salary"));
	   assertEquals(1, getInteger(output, "A", "coworkers"));
	   assertEquals(20000, getInteger(output, "B", "salary"));
	}
	
	
	/**
	 * Tests updating values without aggregations
	 * 
	 * @throws Exception
	 */
	@Test
	public void test018() throws Exception {
	   log("TEST018", Loglevel.DEBUG);
	   

	   // Prepare the 1. execution
	   final String query = "OUT._r <- IN._r, OUT.$(IN._c) <- IN._v";
	   put(input, "A", "salary", 10000);
	   
	         
	   // 1. Execution
	   notaqlEvaluate(generateQuery(query, false));
	   
	   
	   // 1. Assertion
	   assertEquals(10000, getInteger(output, "A", "salary"));
	   
	   
	   // Prepare the 2. execution
	   put(input, "A", "salary", 15000);
	   
	   
	   // 2. Execution
	   notaqlEvaluate(generateQuery(query, true));
	   
	   
	   // 2. Assertion
	   assertEquals(15000, getInteger(output, "A", "salary"));
	}

	
	/**
	 * Transposes a table with additional inserts
	 * 
	 * @throws Exception
	 */
	@Test
	public void test019() throws Exception {
	   log("TEST019", Loglevel.DEBUG);
	   

	   // Prepare the 1. execution
	   final String query = "OUT._r <- IN._c, OUT.$(IN._r) <- IN._v";
	   put(input, "A", "salary", 10000);
	   
	         
	   // 1. Execution
	   notaqlEvaluate(generateQuery(query, false));
	   
	   
	   // 1. Assertion
	   assertEquals(10000, getInteger(output, "salary", "A"));
	   
	   
	   // Prepare the 2. execution
	   put(input, "A", "coworkers", 1);
	   put(input, "B", "salary", 20000);
	   
	   
	   // 2. Execution
	   notaqlEvaluate(generateQuery(query, true));
	   
	   
	   // 2. Assertion
	   assertEquals(10000, getInteger(output, "salary", "A"));
	   assertEquals(1, getInteger(output, "coworkers", "A"));
	   assertEquals(20000, getInteger(output, "salary", "B"));
	}
	
	
	/**
	 * Transposes a table with additional updates
	 * 
	 * @throws Exception
	 */
	@Test
	public void test020() throws Exception {
	   log("TEST020", Loglevel.DEBUG);
	   

	   // Prepare the 1. execution
	   final String query = "OUT._r <- IN._c, OUT.$(IN._r) <- IN._v";
	   put(input, "A", "salary", 10000);
	   
	         
	   // 1. Execution
	   notaqlEvaluate(generateQuery(query, false));
	   
	   
	   // 1. Assertion
	   assertEquals(10000, getInteger(output, "salary", "A"));
	   
	   
	   // Prepare the 2. execution
	   put(input, "A", "salary", 15000);
	   
	   
	   // 2. Execution
	   notaqlEvaluate(generateQuery(query, true));
	   
	   
	   // 2. Assertion
	   assertEquals(15000, getInteger(output, "salary", "A"));
	}
	
	
	/**
	 * Tests if the execution fails if the table does not have enough max versions.  
	 * 
	 * @throws Exception
	 */
	@Test
	public void test021() throws Exception {
		log("TEST021", Loglevel.DEBUG);
		
		
		// Create a table without versions
		HTable inputNoVersions = null;
		try {
			inputNoVersions = getTable(TEST_PREFIX + "inputNoVersions", null);
			
	
			// Prepare the 1. execution
			final String query = "OUT._id <- 'result', OUT.sum <- SUM(IN.salary), OUT.count <- COUNT(IN._id)";
			
					
			// 1. Execution
			try {
				notaqlEvaluate(generateQuery(query, true, inputNoVersions));
				fail("No " + TableHasNoVersionsException.class.getSimpleName() + " thrown");
			} catch (TableHasNoVersionsException e) {}
		} finally {
			if (inputNoVersions != null)
				drop(inputNoVersions);
		}
	}
}
