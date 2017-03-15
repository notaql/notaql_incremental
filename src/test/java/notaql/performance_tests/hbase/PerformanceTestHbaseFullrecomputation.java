package notaql.performance_tests.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;

import notaql.engines.hbase.HBaseApi;
import notaql.incremental_tests.Loglevel;
import notaql.performance_tests.PerformanceTest;

/**
 * Methods and variables to be used for HBase-tests.
 * 
 * The database will be created in the first run and dropped in the last run. Because of this the statistics of these two runs
 * should be discarded.
 */
public abstract class PerformanceTestHbaseFullrecomputation extends PerformanceTest {
	// Configuration
	protected static final String TABLE_IN_ID = TEST_PREFIX + "input";
	private static final String TABLE_OUT_ID = TEST_PREFIX + "output";
	
	
	// Class variables
	protected static HBaseApi hbaseApi;
	protected static HTable input; 
	protected static HTable output;
	
	
	/**
	 * Sets up the needed tables for the OUTPUT of the transformation.
	 * 
	 * After each test these tables will be truncated. This should be faster compared
	 * to removing and re-creating the tables each time.
	 * 
	 * 
	 * Also sets up the HBase connection for the HBaseAPI.
	 */
	@Override
	protected void beforeClass() throws Exception {
		super.beforeClass();
		
		hbaseApi = new HBaseApi();
		input = createInputTable();
		output = getTable(TABLE_OUT_ID);
	}
	
	
	/**
	 * May be overwritten (e.g. for supporting timestamps).
	 * 
	 * @return the input table
	 * @throws IOException
	 */
	protected HTable createInputTable() throws IOException {
		return getTable(TABLE_IN_ID);
	}
	
	
	/**
	 * Drops the tables which were created by beforeClass().
	 * 
	 * Also closes the HBase connections.
	 */
	@Override
	protected void afterClass() throws Exception {
		super.afterClass();

		drop(output);

		if (input != null)
			drop(input);
		
		hbaseApi.close();
	}
	
	
	/**
	 * Truncates the tables.
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		
		if (input == null)
			throw new IllegalStateException("tableInput was not initialized");
		
		truncate(input);
		truncate(output);
	}
	
	
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody 
	 * @return
	 */
	protected static String generateQuery(String queryBody) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("IN-ENGINE: hbase(table_id <- '" + input.getName() + "'),");
		sb.append("OUT-ENGINE: hbase(table_id <- '" + output.getName() + "'),");
		sb.append(queryBody);
		
		return sb.toString();
	}
	
	
	/**
	 * Creates a new table with the given name if it doesn't exist and also adds the (NotaQl-)default column family.  
	 * 
	 * @param tableId Name of the (possibly new) table
	 * @return the table
	 * @throws IOException
	 */
	protected HTable getTable(String tableId) throws IOException {
		return getTable(tableId, null);
	}
	
	
	/**
	 * Creates a new table with the given name if it doesn't exist and also adds the (NotaQl-)default column family.  
	 * 
	 * @param tableId Name of the (possibly new) table
	 * @param numberOfmaxVersions
	 * @return the table
	 */
	protected HTable getTable(String tableId, Integer numberOfmaxVersions) throws IOException {
		log("getTable('" + tableId + "', '" + numberOfmaxVersions + "')", Loglevel.DEBUG);
		
		return hbaseApi.getOrCreateTable(tableId, numberOfmaxVersions);
	}
	
	
	/**
	 * Drops the table with the given name.
	 * 
	 * @param table_id
	 * @throws IOException
	 */
	protected void drop(HTable table) throws IOException {
		log("drop('" + table.getName() + "')", Loglevel.DEBUG);
		
		hbaseApi.dropTable(table);
	}
	
	
	/**
	 * Truncates the given table.
	 * 
	 * @param table
	 * @throws IOException
	 */
	public void truncate(HTable table) throws IOException {
		log("truncate('" + table.getName() + "')", Loglevel.DEBUG);
		
		hbaseApi.truncate(table.getName());
	}
	
	
	/**
	 * Puts a value to the given table.
	 * 
	 * @param table
	 * @param rowId
	 * @param column
	 * @param value
	 * @throws IOException
	 */
	protected void put(HTable table, String rowId, String column, Object value) throws IOException {
		// To many puts... commented out
		// log("put('" + table.getName() + "', '" + rowId + "', '" + column + "', '" + value + "')", Loglevel.DEBUG);
		
		hbaseApi.put(table, rowId, column, value, true);
	}
	
	
	/**
	 * Puts a value to the given table.
	 * 
	 * @param table
	 * @param rowId
	 * @param column
	 * @param value
	 * @param autoflush
	 * @throws IOException
	 */
	protected void put(HTable table, String rowId, String column, Object value, boolean autoflush) throws IOException {
		// To many puts... commented out
		// log("put('" + table.getName() + "', '" + rowId + "', '" + column + "', '" + value + "')", Loglevel.DEBUG);
		
		hbaseApi.put(table, rowId, column, value, autoflush);
	}
	
	/**
	 * Puts a value to the given table.
	 * 
	 * @param table
	 * @throws IOException
	 */
	protected void flush(HTable table) throws IOException {
		hbaseApi.flush(table);
	}
	
	
	/**
	 * Inserts new testdata into the database.
	 * 
	 * The testdata contains salary informations.
	 * 
	 * @param databaseId
	 * @param quantity
	 */
	protected final String DATA_SALARY_COLUMN_NAME = "salary";
	protected void addTestDataSalary(HTable table, int quantity, int offset) throws IOException {
		// Check input
		if (quantity == 0)
			return;
		
		
		// Insert new data
		for (int i=offset; i<quantity+offset; i++)
			put(table, String.valueOf(i), DATA_SALARY_COLUMN_NAME, (i%2)+1, false);
		flush(table);
	}
	
	
	/**
	 * Changes testdata in the database.
	 * 
	 * The testdata contains salary informations.
	 * 
	 * @param databaseId
	 * @param changePercentage > 0 (nothing changed) && < 1 (everything is new)
	 * @throws IOException 
	 */
	protected void changeTestDataSalary(HTable table, double changePercentage) throws IOException {
		// Check input
		if (changePercentage == 0)
			return;
		else if (changePercentage > 1 || changePercentage < 0)
			throw new IllegalArgumentException("changePercentage out of range");
			
		
		// Change the previous data
		int changeDataBound = (int) (changePercentage * DATA_SIZE);
		
		if (changeDataBound == 0)
			log("No data changed (DATA_SIZE = " + DATA_SIZE + ", changePercentage = " + changePercentage + ")", Loglevel.WARN);
		else
			log("Changing " + changeDataBound + " values", Loglevel.INFO);

		for (int i=0; i<changeDataBound; i++) {
			put(table, String.valueOf(i), DATA_SALARY_COLUMN_NAME, (i%2)+2, false);
		}
		flush(table);
	}
}
