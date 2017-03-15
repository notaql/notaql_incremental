package notaql.incremental_tests.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import notaql.engines.hbase.HBaseApi;
import notaql.incremental_tests.Loglevel;
import notaql.incremental_tests.Test;

/**
 * Methods and variables to be used for HBase-tests.
 */
public abstract class TestHbase extends Test {
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
	@BeforeClass
	public static void beforeClass() throws Exception {
		Test.beforeClass();
		
		hbaseApi = new HBaseApi();
		output = getTable(TABLE_OUT_ID);
	}
	
	
	/**
	 * Drops the tables which were created by beforeClass().
	 * 
	 * Also closes the HBase connections.
	 */
	@AfterClass
	public static void afterClass() throws Exception {
		Test.afterClass();

		drop(output);

		if (input != null)
			drop(input);
		
		hbaseApi.close();
	}
	
	
	/**
	 * Truncates the tables.
	 */
	@After
	public void tearDown() throws Exception {
		log("tearDown()", Loglevel.DEBUG);
		
		if (input == null)
			throw new IllegalStateException("tableInput was not initialized");
		
		truncate(input);
		truncate(output);
	}
	
	
	/**
	 * Creates a new table with the given name if it doesn't exist and also adds the (NotaQl-)default column family.  
	 * 
	 * @param tableId Name of the (possibly new) table
	 * @return the table
	 * @throws IOException
	 */
	protected static HTable getTable(String tableId) throws IOException {
		return getTable(tableId, null);
	}
	
	
	/**
	 * Creates a new table with the given name if it doesn't exist and also adds the (NotaQl-)default column family.  
	 * 
	 * @param tableId Name of the (possibly new) table
	 * @param numberOfmaxVersions
	 * @return the table
	 */
	protected static HTable getTable(String tableId, Integer numberOfmaxVersions) throws IOException {
		logStatic(TestHbase.class, "getTable('" + tableId + "', '" + numberOfmaxVersions + "')", Loglevel.DEBUG);
		
		return hbaseApi.getOrCreateTable(tableId, numberOfmaxVersions);
	}
	
	
	/**
	 * Drops the table with the given name.
	 * 
	 * @param table_id
	 * @throws IOException
	 */
	protected static void drop(HTable table) throws IOException {
		logStatic(TestHbase.class, "drop('" + table.getName() + "')", Loglevel.DEBUG);
		
		hbaseApi.dropTable(table);
	}
	
	
	/**
	 * Deletes a row from the given table.
	 * 
	 * @param table
	 * @param rowId
	 * @throws IOException
	 */
	protected void delete(HTable table, String rowId) throws IOException {
		log("delete('" + table.getName() + "', '" + rowId + "')", Loglevel.DEBUG);
		
		hbaseApi.delete(table, rowId);
	}
	
	
	/**
	 * Gets data from the given table by row-id, column family and column name.
	 * 
	 * @param table
	 * @param rowId
	 * @param columnFamily
	 * @param column
	 * @return the data as String
	 * @throws IOException
	 */
	protected String getString(HTable table, String rowId, String column) throws IOException {
		log("getString(" + table.getName() + "', '" + rowId + "', '" + column + "')", Loglevel.DEBUG);
		
		return hbaseApi.get(table, rowId, column);
	}
	
	
	/**
	 * Gets data from the given table by row-id, column family and column name.
	 * 
	 * @param table
	 * @param rowId
	 * @param column
	 * @return the data as int
	 * @throws IOException
	 */
	protected int getInteger(HTable table, String rowId, String column) throws IOException {
		log("getInteger('" + table.getName() + "', '" + rowId + "', '" + column + "')", Loglevel.DEBUG);

		return hbaseApi.getNumber(table, rowId, column).intValue();
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
		log("put('" + table.getName() + "', '" + rowId + "', '" + column + "', '" + value + "')", Loglevel.DEBUG);
		
		hbaseApi.put(table, rowId, column, value);
	}
}
