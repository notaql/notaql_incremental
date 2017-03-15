package notaql.engines.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import notaql.datamodel.ValueUtils;
import notaql.engines.hbase.triggers.coprocessor.Coprocessor;

/**
 * Collection of frequently used methods for HBase.
 */
public class HBaseApi {
	// Configuration
	public static final String DEFAULT_COLUMN_FAMILY = "default";
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 60000;
	
	
	// Object variables
	private final String host;
	private final int port;
	private HBaseAdmin hBaseAdmin;
	
	
	/**
	 * Initializes the API with the default host and default port.
	 */
	public HBaseApi() {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}
	
	
	/**
	 * Initializes the API with the given host and the default port.
	 * 
	 * @param host
	 */
	public HBaseApi(String host) {
		this(host, DEFAULT_PORT);
	}
	
	
	/**
	 * Initializes the API with the default host and the given port.
	 * 
	 * @param port
	 */
	public HBaseApi(int port) {
		this(DEFAULT_HOST, port);
	}
	
	
	/**
	 * Initializes the API with the given host and the given port.
	 * 
	 * @param host
	 * @param port
	 */
	public HBaseApi(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	
	/**
	 * @return the hostname
	 */
	public String getHost() {
		return this.host;
	}
	
	
	/**
	 * @return the port
	 */
	public int getPort() {
		return this.port;
	}
	
	
	/**
	 * Gets an HBaseAdmin instance.
	 * 
	 * @return
	 * @throws IOException
	 */
	public HBaseAdmin getAdmin() throws IOException {
		if (hBaseAdmin == null)
			hBaseAdmin = new HBaseAdmin(getConfiguration());
		
		return hBaseAdmin;
	}
	
	
	/**
	 * Closes the HBaseAdmin instance (if any).
	 * @throws IOException 
	 */
	public void close() throws IOException {
		if (hBaseAdmin != null)
			hBaseAdmin.close();
	}
	
	
	/**
	 * Creates a new table with the given name if it doesn't exist and also adds the (NotaQl-)default column family.
	 * 
	 * @param hbaseAdmin
	 * @param tableId
	 * @return
	 * @throws IOException
	 */
	public HTable getOrCreateTable(String tableId) throws IOException {
		return getOrCreateTable(tableId, null);
	}
	
	
	/**
	 * Creates a new table with the given name if it doesn't exist and also adds the (NotaQl-)default column family with the specified number of versions.  
	 * 
	 * @param hbaseAdmin
	 * @param tableId
	 * @param numberOfmaxVersions
	 * @return
	 * @throws IOException
	 */
	public HTable getOrCreateTable(String tableId, Integer numberOfmaxVersions) throws IOException {
		// Get an HBaseAdmin
		HBaseAdmin hbaseAdmin = getAdmin();
		
		
		// Create the table
        if (!hbaseAdmin.tableExists(tableId)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableId));
            
            HColumnDescriptor defaultColumnFamily = new HColumnDescriptor(DEFAULT_COLUMN_FAMILY);
            
            if (numberOfmaxVersions != null)
            	defaultColumnFamily.setMaxVersions(numberOfmaxVersions);
            		
            
            tableDescriptor.addFamily(defaultColumnFamily);
            
            hbaseAdmin.createTable(tableDescriptor);
            
            
            // The following loop avoids a race condition which shall be fixed in future releases of hbase-client
            int maxWaitTimeMillis = 10000;
            int currentWaitTimeMillis = 0;
            int waitTimePerCheckMillis = 100;
            while (!hbaseAdmin.isTableEnabled(tableId)) {
            	try {
					Thread.sleep(waitTimePerCheckMillis);
				} catch (InterruptedException e) {}
            	
            	currentWaitTimeMillis += waitTimePerCheckMillis;
            	
            	if (currentWaitTimeMillis >= maxWaitTimeMillis)
            		throw new IllegalStateException("Waiting for the new table '" + tableId + "' to be enabled timed out after " + maxWaitTimeMillis + " ms");
            }
        }
        
        
        // Return the table
        return new HTable(getConfiguration(), tableId);
	}
	
	
	/**
	 * Drops the table with the given name if it exists.
	 * 
	 * If the table doesn't exist this method returns silently.
	 * 
	 * @param hbaseAdmin
	 * @param table
	 * @throws IOException
	 */
	public void dropTable(String tableName) throws IOException {
		// Get an HBaseAdmin
		HBaseAdmin hbaseAdmin = getAdmin();
		
		
		// Drop the table if it exists.
		if (hbaseAdmin.tableExists(tableName)) {
			if (!hbaseAdmin.isTableDisabled(tableName))
				hbaseAdmin.disableTable(tableName);
			
			hbaseAdmin.deleteTable(tableName);
		}
	}
	
	
	/**
	 * Drops the table.
	 * 
	 * @param hbaseAdmin
	 * @param table
	 * @throws IOException
	 */
	public void dropTable(HTable table) throws IOException {
		dropTable(table.getName().toString());
	}

    
    /**
     * Creates the non existing column families from the list
     * 
     * @param table
     * @param columnFamilies
     */
	public void createColumnFamilies(HTable table, Collection<String> columnFamilies) throws IOException {
		// Get an HBaseAdmin
		HBaseAdmin hbaseAdmin = getAdmin();


		// Collect the non existing column families
		columnFamilies.removeAll(table.getTableDescriptor().getFamilies());

		
		// Add all remaining column families
		if (!columnFamilies.isEmpty()) {
			if (!hbaseAdmin.isTableDisabled(table.getName()))
				hbaseAdmin.disableTable(table.getTableName());

			for (String columnFamily : columnFamilies)
				hbaseAdmin.addColumn(table.getTableName(), new HColumnDescriptor(columnFamily));
			
			hbaseAdmin.enableTable(table.getTableName());
		}
	}

    
    /**
     * Checks if the column family exists in the given table and creates it if this is not the case.
     * 
     * @param table
     * @param columnFamily
     */
	public void createColumnFamily(HTable table, String columnFamily) throws IOException {
		createColumnFamilies(table, Arrays.asList(columnFamily));
	}
	
	
	/**
	 * Deletes a row from the given table.
	 * 
	 * @param table
	 * @param rowId
	 * @throws IOException
	 */
	public void delete(HTable table, String rowId) throws IOException {
		table.delete(new Delete(Bytes.toBytes(rowId)));
	}
	
	
	/**
	 * Gets data from the given table by row-id.
	 * 
	 * @param table
	 * @param rowId
	 * @return
	 * @throws IOException 
	 * @throws Exception
	 */
	public Result get(HTable table, String rowId) throws IOException {
		return table.get(new Get(Bytes.toBytes(rowId)));
	}
	
	
	/**
	 * Gets data from the given table by row-id, column family and column name.
	 * 
	 * @param table
	 * @param rowId
	 * @param column
	 * @return the data as String
	 * @throws Exception
	 */
	public String get(HTable table, String rowId, String column) throws IOException {
		return get(table, rowId, DEFAULT_COLUMN_FAMILY, column);
	}
	
	
	/**
	 * Gets data from the given table by row-id, column family and column name.
	 * 
	 * @param table
	 * @param rowId
	 * @param columnFamily
	 * @param column
	 * @return the data as String
	 * @throws Exception
	 */
	public String get(HTable table, String rowId, String columnFamily, String column) throws IOException {
		return Bytes.toString(get(table, rowId).getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)));
	}
	
	
	/**
	 * Gets data from the given table by row-id, column family and column name.
	 * 
	 * @param table
	 * @param rowId
	 * @param column
	 * @return the data
	 * @throws Exception
	 */
	public Number getNumber(HTable table, String rowId, String column) throws IOException {
		return getNumber(table, rowId, DEFAULT_COLUMN_FAMILY, column);
	}
	
	
	/**
	 * Gets data from the given table by row-id, column family and column name.
	 * 
	 * @param table
	 * @param rowId
	 * @param columnFamily
	 * @param column
	 * @return the data
	 * @throws IOException
	 */
	public Number getNumber(HTable table, String rowId, String columnFamily, String column) throws IOException {
		String string = get(table, rowId, columnFamily, column);

		return ValueUtils.stringToNumber(string);
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
	public void put(HTable table, String rowId, String column, Object value) throws IOException {
		put(table, rowId, DEFAULT_COLUMN_FAMILY, column, value);
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
	public void put(HTable table, String rowId, String column, Object value, boolean autoflush) throws IOException {
		put(table, rowId, DEFAULT_COLUMN_FAMILY, column, value, autoflush);
	}
	
	
	/**
	 * Puts a value to the given table.
	 * 
	 * @param table
	 * @param rowId
	 * @param columnFamily
	 * @param column
	 * @param value
	 * @throws IOException
	 */
	public void put(HTable table, String rowId, String columnFamily, String column, Object value) throws IOException {
		put(table, rowId, columnFamily, column, value, true);
	}
	
	
	/**
	 * Puts a value to the given table.
	 * 
	 * @param table
	 * @param rowId
	 * @param columnFamily
	 * @param column
	 * @param value
	 * @param autoflush
	 * @throws IOException
	 */
	public void put(HTable table, String rowId, String columnFamily, String column, Object value, boolean autoflush) throws IOException {
		Put put = new Put(Bytes.toBytes(rowId));
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(String.valueOf(value)));
		
		put(table, put, autoflush);
	}
	
	
	/**
	 * Puts a value to the given table.
	 * 
	 * @param table
	 * @param put
	 * @throws IOException
	 */
	public void put(HTable table, Put put) throws IOException {
		put(table, put, true);
	}
	
	
	/**
	 * Puts a value to the given table.
	 * 
	 * @param table
	 * @param put
	 * @param autoflush
	 * @throws IOException
	 */
	public void put(HTable table, Put put, boolean autoflush) throws IOException {
		if ((!autoflush && table.isAutoFlush()) // User wants no autocommit but it is enabled
				|| (autoflush && !table.isAutoFlush())) // User wants autocommit but it is disabled
			table.setAutoFlushTo(autoflush);
		
		table.put(put);
	}
	
	
	/**
	 * Flushes a db
	 * 
	 * @param table
	 * @throws IOException
	 */
	public void flush(HTable table) throws IOException {
		table.flushCommits();
	}
	
	
	/**
	 * Puts many values to the given table.
	 * 
	 * @param table
	 * @param put
	 * @throws IOException
	 */
	public void put(HTable table, List<Put> puts) throws IOException {
		table.put(puts);
		table.flushCommits();
	}
	
	
	/**
	 * Copies the existing oldTable using snapshots.
	 * 
	 * If the snapshot already exists an exception will be thrown.
	 * 
	 * @param oldTableId
	 * @param newTableId
	 * @param snapshotName
	 * @return the new table
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public HTable copyTable(String oldTableId, String newTableId, String snapshotName) throws IOException, InterruptedException {
		// Get an HBaseAdmin
		HBaseAdmin hbaseAdmin = getAdmin();

		
		// Copy the table
		hbaseAdmin.snapshot(snapshotName, oldTableId);
		hbaseAdmin.cloneSnapshot(snapshotName, newTableId);
		hbaseAdmin.deleteSnapshot(snapshotName);
		
		
		// Return the new table
		return getOrCreateTable(newTableId);
	}
	
	
	/**
	 * Truncates the given table.
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public void truncate(TableName tableName) throws IOException {
		// Get an HBaseAdmin
		HBaseAdmin hbaseAdmin = getAdmin();

		
		// Disable the table (if needed) and truncate it  
		if (!hbaseAdmin.isTableDisabled(tableName))
			hbaseAdmin.disableTable(tableName);
		
		hbaseAdmin.truncateTable(tableName, false);
	}
	
	
	/**
	 * Truncates the given table.
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public void truncate(String tableId) throws IOException {
		truncate(TableName.valueOf(tableId));
	}
	
	
	/**
	 * Attaches a coprocessor to the database (re-attaches if it is already present).
	 * 
	 * @param tableId
	 * @param coprocessorName name of the coprocessor including the package (ClassOfCoprocessor.class.getName()) inside the jar
	 * @param coprocessorTriggerJar path to the jar
	 * @param environment variables which can be used inside the coprocessor
	 * @throws IOException
	 */
	public void addCoprocessor(String tableId, String coprocessorName, Path coprocessorTriggerJar, Map<String, String> coprocessorEnvironment) throws IOException {
		// Get an HBaseAdmin
		HBaseAdmin hbaseAdmin = getAdmin();

		
		// Get the table descriptor
		HTable table = getOrCreateTable(tableId);
		HTableDescriptor tableDescriptor = hbaseAdmin.getTableDescriptor(table.getName());
        

        // Add the coprocessor
        if (hbaseAdmin.isTableEnabled(table.getName()))
        	hbaseAdmin.disableTable(table.getName());

        if (tableDescriptor.hasCoprocessor(coprocessorName))
            tableDescriptor.removeCoprocessor(coprocessorName);

        tableDescriptor.addCoprocessor(coprocessorName, coprocessorTriggerJar, Coprocessor.PRIORITY_USER, coprocessorEnvironment);
        
        
        // "Commit"
        hbaseAdmin.modifyTable(table.getName(), tableDescriptor);
        hbaseAdmin.enableTable(table.getName());
	}
	
	
	/**
	 * Removes a coprocessor from the database
	 * 
	 * @param tableId
	 * @param coprocessorName name of the coprocessor
	 * @throws IOException
	 */
	public void removeCoprocessor(String tableId, String coprocessorName) throws IOException {
		// Get an HBaseAdmin
		HBaseAdmin hbaseAdmin = getAdmin();

		
		// Get the table descriptor
		HTable table = getOrCreateTable(tableId);
        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        
        
        // Remove the coprocessor
        if (hbaseAdmin.isTableEnabled(table.getName()))
        	hbaseAdmin.disableTable(table.getName());

        if (tableDescriptor.hasCoprocessor(coprocessorName))
        	tableDescriptor.removeCoprocessor(coprocessorName);

        
        // "Commit"
        hbaseAdmin.modifyTable(table.getName(), tableDescriptor);
        hbaseAdmin.enableTable(table.getName());
	}
	
	
	/**
	 * Gets the max number of versions of a table.
	 * 
	 * @param table
	 * @throws IOException 
	 */
	public int getMaxNumberOfVersions(HTable table) throws IOException {
		return this.getMaxNumberOfVersions(table, DEFAULT_COLUMN_FAMILY);
	}
	
	
	/**
	 * Gets the max number of versions of a table.
	 * 
	 * @param table
	 * @param columnFamily
	 * @throws IOException 
	 */
	public int getMaxNumberOfVersions(HTable table, String columnFamily) throws IOException {
		return this.getMaxNumberOfVersions(table, Collections.singleton(columnFamily));
	}
	
	
	/**
	 * Gets the max number of versions of a table.
	 * 
	 * @param table
	 * @param columnFamilies
	 * @throws IOException 
	 */
	public int getMaxNumberOfVersions(HTable table, Collection<String> columnFamilies) throws IOException {
		if (columnFamilies.isEmpty())
			throw new IllegalArgumentException("No column-families passed");
		
		
		int minimumMaxVersions = Integer.MAX_VALUE;
		
		for (String columnFamily : columnFamilies) {
			HColumnDescriptor columnFamilyDescriptor = table.getTableDescriptor().getFamily(Bytes.toBytes(columnFamily));
			
			if (columnFamilyDescriptor == null)
				throw new IllegalArgumentException("Table has no column-family '" + columnFamily + "'");
			
			int columnFamilyMaxVersions = columnFamilyDescriptor.getMaxVersions();
			
			if (columnFamilyMaxVersions < minimumMaxVersions)
				minimumMaxVersions = columnFamilyMaxVersions;
		}
		
		
		return minimumMaxVersions;
	}
	
	
	/**
	 * Sets the max number of versions of a table.
	 * 
	 * @param table
	 * @param maxVersions
	 * @throws IOException 
	 */
	public void getMaxNumberOfVersions(HTable table, int maxVersions) throws IOException {
		for (HColumnDescriptor columnFamily : table.getTableDescriptor().getColumnFamilies())
			columnFamily.setMaxVersions(maxVersions);
	}
	
	
	/**
	 * Returns a configuration for this API.
	 * 
	 * Note: To avoid concurrency errors a new configuration will be created always.
	 * 
	 * @return configuration
	 */
	public Configuration getConfiguration() {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", this.host);
		configuration.set("hbase.master", this.host + ":" + this.port);
		
		return configuration;
	}
}
