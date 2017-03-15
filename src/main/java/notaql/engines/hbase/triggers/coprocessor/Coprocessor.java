package notaql.engines.hbase.triggers.coprocessor;


import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import notaql.engines.hbase.HBaseApi;


public class Coprocessor extends BaseRegionObserver {
	// Configuration
	public static final String ENVIRONMENT_KEY_TABLE_ID = "table_id";
	public static final String ENVIRONMENT_KEY_HTTP_SERVER_URL = "http_server_url";
	
	
	// Object variables
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private String tableId;
	private URL httpServerUrl;
	private HTable table;

	
    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#start(org.apache.hadoop.hbase.CoprocessorEnvironment)
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
    	this.tableId = e.getConfiguration().get(ENVIRONMENT_KEY_TABLE_ID);
    	this.httpServerUrl = new URL(e.getConfiguration().get(ENVIRONMENT_KEY_HTTP_SERVER_URL));
    	this.table = new HBaseApi("localhost").getOrCreateTable(this.tableId);
    }

    
    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#prePut(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Put, org.apache.hadoop.hbase.regionserver.wal.WALEdit, org.apache.hadoop.hbase.client.Durability)
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
    	Result dataOld = this.table.get(new Get(put.getRow()));
    	asyncServerRequestTrigger(dataOld, put);
    }

    
    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.coprocessor.BaseRegionObserver#preDelete(org.apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.client.Delete, org.apache.hadoop.hbase.regionserver.wal.WALEdit, org.apache.hadoop.hbase.client.Durability)
     */
    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
    	Result dataOld = this.table.get(new Get(delete.getRow()));
    	asyncServerRequestTrigger(dataOld, null);
    }
    
    
    /**
     * Starts an async request to the server. The SingleThreadExecutor and its queue will make sure that the correct
     * order of the operations won't be messed up. This means that there is no need for locks (so PUT and DELETEs are
     * handled with nearly the same speed as if there was no coprocessor) but the order of the operations is still correct.
     * 
     * The main disadvantage over sync-requests is the change to eventual consistency for the NotaQL-view. If the view shall
     * be updated *before* the put/delete finishes async requests can't be used.
     * 
     * @param putNew
     * @param dataOld
     * @throws IOException
     */
    private void asyncServerRequestTrigger(Result dataOld, Put putNew) throws IOException {
    	executor.submit(() -> {
    		try {
		    	// Build the json
		    	StringBuilder stringBuilder = new StringBuilder();
		    	stringBuilder.append("{\"request_type\":\"trigger_hbase\",\"request\":{\"table_id\":\"");
		    	stringBuilder.append(this.tableId);
		    	stringBuilder.append("\"");
		    	
		    	if (putNew != null || dataOld != null)
		        	stringBuilder.append(",");
		    	    	
		    	if (putNew != null) {
		    		stringBuilder.append("\"data_new\":\"");
		    		stringBuilder.append(serializeHbase(dataOld, putNew).toSerializedString());
		    		stringBuilder.append("\"");
		    		
		    		if (dataOld != null)
		        		stringBuilder.append(",");
		    	}
		    	
		    	if (dataOld != null) {
		    		stringBuilder.append("\"data_old\":\"");
		    		stringBuilder.append(serializeHbase(dataOld).toSerializedString());
		    		stringBuilder.append("\"");
		    	}    		
		    	
		    	stringBuilder.append("}}");
		    	
		    	
		    	// Start the server request
		    	try {
		    		Util.doHttpPost(httpServerUrl, "json=" + URLEncoder.encode(stringBuilder.toString(), "UTF-8"));
				} catch (Exception e) {
					e.printStackTrace();
				}
    		} catch (Exception e) {
    			// Avoid a crash of the whole region server in case of exceptions
    			e.printStackTrace();
    		}
				
    	});
    }
    
    
    /**
     * Serializes the HBase-Result to json
     * 
     * @param dataOld
     * @return json
     * @throws IOException 
     */
    public static SerializableResult serializeHbase(Result dataOld) throws IOException {
    	return new SerializableResult(dataOld);
    }
    
    
    /**
     * Serializes the HBase-Put to json. 
     * 
     * @param dataOld the data which was previously stored in the table for the given row-id (because the non-changed data is not contained in the Put)
     * @param putNew the new Put-data
     * @return json
     * @throws IOException 
     */
    public static SerializableResult serializeHbase(Result dataOld, Put putNew) throws IOException {
    	// Convert the Put into Cells
    	List<Cell> cellsNew = new ArrayList<Cell>(putNew.size());
        CellScanner cellScanner = putNew.cellScanner();
        
        while (cellScanner.advance())
            cellsNew.add(cellScanner.current());
        

        // Merge with the previous data
        if (dataOld != null && !dataOld.isEmpty())
        	cellsNew.addAll(dataOld.listCells());

    	return new SerializableResult(cellsNew);
    }
}