package notaql.engines.hbase;

import java.io.IOException;

import notaql.engines.DatabaseReference;

public class HBaseDatabaseReference extends DatabaseReference  {
	// Configuration
	private static final String TEXT_PREFIX_SNAPSHOTS = "snapshot_";
	
	
	// Object variables
	public final String tableId;
	
	
	public HBaseDatabaseReference(String host, int port, String tableId) {
		super(host, port);
		this.tableId = tableId;
	}
	
	
	/* (non-Javadoc)
	 * @see notaql.engines.DatabaseReference#drop()
	 */
	@Override
	public void drop() throws IOException {
		HBaseApi hbaseApi = new HBaseApi(host, port);
		try {
			hbaseApi.dropTable(tableId);
		} finally {
			hbaseApi.close();
		}
	}
	

	/* (non-Javadoc)
	 * @see notaql.engines.DatabaseReference#createSnapshot(notaql.engines.DatabaseReference)
	 */
	@Override
	public DatabaseReference createSnapshot(DatabaseReference original, String suffix) throws IOException, InterruptedException {
		if (!(original instanceof HBaseDatabaseReference))
			throw new IllegalArgumentException("Input is no hbase-reference");
		
		HBaseDatabaseReference hbaseOriginal = (HBaseDatabaseReference) original;
			
		
		// Parameters
		String tableId = hbaseOriginal.tableId;
		String snapshotTableId = TEXT_PREFIX_SNAPSHOTS + tableId + "_" + suffix;
		
		
		// Create the snapshot
		String host = hbaseOriginal.host;
		int port = hbaseOriginal.port;
		HBaseApi hbaseApi = new HBaseApi(host ,port);
		hbaseApi.dropTable(snapshotTableId);
		hbaseApi.copyTable(tableId, snapshotTableId, snapshotTableId + "_snapshot");
		
		return new HBaseDatabaseReference(host, port, snapshotTableId);
	}
}