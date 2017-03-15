package notaql.engines;

import java.io.IOException;

/**
 * A general reference to a database item. 
 */
public abstract class DatabaseReference {
	protected final String host;
	protected final int port;

	public DatabaseReference(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public abstract void drop() throws IOException;
	
	/**
	 * Creates a snapshot. The previous copy of the snapshot will be deleted if this is necessary.
	 * 
	 * @return the reference to the snapshot
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public abstract DatabaseReference createSnapshot(DatabaseReference original, String suffix) throws IOException, InterruptedException;
}
