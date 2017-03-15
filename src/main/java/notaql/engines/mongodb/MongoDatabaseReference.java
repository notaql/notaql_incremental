package notaql.engines.mongodb;

import java.io.IOException;

import com.mongodb.DBCollection;

import notaql.engines.DatabaseReference;

public class MongoDatabaseReference extends DatabaseReference {
	// Configuration
	private static final String TEXT_PREFIX_SNAPSHOTS = "snapshot_";
	
	
	// Object variables
	public final String database;
	public final String collection;
	
	
	public MongoDatabaseReference(String host, int port, String database, String collection) {
		super(host, port);
		this.database = database;
		this.collection = collection;
	}
	
	
	/* (non-Javadoc)
	 * @see notaql.engines.DatabaseReference#drop()
	 */
	@Override
	public void drop() throws IOException {
		MongoApi mongoApi = new MongoApi(host, port);
		try {
			DBCollection mongoSnapshotCollection = mongoApi.getCollection(database, collection);
			mongoApi.truncate(mongoSnapshotCollection);
		} finally {
			mongoApi.close();
		}
	}

	
	/* (non-Javadoc)
	 * @see notaql.engines.DatabaseReference#createSnapshot(notaql.engines.DatabaseReference, java.lang.String)
	 */
	@Override
	public DatabaseReference createSnapshot(DatabaseReference original, String suffix) {
		if (!(original instanceof MongoDatabaseReference))
			throw new IllegalArgumentException("Input is no mongo-reference");
		
		MongoDatabaseReference mongoOriginal = (MongoDatabaseReference) original;
		
		String databaseName = mongoOriginal.database;
		String collectionName = mongoOriginal.collection;
		String snapshotCollectionName = TEXT_PREFIX_SNAPSHOTS + collectionName + "_" + suffix;
		
		// Create the snapshot
		String host = mongoOriginal.host;
		int port = mongoOriginal.port;
		MongoApi mongoApi = new MongoApi(host, port);
		try {
			DBCollection collection = mongoApi.getCollection(databaseName, collectionName);
			mongoApi.copyCollection(collection, snapshotCollectionName);
			
			return new MongoDatabaseReference(host, port, databaseName, snapshotCollectionName);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				mongoApi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
}