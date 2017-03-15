package notaql.engines.redis;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import notaql.engines.DatabaseReference;
import notaql.extensions.advisor.util.LruHashMap;

public class RedisDatabaseReference extends DatabaseReference {
	// Class variables
	private static final int REDIS_RESERVED_SNAPSHOT_LOWER_INCLUDING = 10;
	private static final int REDIS_RESERVED_SNAPSHOT_UPPER_INCLUDING = 15; // At minimum lower_bound+1!
	private static LruHashMap<String, DatabaseReference> redisSnapshots = new LruHashMap<String, DatabaseReference>(REDIS_RESERVED_SNAPSHOT_UPPER_INCLUDING-REDIS_RESERVED_SNAPSHOT_LOWER_INCLUDING); // transformation -> snapshot
	
	
	// Object variables
	public final int databaseId;
	
	
	public RedisDatabaseReference(String host, int port, int databaseId) {
		super(host,port);
		this.databaseId = databaseId;
	}
	
	
	public static Map<String, DatabaseReference> getSnapshots() {
		return redisSnapshots;
	}
	
	
	/* (non-Javadoc)
	 * @see notaql.engines.DatabaseReference#drop()
	 */
	@Override
	public void drop() {
		RedisApi redisApi = new RedisApi(host, port);
		try {
			redisApi.dropDatabase(databaseId);
		} finally {
			redisApi.close();
		}
	}


	/* (non-Javadoc)
	 * @see notaql.engines.DatabaseReference#createSnapshot(notaql.engines.DatabaseReference, java.lang.String, java.lang.String)
	 */
	@Override
	public DatabaseReference createSnapshot(DatabaseReference original, String suffix) {
		if (!(original instanceof RedisDatabaseReference))
			throw new IllegalArgumentException("Input is no redis-reference");
		
		RedisDatabaseReference redisOriginal = (RedisDatabaseReference) original;
		

		// Find the first unused snapshot id
		Integer snapshotDatabaseId = null;
	
		if (!redisSnapshots.containsKey(suffix)) {
			redisSnapshots.removeEldestEntry();
			
			Set<Integer> idIsUsed = new HashSet<Integer>();
			for (DatabaseReference snapshot : redisSnapshots.values())
				idIsUsed.add(((RedisDatabaseReference) snapshot).databaseId);
	
			for (int i = REDIS_RESERVED_SNAPSHOT_LOWER_INCLUDING; i<= REDIS_RESERVED_SNAPSHOT_UPPER_INCLUDING; i++) {
				if (!idIsUsed.contains(i)) {
					snapshotDatabaseId = i;
					break;
				}
			}
		}
		else
			snapshotDatabaseId = ((RedisDatabaseReference) redisSnapshots.get(suffix)).databaseId;
		
		if (snapshotDatabaseId == null)
			throw new IllegalStateException("No unused database-id for the snapshot! Is upperbound > lowerbound ?");
		
		
		// Create the snapshot
		int databaseId = redisOriginal.databaseId;
		String host = RedisEngineEvaluator.getHost();
		int port = RedisEngineEvaluator.getPort();
		RedisApi redisApi = new RedisApi(host, port);
		try {
			redisApi.dropDatabase(snapshotDatabaseId);
			redisApi.copyDatabase(databaseId, snapshotDatabaseId);
		
			return new RedisDatabaseReference(host, port, snapshotDatabaseId);
		} finally {
			redisApi.close();
		}
	}
	
	
}
