package notaql.engines.redis;

import notaql.datamodel.ValueUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

/**
 * Collection of frequently used methods for Redis.
 */
public class RedisApi {
	// Configuration
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = Protocol.DEFAULT_PORT;
	
	
	// Class variables
	private static JedisPool jedisDefaultPool;
	
	
	// Object variables
	private final JedisPool jedisPool;
	private final String host;
	private final int port;
	
	
	/**
	 * Initializes the API with the default host and default port.
	 */
	public RedisApi() {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}
	
	
	/**
	 * Initializes the API with the given host and the default port.
	 * 
	 * @param host
	 */
	public RedisApi(String host) {
		this(host, DEFAULT_PORT);
	}
	
	
	/**
	 * Initializes the API with the default host and the given port.
	 * 
	 * @param port
	 */
	public RedisApi(int port) {
		this(DEFAULT_HOST, port);
	}
	
	
	/**
	 * Initializes the API with the given host and the given port.
	 * 
	 * @param host
	 * @param port
	 */
	public RedisApi(String host, int port) {
		this.host = host;
		this.port = port;
		
		
		if (host.equals(DEFAULT_HOST) && port == DEFAULT_PORT) {
			jedisPool = null;
			
			if (jedisDefaultPool == null)
				jedisDefaultPool = new JedisPool(host, port);
		}
		else
			jedisPool = new JedisPool(host, port);
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
	 * Jedis is not thread safe. Use this method.
	 * 
	 * @return a jedis instance
	 */
	public Jedis getJedis() {
		if (jedisPool == null) {
			// Use the default pool
			return jedisDefaultPool.getResource();
		}
		else {
			// Use the pool for this host:port combination
			return jedisPool.getResource();
		}	
	}
	
	
	/**
	 * Closes the MongoClient instance (if any).
	 */
	public void close() {
		if (jedisPool != null)
			jedisPool.destroy();
	}	
	
	
	/**
	 * Drops the database with the given id.
	 * 
	 * @param databaseId
	 */
	public void dropDatabase(int databaseId) {
		try (Jedis jedis = getJedis()) {
			jedis.select(databaseId);
			jedis.flushDB();
		}
	}
	
	
	/**
	 * Deletes a key from the given database.
	 * 
	 * @param databaseId
	 * @param key
	 */
	public void delete(int databaseId, String key) {
		try (Jedis jedis = getJedis()) {
			jedis.select(databaseId);
			jedis.del(key);
			jedis.dbSize();
		}
	}
	
	
	/**
	 * Gets data from the given database by its key.
	 * 
	 * @param databaseId
	 * @param key
	 * @return the data
	 */
	public String get(int databaseId, String key) {
		try (Jedis jedis = getJedis()) {
			jedis.select(databaseId);
			return jedis.get(key);
		}
	}
	
	
	/**
	 * Gets data from the given database by its key from inside a HashMap.
	 * 
	 * @param databaseId
	 * @param key
	 * @param hashKey
	 * @return the data
	 */
	public String get(int databaseId, String key, String hashKey) {
		try (Jedis jedis = getJedis()) {
			jedis.select(databaseId);
			return jedis.hget(key, hashKey);
		}
	}
	
	
	/**
	 * Gets a Number from the given database by its key
	 * 
	 * @param databaseId
	 * @param key
	 * @return the data
	 */
	public Number getNumber(int databaseId, String key) {
		String string = get(databaseId, key);
		
		return ValueUtils.stringToNumber(string);
	}
	
	
	/**
	 * Gets a Number from the given database by its key
	 * 
	 * @param databaseId
	 * @param key
	 * @param hashKey
	 * @return the data
	 */
	public Number getNumber(int databaseId, String key, String hashKey) {
		String string = get(databaseId, key, hashKey);
		
		return ValueUtils.stringToNumber(string);
	}
	
	
	/**
	 * Puts (as upsert) data into the given database.
	 * 
	 * @param databaseId
	 * @param key
	 * @param value
	 */
	public void put(int databaseId, String key, Object value) {
		try (Jedis jedis = getJedis()) {
			jedis.select(databaseId);
			jedis.set(key, String.valueOf(value));
		}
	}
	
	
	/**
	 * Puts (as upsert) data into the given database.
	 * 
	 * @param databaseId
	 * @param key
	 * @param value
	 */
	public void put(int databaseId, String key, String hashKey, Object value) {
		try (Jedis jedis = getJedis()) {
			jedis.select(databaseId);
			jedis.hset(key, hashKey, String.valueOf(value));
		}
	}
	
	
	/**
	 * Gets an approximation for the number of items in this database.
	 * 
	 * @param databaseId
	 * @return dbsize
	 */
	public long dbsize(int databaseId) {
		try (Jedis jedis = getJedis()) {
			jedis.select(databaseId);
			return jedis.dbSize();
		}
	}
	
	
	/**
	 * Copies the existing database to a different database.
	 * 
	 * @param databaseId
	 * @param newDatabaseId
	 */
	public void copyDatabase(int databaseId, int newDatabaseId) {
		Jedis jedisNew = null;
		try {
			jedisNew = getJedis();
			jedisNew.select(newDatabaseId);
			
			try (Jedis jedis = getJedis()) {
				jedis.select(databaseId);
				
				for (String key : jedis.keys("*")) {
					if (jedis.type(key).equals("hash")) {
						// Copy the hashmap
						for (String hashKey : jedis.hkeys(key))
							jedisNew.hset(key, hashKey, jedis.hget(key, hashKey));
					}
					else
						jedisNew.set(key, jedis.get(key));
				}
			}
		} finally {
			if (jedisNew != null)
				jedisNew.close();
		}
	}
}
