package notaql.engines.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

/**
 * Collection of frequently used methods for MongoDB.
 */
public class MongoApi {
	// Configuration
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 27017;
	
	
	// Object variables
	private final String host;
	private final int port;
	private MongoClient mongoClient;
	
	
	/**
	 * Initializes the API with the default host and default port.
	 */
	public MongoApi() {
		this(DEFAULT_HOST, DEFAULT_PORT);
	}
	
	
	/**
	 * Initializes the API with the given host and the default port.
	 * 
	 * @param host
	 */
	public MongoApi(String host) {
		this(host, DEFAULT_PORT);
	}
	
	
	/**
	 * Initializes the API with the default host and the given port.
	 * 
	 * @param port
	 */
	public MongoApi(int port) {
		this(DEFAULT_HOST, port);
	}
	
	
	/**
	 * Initializes the API with the given host and the given port.
	 * 
	 * @param host
	 * @param port
	 */
	public MongoApi(String host, int port) {
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
	 * Gets a MongoClient instance.
	 * 
	 * @return
	 * @throws IOException
	 */
	public MongoClient getMongo() throws IOException {
		if (mongoClient == null)
			mongoClient = new MongoClient(this.host, this.port);
		
		return mongoClient;
	}
	
	
	/**
	 * Closes the MongoClient instance (if any).
	 * @throws IOException 
	 */
	public void close() throws IOException {
		if (mongoClient != null)
			mongoClient.close();
	}
	
	
	/**
	 * Gets the database with the given name.
	 * 
	 * MongoDB creates the db on the first method call if it does not exist (only getting it won't create it).
	 * 
	 * @param databaseName
	 * @return
	 * @throws IOException
	 */
	public DB getDatabase(String databaseName) throws IOException {
		// Get a MongoDB-Connection
		MongoClient mongo = getMongo();
		
		return mongo.getDB(databaseName);
	}
	
	
	/**
	 * Gets the collection with the given name.
	 * 
	 * @param collectionName
	 * @param databaseName
	 * @return
	 */
	public DBCollection getCollection(String databaseName, String collectionName) throws IOException {
		return getCollection(databaseName, collectionName, false);
	}
	
	
	/**
	 * Gets the collection with the given name.
	 * 
	 * @param collectionName
	 * @param databaseName
	 * @param sharded
	 * @return
	 */
	public DBCollection getCollection(String databaseName, String collectionName, boolean sharded) throws IOException {
		// Get a MongoDB-Connection
		MongoClient mongo = getMongo();

		DBCollection collection = mongo.getDB(databaseName).getCollection(collectionName);
		
		// Shard the collection
		if (sharded) {
			// Hashed index on collection
			BasicDBObject index = new BasicDBObject("_id", "hashed");
			collection.createIndex(index);
			
			
			// Sharded DB?
			BasicDBObject shardCommandDb = new BasicDBObject("enableSharding", databaseName);
			mongo.getDB("admin").command(shardCommandDb);
			
			
			// Sharded collection
			BasicDBObject shardCommandCollection = new BasicDBObject("shardCollection", databaseName + "." + collectionName).append("key", index);
			mongo.getDB("admin").command(shardCommandCollection);
		}
		
		return collection;
	}
	
	
	
	/**
	 * Drops the database with the given name.
	 * 
	 * @param databaseName
	 * @throws IOException
	 */
	public void dropDatabase(String databaseName) throws IOException {
		// Get a MongoDB-Connection
		MongoClient mongo = getMongo();
		
		mongo.dropDatabase(databaseName);
	}
	
	
	/**
	 * Deletes an object with the given object id from the collection.
	 * 
	 * @param collection
	 * @param objectId
	 */
	public void delete(DBCollection collection, String objectId) {
		delete(collection, "_id", objectId);
	}
	
	
	/**
	 * Deletes objects from the given collection.
	 * 
	 * @param collection
	 * @param key
	 * @param value
	 */
	public void delete(DBCollection collection, String key, String value) {
		BasicDBObject searchQuery = new BasicDBObject().append(key, value);
		collection.remove(searchQuery);
	}
	
	
	/**
	 * Gets a value from the given collection by object-id and key.
	 * 
	 * @param collection
	 * @param objectId
	 * @param value
	 * @return the data
	 */
	public Object get(DBCollection collection, String objectId, String key) {
		List<Object> list = getMulti(collection, "_id", objectId, key);
		
		if (list.isEmpty())
			return null;
		else
			return list.get(0);
	}
	
	
	/**
	 * Gets a value from the given collection by searching for a specific key/value combination
	 * 
	 * @param collection
	 * @param key
	 * @param value
	 * @param keyInsideObject
	 * @return the data
	 */
	public Object get(DBCollection collection, String key, String value, String keyInsideObject) {
		List<Object> list = getMulti(collection, key, value, keyInsideObject);
		
		if (list.isEmpty())
			return null;
		else
			return list.get(0);
	}
	
	
	/**
	 * Gets values from the given collection by searching for a specific key/value combination
	 * 
	 * @param collection
	 * @param key
	 * @param value
	 * @param keyInsideObject
	 * @return the data
	 */
	public List<Object> getMulti(DBCollection collection, String key, String value, String keyInsideObject) {
		BasicDBObject searchQuery = new BasicDBObject().append(key, value);
		DBCursor cursor = collection.find(searchQuery);
		
		if (!cursor.hasNext())
			return Collections.emptyList();
		else {
			List<Object> list = new ArrayList<Object>();
			
			while (cursor.hasNext())
				list.add(((BasicDBObject) cursor.next()).get(keyInsideObject));
			
			return list;
		}
	}
	
	
	/**
	 * Checks if the given query returns at least one object.
	 * 
	 * @param collection
	 * @param searchQuery
	 * @return
	 */
	public boolean exists(DBCollection collection, BasicDBObject searchQuery) {
		DBCursor cursor = collection.find(searchQuery).limit(1);
		
		if (cursor.hasNext())
			return true;
		else
			return false;
	}
	
	
	/**
	 * Puts (as upsert) an object to the object in the given collection.
	 * 
	 * @param databaseName
	 * @param objectId
	 * @param key
	 * @param value
	 */
	public void put(DBCollection collection, Object objectId, String key, Object value) {
		put(collection, objectId, key, value, true);
	}
	
	
	/**
	 * Puts an object to the object in the given collection.
	 * 
	 * @param databaseName
	 * @param objectId
	 * @param key
	 * @param value
	 * @param upsert
	 */
	public void put(DBCollection collection, Object objectId, String key, Object value, boolean upsert) {
		put(collection, "_id", objectId, key, value, upsert);
	}
	
	
	/**
	 * Puts (as upsert) an object to the object in the given collection.
	 * 
	 * @param databaseName
	 * @param key
	 * @param value
	 * @param keyInsideObject
	 * @param valueInsideObject
	 */
	public void put(DBCollection collection, String key, Object value, String keyInsideObject, Object valueInsideObject) {
		put(collection, key, value, keyInsideObject, valueInsideObject, true);
	}
	
	
	/**
	 * Puts an object to the object in the given collection.
	 * 
	 * @param databaseName
	 * @param key
	 * @param value
	 * @param keyInsideObject
	 * @param valueInsideObject
	 * @param updateOne
	 */
	public void put(DBCollection collection, String key, Object value, String keyInsideObject, Object valueInsideObject, boolean updateOne) {
		put(collection, key, value, keyInsideObject, valueInsideObject, updateOne, true);
	}
	
	
	/**
	 * Puts an object to the object in the given collection.
	 * 
	 * @param databaseName
	 * @param key
	 * @param value
	 * @param keyInsideObject
	 * @param valueInsideObject
	 * @param updateOne
	 * @param upsert
	 */
	public void put(DBCollection collection, String key, Object value, String keyInsideObject, Object valueInsideObject, boolean updateOne, boolean upsert) {
		put(collection, key, value, keyInsideObject, valueInsideObject, updateOne, upsert, null);
	}
	
	
	/**
	 * Puts an object to the object in the given collection.
	 * 
	 * @param databaseName
	 * @param key
	 * @param value
	 * @param keyInsideObject
	 * @param valueInsideObject
	 * @param updateOne
	 * @param upsert
	 * @param bulk
	 */
	public void put(DBCollection collection, String key, Object value, String keyInsideObject, Object valueInsideObject, boolean updateOne, boolean upsert, BulkWriteOperation bulk) {
		if (upsert) {
			// Create an object with the new values
			BasicDBObject newObject = new BasicDBObject().append("$set", new BasicDBObject().append(keyInsideObject, valueInsideObject));
			
			
			// Do an upsert
			BasicDBObject searchQuery = new BasicDBObject().append(key, value);
			
			if (bulk != null) {
				if (updateOne)
					bulk.find(searchQuery).upsert().updateOne(newObject);
				else
					bulk.find(searchQuery).upsert().update(newObject);
			}
			else
				collection.update(searchQuery, newObject, true, updateOne);
		}
		else {
			// Create an object with the new values
			BasicDBObject newObject = new BasicDBObject();
			newObject.put(key, value);
			newObject.put(keyInsideObject, valueInsideObject);
			
			
			// Insert
			if (bulk != null)
				bulk.insert(newObject);
			else
				collection.insert(newObject);
		}
	}
	
	
	/**
	 * Copies the existing collection to a new collection in the same db.
	 * 
	 * Any contained documents will be overwritten.
	 * 
	 * @param collection
	 * @param newCollectionName
	 * @return the new collection
	 * @throws IOException 
	 */
	public DBCollection copyCollection(DBCollection collection, String newCollectionName) throws IOException {
		DBCollection newCollection = getCollection(collection.getDB().getName(), newCollectionName);
		truncate(newCollection);
		
		// Available for version >= 2.6
		collection.aggregate(Arrays.asList(new BasicDBObject("$out", newCollectionName)));
		
		// Available for version < 2.6
		// DBCursor cursor = collection.find();
		// while (cursor.hasNext())
		// 	newCollection.insert(cursor.next());
		
		
		return newCollection;
	}
	
	
	/**
	 * Truncates the given collection.
	 * 
	 * @param collection
	 */
	public void truncate(DBCollection collection) {
		collection.remove(new BasicDBObject());
	}
}
