/*
 * Copyright 2015 by Thomas Lottermann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package notaql.engines.mongodb;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;

import notaql.NotaQL;
import notaql.SparkFactory;
import notaql.datamodel.AtomValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Value;
import notaql.engines.DatabaseReference;
import notaql.engines.DatabaseSystem;
import notaql.engines.Engine;
import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.resultcombiner.CombiningEngineEvaluator;
import notaql.engines.mongodb.datamodel.ValueConverter;
import notaql.engines.mongodb.exceptions.UserDefinedOIDException;
import notaql.engines.mongodb.model.vdata.HashFunctionVData;
import notaql.engines.mongodb.model.vdata.ListCountFunctionVData;
import notaql.engines.mongodb.model.vdata.ObjectIdFunctionVData;
import notaql.engines.mongodb.parser.path.MongoDBInputPathParser;
import notaql.engines.mongodb.parser.path.MongoDBOutputPathParser;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.model.vdata.FunctionVData;
import notaql.parser.TransformationParser;
import notaql.parser.path.InputPathParser;
import notaql.parser.path.OutputPathParser;
import scala.Tuple2;


public class MongoDBEngineEvaluator extends EngineEvaluator implements CombiningEngineEvaluator, DatabaseSystem {
	// Configuration
	private static final String PROPERTY_MONGODB_HOST = "mongodb_host";
	private static final int MONGODB_BSON_TYPE_OBJECTID = 7;
	
	
	// Object variables
	private DatabaseReference databaseReference;
    protected final String database_name;
    protected final String collection_name;

    
    public MongoDBEngineEvaluator(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {
        super(engine, parser, params);

        this.database_name = params.get(MongoDBEngine.PARAMETER_NAME_DATABASE_NAME).getValue().toString();
        this.collection_name = params.get(MongoDBEngine.PARAMETER_NAME_COLLECTION_NAME).getValue().toString();
    }


    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getInputPathParser()
     */
    @Override
    public InputPathParser getInputPathParser() {
        return new MongoDBInputPathParser(this.getParser());
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getOutputPathParser()
     */
    @Override
    public OutputPathParser getOutputPathParser() {
        return new MongoDBOutputPathParser(this.getParser());
    }
    
    
    /**
     * @return the database name
     */
    public String getDatabaseName() {
    	return this.database_name;
    }
    
    
    /**
     * @return the collection name
     */
    public String getCollectionName() {
    	return this.collection_name;
    }
    
    
    /**
     * Retrieves the data from the given database and collection without using the in-filter
     * 
     * @param databaseName
     * @param collectionName
     * @return the converted data
     */
    protected JavaRDD<Value> getData(String databaseName, String collectionName) {
    	return getDataFiltered(null, databaseName, collectionName);
    }
    
    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getDataFiltered(notaql.model.Transformation)
     */
    public JavaRDD<Value> getDataFiltered(Transformation transformation) {
    	return getDataFiltered(transformation, this.database_name, this.collection_name);
    }

	/**
	 * Retrives the data from the given database.
	 * 
	 * @param transformation
	 * @param databaseName
	 * @param collectionName
	 * @return
	 */
	protected JavaRDD<Value> getDataFiltered(Transformation transformation, String databaseName, String collectionName) {
		return this.getDataFiltered(transformation, databaseName, collectionName, null);
	}
    
	/**
	 * Retrives the data from the given database using timestamps.
	 * 
	 * @param transformation
	 * @param databaseName
	 * @param collectionName
	 * @param previousTimestamp if set to a timestamp only documents with previousTimestamp <= creation_time < current_time will be returned. This also limits the results to documents with a generated timestamp.
	 * @return
	 */
	protected JavaRDD<Value> getDataFiltered(Transformation transformation, String databaseName, String collectionName, Long previousTimestamp) {
    	// Configuration for the following steps
    	Configuration sparkConfiguration = this.createSparkConfiguration("mongo.input.uri", databaseName, collectionName);
    	
    	ValueConverter valueConverter;
    	if (previousTimestamp == null)
    		valueConverter = new ValueConverter();
    	else
    		valueConverter = new notaql.engines.mongodb.timestamp.TimestampValueConverter(previousTimestamp);
    	
    	
        // Create a partial filter from the transformation (more locality)
    	// Don't limit the maximum timestamp to the current system-timestamp because this also limits the result
    	//   to documents with generated timestamps
        BSONObject query = null;
        if (transformation != null && transformation.getInPredicate() != null) {
            query = FilterTranslator.toMongoDBQuery(transformation.getInPredicate());
        }
        
        if (previousTimestamp != null) {
        	// Make sure that this query won't exclude any documents without generated object-ids
        	try {
	        	BasicDBObject queryOid = new BasicDBObject("_id", new BasicDBObject("$not", new BasicDBObject("$type", MONGODB_BSON_TYPE_OBJECTID)));
	        	MongoApi mongoApi = new MongoApi(this.getHost(), this.getPort());
	        	
	        	if (mongoApi.exists(mongoApi.getCollection(databaseName, collectionName), queryOid))
	        		throw new UserDefinedOIDException("At least one document with a user-defined _id in " + databaseName + "." + collectionName + " => No timestamp query possible");
        	
				mongoApi.close();
			} catch (IOException e) {
				e.printStackTrace();
				throw new EvaluationException("Couldn't check the _id in " + databaseName + "." + collectionName + ": " + e.getMessage());
			}
        	
        	
        	// Generate the query
        	if (query == null)
        		query = new BasicBSONObject();
    		
    		// Add the end-timestamp restriction to the query (only objects which were created before the query was started)
        	// The current timestamp is increased by 1 to avoid problems with data commited right before the query
    		query.put("_id", new BasicBSONObject("$lte", new ObjectId(Long.toHexString((this.getCurrentTimestamp()/1000)+1) + "0000000000000000")));
    		
    		// Add the start-timestamp restriction to the query (only objects which were created after the previous timestamp)
    		((BasicBSONObject) query.get("_id")).put("$gte", new ObjectId(Long.toHexString(previousTimestamp/1000) + "0000000000000000"));
        }
        
        if (query != null)
        	sparkConfiguration.set("mongo.input.query", query.toString());
                

    	// Use spark to retrieve the raw input rdds
        // These raw input rdds are then converted to the inner format used by this notaql-framework
        JavaSparkContext spark = SparkFactory.getSparkContext();
        JavaPairRDD<Object, BSONObject> dataRaw = spark.newAPIHadoopRDD(sparkConfiguration, MongoInputFormat.class, Object.class, BSONObject.class);
        return dataRaw.map(tuple -> valueConverter.convertToNotaQL(tuple._2));
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#store(org.apache.spark.api.java.JavaRDD)
     */
    @Override
    public void store(JavaRDD<ObjectValue> resultRaw) {
		super.store(resultRaw);
		
		
    	// Configuration for the following steps
    	Configuration sparkConfiguration = this.createSparkConfiguration("mongo.output.uri", this.database_name, this.collection_name);


        // Generate the resulting objects using spark
        JavaPairRDD<Object, BSONObject> resultObjects = resultRaw
        		.filter(objectvalue -> !(objectvalue.toMap().isEmpty() || objectvalue.toMap().size() == 1 && objectvalue.toMap().containsKey(getRowIdentifierStep()))) // Empty documents don't have to be saved
        		.mapToPair(object -> new Tuple2<>(null, (DBObject)ValueConverter.convertFromNotaQL(object)));

        
        // Store the result using spark
        // The path argument is unused; all documents will go to 'mongo.output.uri'.
        resultObjects.saveAsNewAPIHadoopFile("file:///notused", Object.class, BSONObject.class, MongoOutputFormat.class, sparkConfiguration);
    }
    
    
    /**
     * @return the host
     */
    public String getHost() {
    	return NotaQL.getProperties().getProperty(PROPERTY_MONGODB_HOST, MongoApi.DEFAULT_HOST);
    }
    
    
    /**
     * @return the port
     */
    public int getPort() {
    	return MongoApi.DEFAULT_PORT;
    }

    
    /**
     * @return the default configuration to be used with spark
     */
    protected Configuration createSparkConfiguration(String propertyKeyPath, String databaseName, String collectionName) {
        Configuration sparkConfiguration = new Configuration();
        String mongoDBHost = "mongodb://" + this.getHost() + ":" + String.valueOf(this.getPort()) + "/";
        sparkConfiguration.set(propertyKeyPath, mongoDBHost + databaseName + "." + collectionName);

        return sparkConfiguration;
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getFunction(java.lang.String)
     */
    @Override
    public FunctionVData getFunction(String name) {
        switch (name) {
	    	case "LIST_COUNT":
	            return new ListCountFunctionVData();
	            
	    	case "HASH":
	            return new HashFunctionVData();
	            
	    	case "OBJECT_ID":
	            return new ObjectIdFunctionVData();
			
			default:
				return null;
	    }
    }


	/* (non-Javadoc)
	 * @see notaql.engines.EngineEvaluator#isBaseType()
	 */
	@Override
	public boolean isBaseType() {
		return this.getClass().equals(MongoDBEngineEvaluator.class);
	}
	
	
	public DatabaseReference getDatabaseReference() {
		if (databaseReference == null)
			databaseReference = new MongoDatabaseReference(this.getHost(), this.getPort(), this.database_name, this.collection_name);
		return databaseReference;
	}
}
