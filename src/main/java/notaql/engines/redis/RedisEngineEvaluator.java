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

package notaql.engines.redis;

import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import notaql.NotaQL;
import notaql.SparkFactory;
import notaql.datamodel.AtomValue;
import notaql.datamodel.ListBasedCollectionValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.StringValue;
import notaql.datamodel.Value;
import notaql.engines.DatabaseReference;
import notaql.engines.DatabaseSystem;
import notaql.engines.Engine;
import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.resultcombiner.CombiningEngineEvaluator;
import notaql.engines.mongodb.model.vdata.ListCountFunctionVData;
import notaql.engines.redis.datamodel.ValueConverter;
import notaql.engines.redis.model.vdata.HashMapConstructorVData;
import notaql.engines.redis.parser.path.RedisInputPathParser;
import notaql.engines.redis.parser.path.RedisOutputPathParser;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.model.vdata.ConstructorVData;
import notaql.model.vdata.FunctionVData;
import notaql.parser.TransformationParser;
import notaql.parser.path.InputPathParser;
import notaql.parser.path.OutputPathParser;
import redis.clients.jedis.Jedis;

/**
 * Created by thomas on 23.02.15.
 */
public class RedisEngineEvaluator extends EngineEvaluator implements CombiningEngineEvaluator, DatabaseSystem  {
	// Configuration
	private static final String PROPERTY_REDIS_HOST = "redis_host";
	
	
    // Object variables
	private DatabaseReference databaseReference;
	protected final int database_id;
	protected RedisApi redisApi;
    

    public RedisEngineEvaluator(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {
        super(engine, parser, params);

        try {
            this.database_id = Integer.parseInt(params.get(RedisEngine.PARAMETER_NAME_DATABASE_ID).getValue().toString());
        } catch(NumberFormatException e) {
            throw new EvaluationException("Redis-engine expects the database_id to be an integer");
        }
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getInputPathParser()
     */
    @Override
    public InputPathParser getInputPathParser() {
        return new RedisInputPathParser(this.getParser());
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getOutputPathParser()
     */
    @Override
    public OutputPathParser getOutputPathParser() {
        return new RedisOutputPathParser(this.getParser());
    }
    
    
    /**
     * @return the database_id
     */
    public int getDatabaseId() {
    	return this.database_id;
    }
    
    
    /**
     * @return the host for this instance
     */
    public static String getHost() {
    	return NotaQL.getProperties().getProperty(PROPERTY_REDIS_HOST, RedisApi.DEFAULT_HOST);
    }
    
    
    /**
     * @return the host for this instance
     */
    public static int getPort() {
    	return RedisApi.DEFAULT_PORT;
    }
    
    
    /**
     * @return a jedis instance of this engine evaluator.
     */
    public static Jedis getJedis() {
    	return new Jedis(getHost(), getPort());
    }
    

    /**
     * Retrives the data from the given database
     * 
     * @param transformation
     * @param databaseId
     * @return
     */
    protected JavaRDD<Value> getData(int databaseId) {
    	// Configuration
        final ListBasedCollectionValue redisCompleteCollection = new ListBasedCollectionValue();
        
        
    	// Connect to the database and read in *all* objects
        // TODO: this is really slow and doesn't scale!
        try (Jedis jedis = getJedis()) {
	        jedis.select(databaseId);
	        
	        
	        final Set<String> redisKeys = jedis.keys("*");
	        for (String redisKey: redisKeys) {
	            ObjectValue objectValue = new ObjectValue();
	            objectValue.put(EngineEvaluator.getRowIdentifierStep(), new StringValue(redisKey));
	            objectValue.put(new Step<>("_v"), ValueConverter.readFromRedis(jedis, redisKey));
	
	            redisCompleteCollection.add(objectValue);
	        }
        }


    	// Use spark to parallelize (= convert) the raw data from redis
        // After that the entries which do not fulfill the input filter will be filtered  (queries of MongoDB are less expressive than NotaQL so we have to this again even if there was a mongodb-filter defined)
        final JavaSparkContext spark = SparkFactory.getSparkContext();
        JavaRDD<Value> dataConverted = spark.parallelize(redisCompleteCollection);
        
        
        return dataConverted;
    }
    
    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getDataFiltered(notaql.model.Transformation)
     */
    // No engine specific filtering (using the transformation) is currently applied
    public JavaRDD<Value> getDataFiltered(Transformation transformation) {
    	return this.getData(this.database_id);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#store(org.apache.spark.api.java.JavaRDD)
     */
    @Override
    public void store(JavaRDD<ObjectValue> resultRaw) {
		super.store(resultRaw);
		
		
    	// Configuration
    	final String host = getHost();
        

    	// Write all objects
    	ValueConverter valueConverter = new ValueConverter(host, RedisApi.DEFAULT_PORT, this.database_id);
    	resultRaw.foreach(valueConverter::writeToRedis);
    }


    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getConstructor(java.lang.String)
     */
    @Override
    public ConstructorVData getConstructor(String name) {
        if(name.equals("HM"))
            return new HashMapConstructorVData();
        return null;
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getFunction(java.lang.String)
     */
    @Override
    public FunctionVData getFunction(String name) {
        switch (name) {
	    	case "LIST_COUNT":
	            return new ListCountFunctionVData();
			
			default:
				return null;
	    }
    }


	/* (non-Javadoc)
	 * @see notaql.engines.EngineEvaluator#isBaseType()
	 */
	@Override
	public boolean isBaseType() {
		return this.getClass().equals(RedisEngineEvaluator.class);
	}
	
	
	public DatabaseReference getDatabaseReference() {
		if (databaseReference == null)
			databaseReference = new RedisDatabaseReference(getHost(), getPort(), this.database_id);
		return databaseReference;
	}
}
