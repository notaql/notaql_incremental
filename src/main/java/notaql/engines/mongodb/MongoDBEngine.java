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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.auto.service.AutoService;

import notaql.datamodel.AtomValue;
import notaql.datamodel.StringValue;
import notaql.engines.DatabaseReference;
import notaql.engines.Engine;
import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.snapshot.SnapshotEngine;
import notaql.engines.incremental.timestamp.TimestampEngine;
import notaql.engines.mongodb.snapshot.MongoDBEngineEvaluatorSnapshot;
import notaql.engines.mongodb.timestamp.MongoDBEngineEvaluatorTimestamp;
import notaql.parser.TransformationParser;

/**
 * Created by Thomas Lottermann on 06.12.14.
 */
@AutoService(Engine.class)
public class MongoDBEngine extends Engine implements TimestampEngine, SnapshotEngine {
    private static final long serialVersionUID = 5898695057464458198L;
	public static final String PARAMETER_NAME_DATABASE_NAME = "database_name";
	public static final String PARAMETER_NAME_COLLECTION_NAME = "collection_name";
	public static final String PARAMETER_NAME_SNAPSHOT = "snapshot_collection_name";
	public static final String PARAMETER_NAME_TIMESTAMP = "timestamp";


    /* (non-Javadoc)
     * @see notaql.engines.Engine#createEvaluator(notaql.parser.TransformationParser, java.util.Map)
     */
    @Override
    public EngineEvaluator createEvaluator(TransformationParser parser, Map<String, AtomValue<?>> params) {
        if (params.containsKey(PARAMETER_NAME_SNAPSHOT))
        	return new MongoDBEngineEvaluatorSnapshot(this, parser, params);
        
        else if (params.containsKey(PARAMETER_NAME_TIMESTAMP))
        	return new MongoDBEngineEvaluatorTimestamp(this, parser, params);
        
        else
        	return new MongoDBEngineEvaluator(this, parser, params);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.Engine#getEngineName()
     */
    @Override
    public String getEngineName() {
        return "mongodb";
    }


    /* (non-Javadoc)
     * @see notaql.engines.Engine#getArguments()
     */
    @Override
    public List<String> getArguments() {
    	List<String> arguments = new ArrayList<String>(this.getRequiredArguments());
    	arguments.add(PARAMETER_NAME_TIMESTAMP);
    	arguments.add(PARAMETER_NAME_SNAPSHOT);
    	arguments.add(Engine.PARAMETER_NAME_USER_EXPECTS_UPDATES);
    	arguments.add(Engine.PARAMETER_NAME_USER_EXPECTS_DELETES);
    	
    	
    	return arguments;
    }


    /* (non-Javadoc)
     * @see notaql.engines.Engine#getArguments()
     */
    @Override
    public List<String> getRequiredArguments() {
        return Arrays.asList(PARAMETER_NAME_DATABASE_NAME, PARAMETER_NAME_COLLECTION_NAME);
    }


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.timestamp.TimestampEngine#getTimestampParameterName()
	 */
	@Override
	public String getTimestampParameterName() {
		return PARAMETER_NAME_TIMESTAMP;
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.snapshot.SnapshotEngine#getSnapshotParameterName()
	 */
	@Override
	public String getSnapshotParameterName() {
		return PARAMETER_NAME_SNAPSHOT;
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.snapshot.SnapshotEngine#getSnapshotValue(notaql.engines.DatabaseReference)
	 */
	@Override
	public AtomValue<?> getSnapshotValue(DatabaseReference databaseReference) {
		if (!(databaseReference instanceof MongoDatabaseReference))
			throw new IllegalArgumentException("Parameter is no mongo-reference");
		
		return new StringValue(((MongoDatabaseReference) databaseReference).collection);
	}
}
