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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.auto.service.AutoService;

import notaql.datamodel.AtomValue;
import notaql.datamodel.NumberValue;
import notaql.engines.DatabaseReference;
import notaql.engines.Engine;
import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.snapshot.SnapshotEngine;
import notaql.engines.redis.snapshot.RedisEngineEvaluatorSnapshot;
import notaql.parser.TransformationParser;

@AutoService(Engine.class)
public class RedisEngine extends Engine implements SnapshotEngine {
	// Configuration
    private static final long serialVersionUID = 2944588366537254674L;
	public static final String PARAMETER_NAME_DATABASE_ID = "database_id";
	public static final String PARAMETER_NAME_SNAPSHOT = "snapshot_database_id";


    /* (non-Javadoc)
     * @see notaql.engines.Engine#createEvaluator(notaql.parser.TransformationParser, java.util.Map)
     */
    @Override
    public EngineEvaluator createEvaluator(TransformationParser parser, Map<String, AtomValue<?>> params) {
        if (params.containsKey(PARAMETER_NAME_SNAPSHOT))
        	return new RedisEngineEvaluatorSnapshot(this, parser, params);
        
        else
        	return new RedisEngineEvaluator(this, parser, params);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.Engine#getEngineName()
     */
    @Override
    public String getEngineName() {
        return "redis";
    }


    /* (non-Javadoc)
     * @see notaql.engines.Engine#getArguments()
     */
    @Override
    public List<String> getArguments() {
    	List<String> arguments = new ArrayList<String>(this.getRequiredArguments());
    	arguments.add(PARAMETER_NAME_SNAPSHOT);
    	arguments.add(Engine.PARAMETER_NAME_USER_EXPECTS_UPDATES);
    	arguments.add(Engine.PARAMETER_NAME_USER_EXPECTS_DELETES);
    	
    	return arguments;
    }


    /* (non-Javadoc)
     * @see notaql.engines.Engine#getRequiredArguments()
     */
    @Override
    public List<String> getRequiredArguments() {
        return Arrays.asList(PARAMETER_NAME_DATABASE_ID);
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
		if (!(databaseReference instanceof RedisDatabaseReference))
			throw new IllegalArgumentException("Parameter is no redis-reference");
			
		return new NumberValue(((RedisDatabaseReference) databaseReference).databaseId);
	}
}
