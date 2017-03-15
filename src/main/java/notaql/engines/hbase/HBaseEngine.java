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

package notaql.engines.hbase;

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
import notaql.engines.hbase.snapshot.HBaseEngineEvaluatorSnapshot;
import notaql.engines.hbase.timestamp.HBaseEngineEvaluatorTimestamp;
import notaql.engines.hbase.triggers.HBaseEngineEvaluatorTrigger;
import notaql.engines.incremental.snapshot.SnapshotEngine;
import notaql.engines.incremental.timestamp.TimestampEngine;
import notaql.parser.TransformationParser;

/**
 * The HBase engine. This expects this argument: table_id
 */
@AutoService(Engine.class)
public class HBaseEngine extends Engine implements TimestampEngine, SnapshotEngine {
	// Configuration
    private static final long serialVersionUID = -4279357370408702185L;
	public static final String PARAMETER_NAME_TABLE_ID = "table_id";
	public static final String PARAMETER_NAME_SNAPSHOT = "snapshot_table_id";
	public static final String PARAMETER_NAME_TIMESTAMP = "timestamp";
	public static final String PARAMETER_NAME_TRIGGERBASED = "triggerbased";

    
    /* (non-Javadoc)
     * @see notaql.engines.Engine#createEvaluator(notaql.parser.TransformationParser, java.util.Map)
     */
    @Override
    public EngineEvaluator createEvaluator(TransformationParser parser, Map<String, AtomValue<?>> params) {
        if (params.containsKey(PARAMETER_NAME_TRIGGERBASED) && !params.get(PARAMETER_NAME_TRIGGERBASED).getValue().toString().equalsIgnoreCase("false"))
        	return new HBaseEngineEvaluatorTrigger(this, parser, params);
        
        else if (params.containsKey(PARAMETER_NAME_SNAPSHOT))
        	return new HBaseEngineEvaluatorSnapshot(this, parser, params);
        
        else if (params.containsKey(PARAMETER_NAME_TIMESTAMP))
        	return new HBaseEngineEvaluatorTimestamp(this, parser, params);
        
        else
        	return new HBaseEngineEvaluator(this, parser, params);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.Engine#getEngineName()
     */
    @Override
    public String getEngineName() {
        return "hbase";
    }


    /* (non-Javadoc)
     * @see notaql.engines.Engine#getArguments()
     */
    @Override
    public List<String> getArguments() {
    	List<String> arguments = new ArrayList<String>(this.getRequiredArguments());
    	arguments.add(PARAMETER_NAME_TIMESTAMP);
    	arguments.add(PARAMETER_NAME_SNAPSHOT);
    	arguments.add(PARAMETER_NAME_TRIGGERBASED);
    	arguments.add(Engine.PARAMETER_NAME_USER_EXPECTS_UPDATES);
    	arguments.add(Engine.PARAMETER_NAME_USER_EXPECTS_DELETES);
    	
    	return arguments;
    }
    
    
    /* (non-Javadoc)
     * @see notaql.engines.Engine#getRequiredArguments()
     */
    @Override
    public List<String> getRequiredArguments() {
        return Arrays.asList(PARAMETER_NAME_TABLE_ID);
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
		if (!(databaseReference instanceof HBaseDatabaseReference))
			throw new IllegalArgumentException("Parameter is no hbase-reference");
		
		return new StringValue(((HBaseDatabaseReference) databaseReference).tableId);
	}
}
