package notaql.engines.redis.snapshot;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.AtomValue;
import notaql.datamodel.Value;
import notaql.engines.Engine;
import notaql.engines.incremental.DeltaFilter;
import notaql.engines.incremental.snapshot.SnapshotEngineEvaluator;
import notaql.engines.redis.RedisEngine;
import notaql.engines.redis.RedisEngineEvaluator;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.parser.TransformationParser;

/**
 * Used for snapshot-based incremental transformations.
 */
public class RedisEngineEvaluatorSnapshot extends RedisEngineEvaluator implements SnapshotEngineEvaluator {
	// Object variables
	private final int snapshot_database_id;
	
	
	public RedisEngineEvaluatorSnapshot(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {		
		super(engine, parser, params);
        
        if (!params.keySet().contains(RedisEngine.PARAMETER_NAME_SNAPSHOT))
            throw new EvaluationException(engine.getEngineName() +" engine (with snapshots) expects the following parameters on initialization: snapshot_database_id," + String.join(", ", engine.getArguments()));

        try {
            this.snapshot_database_id = Integer.parseInt(params.get(RedisEngine.PARAMETER_NAME_SNAPSHOT).getValue().toString());
        } catch(NumberFormatException e) {
            throw new EvaluationException("Redis-engine expects the snapshot_database_id to be an integer");
        }
	}
	
	
	/**
	 * @return the snapshot-database-id
	 */
	public int getSnapshotDatabaseId() {
		return this.snapshot_database_id;
	}
	

	/* (non-Javadoc)
	 * @see notaql.engines.redis.RedisEngineEvaluator#getDataFiltered(notaql.model.Transformation)
	 */
	@Override
	public JavaRDD<Value> getDataFiltered(Transformation transformation) {
		// Redirect this method-call to the SnapshotEngineEvaluator-interface
		return this.getCombinedData();
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.snapshot.SnapshotEngineEvaluator#getConvertedDataMain()
	 */
	@Override
	public JavaRDD<Value> getConvertedDataMain() {
    	return this.getData(this.database_id);
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.snapshot.SnapshotEngineEvaluator#getConvertedDataSnapshot()
	 */
	@Override
	public JavaRDD<Value> getConvertedDataSnapshot() {
    	return this.getData(this.snapshot_database_id);
	}
	

	/* (non-Javadoc)
	 * @see notaql.engines.EngineEvaluator#filterData(notaql.model.Transformation, org.apache.spark.api.java.JavaRDD)
	 */
	@Override
    public JavaRDD<Value> filterData(Transformation transformation, JavaRDD<Value> dataConverted) {
		return DeltaFilter.filterData(transformation, dataConverted);
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.IncrementalEngineEvaluator#supportsUpdates()
	 */
	@Override
	public boolean supportsUpdates() {
		return true;
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.IncrementalEngineEvaluator#supportsDeletes()
	 */
	@Override
	public boolean supportsDeletes() {
		return true;
	}
}
