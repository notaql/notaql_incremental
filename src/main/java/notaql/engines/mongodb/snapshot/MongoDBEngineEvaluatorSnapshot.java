package notaql.engines.mongodb.snapshot;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.AtomValue;
import notaql.datamodel.Value;
import notaql.engines.Engine;
import notaql.engines.incremental.DeltaFilter;
import notaql.engines.incremental.snapshot.SnapshotEngineEvaluator;
import notaql.engines.mongodb.MongoDBEngine;
import notaql.engines.mongodb.MongoDBEngineEvaluator;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.parser.TransformationParser;

/**
 * Used for snapshot-based incremental transformations.
 * 
 * The advantage over the timestamp based implementation is that snapshots basically provide us with a history. Because of this 
 * also updates and deletes are possible (they are not possible with the timestamp implementation because normally there is no history
 * in MongoDB)
 * Another advatage is that while the timestamp-only implementation does detect inserts only per object(-id) this implementation
 * does detect them on the level of the values (Strings, Numbers, ...) and by this is more fine-grained
 * 
 * A disadvantage is that there is no way of using the locality of a database-supported IN-FILTER (because this is incompatible with
 * the DeltaFilter)
 */
public class MongoDBEngineEvaluatorSnapshot extends MongoDBEngineEvaluator implements SnapshotEngineEvaluator  {
	// Object variables
	private final String snapshot_collection_name;
	
	
	public MongoDBEngineEvaluatorSnapshot(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {		
		super(engine, parser, params);
        
        if (!params.keySet().contains(MongoDBEngine.PARAMETER_NAME_SNAPSHOT))
            throw new EvaluationException(engine.getEngineName() +" engine (with snapshots) expects the following parameters on initialization: snapshot_collection_name," + String.join(", ", engine.getArguments()));

        this.snapshot_collection_name = params.get(MongoDBEngine.PARAMETER_NAME_SNAPSHOT).getValue().toString();
	}
	

	/* (non-Javadoc)
	 * @see notaql.engines.mongodb.MongoDBEngineEvaluator#getDataFiltered(notaql.model.Transformation)
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
		return this.getData(this.database_name, this.collection_name);
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.snapshot.SnapshotEngineEvaluator#getConvertedDataSnapshot()
	 */
	@Override
	public JavaRDD<Value> getConvertedDataSnapshot() {
		return this.getData(this.database_name, this.snapshot_collection_name);
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
