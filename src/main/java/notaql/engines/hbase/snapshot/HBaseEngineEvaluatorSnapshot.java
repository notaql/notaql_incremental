package notaql.engines.hbase.snapshot;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.AtomValue;
import notaql.datamodel.Value;
import notaql.engines.Engine;
import notaql.engines.hbase.HBaseEngine;
import notaql.engines.hbase.HBaseEngineEvaluator;
import notaql.engines.incremental.DeltaFilter;
import notaql.engines.incremental.snapshot.SnapshotEngineEvaluator;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.parser.TransformationParser;

/**
 * Used for snapshot-based incremental transformations.
 * 
 * The advantage over the timestamp based implementation is that there is no need to retrieve all versions (only the last versions
 * from the snapshot and the current table are needed).  
 */
public class HBaseEngineEvaluatorSnapshot extends HBaseEngineEvaluator implements SnapshotEngineEvaluator {
	// Object variables
	private final String snapshot_table_id;
	
	
	public HBaseEngineEvaluatorSnapshot(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {		
		super(engine, parser, params);
        
        if (!params.keySet().contains(HBaseEngine.PARAMETER_NAME_SNAPSHOT))
            throw new EvaluationException(engine.getEngineName() +" engine (with snapshots) expects the following parameters on initialization: snapshot_table_id," + String.join(", ", engine.getArguments()));

        this.snapshot_table_id = params.get(HBaseEngine.PARAMETER_NAME_SNAPSHOT).getValue().toString();
	}

	
	/* (non-Javadoc)
	 * @see notaql.engines.hbase.HBaseEngineEvaluator#getDataFiltered(notaql.model.Transformation)
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
    	return this.getData(this.table_id);
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.snapshot.SnapshotEngineEvaluator#getConvertedDataSnapshot()
	 */
	@Override
	public JavaRDD<Value> getConvertedDataSnapshot() {
		return this.getData(this.snapshot_table_id);
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
