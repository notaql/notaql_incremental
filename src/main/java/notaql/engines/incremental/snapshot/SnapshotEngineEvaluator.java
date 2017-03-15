package notaql.engines.incremental.snapshot;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.ObjectValue;
import notaql.datamodel.Value;
import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.IncrementalEngineEvaluator;
import scala.Tuple2;

/**
 * Marks an engine evaluator as being used for snapshot-based incremental computations.
 * 
 * Basically snapshots are used in order to emulate a form of timestamp based computation with full support for updates and deletes (which is only
 * possible with timestamps for Hbase (but without full support for deletes), but not for MongoDB (inserts only), Redis, CSV or JSON (last 3 have no
 * timestamps at all). This type of CDC uses the fact that only 2 versions are needed for computing the delta (the database state at the previous
 * computation and the current state).
 * 
 * Note that the snapshot-data won't be overwritten by the implementations of snapshot-based engine evaluators. This has the following reasons:
 * 1.) It would limit the range of supported engines to engines for which output=input (which is not the case for e.g. csv)
 * 2.) It would make performance benchmarks between timestamp-based engine evaluators and snapshot-based engine evaluators impossible (as timestamp-
 * based evaluators don't have to handle such a kind of additional write-workload)
 */
public interface SnapshotEngineEvaluator extends IncrementalEngineEvaluator {
	/**
	 * Retrieves the converted data from the main source
	 * 
	 * @return RDD with the converted data
	 */
	public JavaRDD<Value> getConvertedDataMain();
	

	/**
	 * Retrieves the converted data from the snapshot
	 * 
	 * @return RDD with the converted data
	 */
	public JavaRDD<Value> getConvertedDataSnapshot();
	 

	/**
	 * This default method provides a Combiner which may be used for rapidly developing new engines. This
	 * combiner does not have any knowledge about the engine specific format. Using this knowledge in an
	 * Engine-specific Combiner might lead to a better performance.
	 * 
	 * If there was an engine-specific Combiner developed this method shall be overriden. 
	 * 
	 * @return Combiner to be used
	 */
	default public SnapshotCombiner getSnapshotCombiner() {
		return new SnapshotCombinerImplementation();
	}
	
	
	/**
	 * Combines the main data and the snapshot data. The resulting RDD contains data which has preserved-, inserted- and
	 * delete-flags (also see TimestampEngineEvaluator).
	 * 
	 * @param dataConvertedMain should only contain ObjectValues
	 * @param dataConvertedSnapshot should only contain ObjectValues
	 * @return combined and flagged data
	 */
	default public JavaRDD<Value> combineWithFlags(JavaRDD<Value> dataConvertedMain, JavaRDD<Value> dataConvertedSnapshot) {
		// Snapshot to pairs
		JavaPairRDD<Value, ObjectValue> dataConvertedSnapshotPairs = dataConvertedSnapshot
				.mapToPair(value -> {
					ObjectValue row = (ObjectValue) value;
					return new Tuple2<Value, ObjectValue>(row.get(EngineEvaluator.getRowIdentifierStep()), row);
				});
		
		
		// Map the main data to pairs, join with the snapshot and combine
		SnapshotCombiner combiner = getSnapshotCombiner();
		
		return dataConvertedMain
			.mapToPair(value -> {
				ObjectValue row = (ObjectValue) value;
				return new Tuple2<Value, ObjectValue>(row.get(EngineEvaluator.getRowIdentifierStep()), row);
			})
			.fullOuterJoin(dataConvertedSnapshotPairs)
			.flatMap(joinresult -> combiner.combine(joinresult._2))
			.map(objectValue -> (Value) objectValue);
	}
	

	/**
	 * Used for accessing the combined and converted data. 
	 * 
	 * @return
	 */
	default public JavaRDD<Value> getCombinedData() {
		// Retrieve the data from both sources and make sure the data contains only ObjectValues
		JavaRDD<Value> dataConvertedMain = getConvertedDataMain();
		JavaRDD<Value> dataConvertedSnapshot = getConvertedDataSnapshot();
		
		
		// Combine the data-sets
		return combineWithFlags(dataConvertedMain, dataConvertedSnapshot);
	}
}
