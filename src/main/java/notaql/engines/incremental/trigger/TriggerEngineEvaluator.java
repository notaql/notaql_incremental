package notaql.engines.incremental.trigger;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.ObjectValue;
import notaql.datamodel.Value;
import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.IncrementalEngineEvaluator;
import notaql.engines.incremental.snapshot.SnapshotCombiner;
import notaql.engines.incremental.snapshot.SnapshotCombinerImplementation;
import notaql.model.Transformation;
import scala.Tuple2;

/**
 * Marks an engine evaluator as being used for trigger-based incremental computations.
 * 
 * Trigger-based are (in contrast to the other EngineEvaluator-types) not started by the User but by the Database itself. This means
 * that the database will send information about the changed table/collection/..., the previous data and the new data to the NotaQL-
 * server. The server then re-executes the incremental computations with this new data.
 * 
 * The SnapshotEngineEvaluator is the base type for trigger-based engine evaluators, because the old data is used as the "snapshot"-data
 * and the new data is the "current"-data. Determining the correct delta flags (preserved, inserted, deleted) from this data is 
 * handled the same way as with snapshots.  
 */
public interface TriggerEngineEvaluator extends IncrementalEngineEvaluator {
	/**
	 * Converts the given String into converted data (which is using the internal NotaQL-format)
	 * 
	 * @param input
	 * @return
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public JavaRDD<Value> stringToDataConverted(String input);
	

    /**
     * Evaluates the transformation with the given data.
     * 
     * @param transformation
     * @param data_new
     * @param data_old
     * @return
     * @throws Exception 
     */
	public JavaRDD<ObjectValue> evaluate(Transformation transformation, String data_new, String data_old);
	
	
	/**
	 * Combines the previous data and the new data send by the trigger. The resulting RDD contains data
	 * which has preserved-, inserted- and delete-flags (also see TimestampEngineEvaluator and SnapshotEngineEvaluator).
	 * 
	 * @param dataConvertedMain should only contain ObjectValues
	 * @param dataConvertedSnapshot should only contain ObjectValues
	 * @return combined and flagged data
	 * @throws Exception 
	 */
	default public JavaRDD<Value> combineWithFlags(String data_new, String data_old) {
		/*
		 * Most parts of the following code were copied from the SnapshotEngineEvaluator interface.
		 * In order to simply copy back changes the variable-name were not changed.
		 */
		JavaRDD<Value> dataConvertedMain = stringToDataConverted(data_new);
		JavaRDD<Value> dataConvertedSnapshot = stringToDataConverted(data_old);

		
		// Snapshot to pairs
		JavaPairRDD<Value, ObjectValue> dataConvertedSnapshotPairs = dataConvertedSnapshot
				.mapToPair(value -> {
					ObjectValue row = (ObjectValue) value;
					return new Tuple2<Value, ObjectValue>(row.get(EngineEvaluator.getRowIdentifierStep()), row);
				});
		
		
		// Map the main data to pairs, join with the snapshot and combine
		SnapshotCombiner combiner = new SnapshotCombinerImplementation();
		
		return dataConvertedMain
			.mapToPair(value -> {
				ObjectValue row = (ObjectValue) value;
				return new Tuple2<Value, ObjectValue>(row.get(EngineEvaluator.getRowIdentifierStep()), row);
			})
			.fullOuterJoin(dataConvertedSnapshotPairs)
			.flatMap(joinresult -> combiner.combine(joinresult._2))
			.map(objectValue -> (Value) objectValue);
	}
}
