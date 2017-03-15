package notaql.engines.incremental.resultcombiner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.ObjectValue;
import notaql.datamodel.Value;
import notaql.datamodel.delta.DeltaValue;
import notaql.engines.EngineEvaluator;
import scala.Tuple2;

public interface CombiningEngineEvaluator {
	/**
	 * Combines the result (previously evaluated by NotaQL) with another result (which is in most cases the previous result).
	 * 
	 * In order to distinguish values in resultNew which shall be combined from the other results and in order to determine the way of combining
	 * the combinable results have to use special classes (e.g. NumberValueSum instead of NumberValue). These classes are set in the finalize()-
	 * methods of the Evaluator of the function (e.g. SUM()).
	 * 
	 * @param resultsNew
	 * @param resultsOld
	 * @return combined result
	 */
	default public JavaRDD<ObjectValue> combine(JavaRDD<ObjectValue> resultsNew, JavaRDD<ObjectValue> resultsOld) {
		// Oldresult to pairs
		JavaPairRDD<Value, ObjectValue> resultsOldPairs = resultsOld
				.mapToPair(value -> {
					ObjectValue row = (ObjectValue) value;
					return new Tuple2<Value, ObjectValue>(getId(row), row);
				});
		
		
		// Map the main data to pairs, join with the snapshot and combine
		ResultCombiner combiner = getResultCombiner();
		
		return resultsNew
			.mapToPair(value -> {
				ObjectValue row = (ObjectValue) value;
				return new Tuple2<Value, ObjectValue>(getId(row), row);
			})
			.fullOuterJoin(resultsOldPairs)
			.map(joinresult -> combiner.combine(joinresult._2));
	}
	 

	/**
	 * This default method provides a Combiner which may be used for rapidly developing new engines. This
	 * combiner does not have any knowledge about the engine specific format. Using this knowledge in an
	 * Engine-specific Combiner might lead to a better performance.
	 * 
	 * If there was an engine-specific Combiner developed this method shall be overriden. 
	 * 
	 * @return Combiner to be used
	 */
	default public ResultCombiner getResultCombiner() {
		return new ResultCombinerImplementation();
	}
	

	/**
	 * Gets the id from the row used for joining the old and the new result.
	 * 
	 * @param row
	 * @return
	 */
	static Value getId(ObjectValue row) {
		Value id = row.get(EngineEvaluator.getRowIdentifierStep());
	
		// Workaround: Overwriting the equals() method doesn't seem to be enough. Maybe a Spark-bug?
		if (id instanceof DeltaValue)
			return ((DeltaValue) id).getRawValue();
		else
			return id;
	}
}
