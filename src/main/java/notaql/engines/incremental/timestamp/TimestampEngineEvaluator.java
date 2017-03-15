package notaql.engines.incremental.timestamp;

import notaql.engines.incremental.IncrementalEngineEvaluator;

/**
 * Marks an engine evaluator as being used for timestamp-based incremental computations.
 * 
 * (Also see the paper "Incremental Data Transformations on Wide-Column Stores with NotaQL" for details.)
 * 
 * Timestamps provivded by the database are used for generating a delta from the previous result to the current state of the database. There
 * are 4 types of values which have to be generated based on the state of the database at the previous computation and the current state:
 * 1) "preserved": The value hasn't changed since the last computation.
 * 2) "insert": The value was not in the database at the previous computation.
 * 3) "delete": The value is not in the database any more but was at the previous computation.
 * 4) "oldresult": The previous result
 * Also the combination "delete"+"insert" (2 values in the input for the same cell) is present if the value was changed.
 */
public interface TimestampEngineEvaluator extends IncrementalEngineEvaluator  {
	/**
	 * @return the latest timestamp visited (all values with later timestamps will be ignored and may be included in the next iteration)
	 */
	public long getCurrentTimestamp();
	
	
	/**
	 * @return the timestamp which was the endTimestamp in the previous computation and is now the start point (e.g. for differentiating between preserved and inserted)
	 */
	public long getPreviousTimestamp();
}
