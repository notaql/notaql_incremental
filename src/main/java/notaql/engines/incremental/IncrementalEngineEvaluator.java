package notaql.engines.incremental;

/**
 * Marks an engine evaluator as being used for incremental computations.
 * 
 * Currently incremental computations have the following limitations:
 * 
 * 1.) Only SUM() and COUNT() are implemented as aggregation functions. Using other functions (including +*-/ ...) will not result in a correct result
 * until they are implemented (they have to handle the multiple values for the same cell (flagged as preserved, deleted, inserted or a combination of the last 2)).
 */
public abstract interface IncrementalEngineEvaluator {
    /**
     * @return true if the user expects updates inside the database. may be null.
     */
    public Boolean userExpectsUpdates();

    
    /**
     * @return true if the user expects deletes inside the database. may be null.
     */
    public Boolean userExpectsDeletes();
    
    
	/**
	 * @return true if this engine supports updates of previous inserts
	 */
	public boolean supportsUpdates();
	
	
	/**
	 * @return true if this engine supports deletes
	 */
	public boolean supportsDeletes();
    
    
    /**
     * @return true if the expected operations and the supported operations are compatible, false otherwise (e.g. no delete support but deletes are expected)
     */
    public default boolean expectedOperationsAndSupportedOperationsCompatible() {
    	if ((userExpectsUpdates() != null && userExpectsUpdates() && !supportsUpdates())
    			|| (userExpectsDeletes() != null && userExpectsDeletes() && !supportsDeletes()))
    		return false;
    	
    	else
    		return true;
    }
}
