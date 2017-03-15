package notaql.extensions.advisor.statistics;

import java.util.Date;
import java.util.Map;

import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.snapshot.SnapshotEngineEvaluator;
import notaql.engines.redis.RedisEngine;
import notaql.engines.redis.RedisEngineEvaluator;

public class StatisticRedis extends Statistic {
	// Configuration
	private static final long serialVersionUID = -8678991430427971695L;
	
	
	public StatisticRedis(long runtimeMillis, Date date, Map<String, String> inEngineParameters, double queryComplexity, long numberOfObjectsUnfiltered, long numberOfObjectsFiltered) {
		super(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
	}
	
	
	/**
	 * @return the database-id
	 */
	private int getDatabaseId() {
		return Integer.valueOf(this.inEngineParameters.get(RedisEngine.PARAMETER_NAME_DATABASE_ID));
	}
	
	
	/**
	 * @return true if this statistic is snapshot based
	 */
	private boolean isFullRecomputation() {
		return !this.isSnapshotBased();
	}
	
	
	/**
	 * @return true if this statistic is snapshot based
	 */
	private boolean isSnapshotBased() {
		return isSnapshotBased(this.inEngineParameters);
	}
	
	
	/**
	 * @param inEngineParameters
	 * @return true if this statistic is snapshot based
	 */
	private static boolean isSnapshotBased(Map<String, String> inEngineParameters) {
		return inEngineParameters.containsKey(RedisEngine.PARAMETER_NAME_SNAPSHOT);
	}


	/* (non-Javadoc)
	 * @see notaql.extensions.advisor.statistics.Statistic#equalsDatabase(notaql.engines.EngineEvaluator)
	 */
	@Override
	public boolean equalsDatabase(EngineEvaluator inEngineEvaluator) {
		return this.getDatabaseId() == ((RedisEngineEvaluator) inEngineEvaluator).getDatabaseId();
	}


	/* (non-Javadoc)
	 * @see notaql.extensions.advisor.statistics.Statistic#equalsExecutionType(notaql.engines.EngineEvaluator)
	 */
	@Override
	public boolean equalsExecutionType(EngineEvaluator inEngineEvaluator) {
		if (inEngineEvaluator instanceof SnapshotEngineEvaluator) {
			if (this.isSnapshotBased())
				return true;
		}
		else {
			if (this.isFullRecomputation())
				return true;
		}
		
		return false;
	}
	
	
	/**
	 * @param inEngineParameters
	 * @return true if this has the same execution type
	 */
	private boolean equalsExecutionType(Map<String, String> inEngineParameters) {
		if (isSnapshotBased(inEngineParameters)) {
			if (this.isSnapshotBased())
				return true;
		}
		else {
			if (this.isFullRecomputation())
				return true;
		}
		
		return false;
	}
	

	/* (non-Javadoc)
	 * @see notaql.extensions.advisor.statistics.Statistic#calculateGlobalDistance(java.util.Map, double, double, long, long, long, long)
	 */
	@Override
	public double calculateGlobalDistance(Map<String, String> inEngineParameters, double queryComplexity, double maxDistanceQueryComplexity, long numberOfObjectsUnfiltered, long maxDistanceNumberOfObjectsUnfiltered, long numberOfObjectsFiltered, long maxDistanceNumberOfObjectsFiltered) {
		// Check if the execution types differ and return the max distance if this is the case
		if (!this.equalsExecutionType(inEngineParameters))
			return 1;
		
		else
			return super.calculateGlobalDistance(inEngineParameters, queryComplexity, maxDistanceQueryComplexity, numberOfObjectsUnfiltered, maxDistanceNumberOfObjectsUnfiltered, numberOfObjectsFiltered, maxDistanceNumberOfObjectsFiltered);
	}
}
