package notaql.extensions.advisor.statistics;

import java.util.Date;
import java.util.Map;

import notaql.engines.EngineEvaluator;
import notaql.engines.hbase.HBaseEngine;
import notaql.engines.hbase.HBaseEngineEvaluator;
import notaql.engines.incremental.snapshot.SnapshotEngineEvaluator;
import notaql.engines.incremental.timestamp.TimestampEngineEvaluator;
import notaql.engines.incremental.trigger.TriggerEngineEvaluator;

public class StatisticHbase extends Statistic {
	// Configuration
	private static final long serialVersionUID = -4687388432784783470L;
	
	
	public StatisticHbase(long runtimeMillis, Date date, Map<String, String> inEngineParameters, double queryComplexity, long numberOfObjectsUnfiltered, long numberOfObjectsFiltered) {
		super(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
	}
	
	
	/**
	 * @return the table id
	 */
	private String getTableId() {
		return this.inEngineParameters.get(HBaseEngine.PARAMETER_NAME_TABLE_ID);
	}
	
	
	/**
	 * @return true if this statistic is snapshot based
	 */
	private boolean isFullRecomputation() {
		return !(this.isSnapshotBased() || this.isTimestampBased() || this.isTriggerBased());
	}
	
	
	/**
	 * @return true if this statistic is timestamp based
	 */
	private boolean isTimestampBased() {
		return isTimestampBased(this.inEngineParameters);
	}
	
	
	/**
	 * @param inEngineParameters
	 * @return true if this statistic is timestamp based
	 */
	private static boolean isTimestampBased(Map<String, String> inEngineParameters) {
		return inEngineParameters.containsKey(HBaseEngine.PARAMETER_NAME_TIMESTAMP)
				&& !isSnapshotBased(inEngineParameters)
				&& !isTriggerBased(inEngineParameters);
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
		return inEngineParameters.containsKey(HBaseEngine.PARAMETER_NAME_SNAPSHOT)
				&& !isTriggerBased(inEngineParameters);
	}
	
	
	/**
	 * @return true if this statistic is trigger based
	 */
	private boolean isTriggerBased() {
		return isTriggerBased(this.inEngineParameters);
	}
	
	
	/**
	 * @param inEngineParameters
	 * @return true if this statistic is trigger based
	 */
	private static boolean isTriggerBased(Map<String, String> inEngineParameters) {
		return inEngineParameters.containsKey(HBaseEngine.PARAMETER_NAME_TRIGGERBASED);
	}


	/* (non-Javadoc)
	 * @see notaql.extensions.advisor.statistics.Statistic#equalsDatabase(notaql.engines.EngineEvaluator)
	 */
	@Override
	public boolean equalsDatabase(EngineEvaluator inEngineEvaluator) {
		return this.getTableId().equalsIgnoreCase(((HBaseEngineEvaluator) inEngineEvaluator).getTableId());
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
		else if (inEngineEvaluator instanceof TimestampEngineEvaluator) {
			if (this.isTimestampBased())
				return true;
		}
		else if (inEngineEvaluator instanceof TriggerEngineEvaluator) {
			if (this.isTriggerBased())
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
	public boolean equalsExecutionType(Map<String, String> inEngineParameters) {
		if (isSnapshotBased(inEngineParameters)) {
			if (this.isSnapshotBased())
				return true;
		}
		else if (isTimestampBased(inEngineParameters)) {
			if (this.isTimestampBased())
				return true;
		}
		else if (isTriggerBased(inEngineParameters)) {
			if (this.isTriggerBased())
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
