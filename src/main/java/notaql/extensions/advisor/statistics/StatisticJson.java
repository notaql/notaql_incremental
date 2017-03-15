package notaql.extensions.advisor.statistics;

import java.util.Date;
import java.util.Map;

import notaql.engines.EngineEvaluator;
import notaql.engines.json.JSONEngine;
import notaql.engines.json.JSONEngineEvaluator;

public class StatisticJson extends Statistic {
	// Configuration
	private static final long serialVersionUID = -6293114902303545544L;
	
	
	public StatisticJson(long runtimeMillis, Date date, Map<String, String> inEngineParameters, double queryComplexity, long numberOfObjectsUnfiltered, long numberOfObjectsFiltered) {
		super(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
	}
	
	
	/**
	 * @return the path
	 */
	private String getPath() {
		return this.inEngineParameters.get(JSONEngine.PARAMETER_NAME_PATH);
	}


	/* (non-Javadoc)
	 * @see notaql.extensions.advisor.statistics.Statistic#equalsDatabase(notaql.engines.EngineEvaluator)
	 */
	@Override
	public boolean equalsDatabase(EngineEvaluator inEngineEvaluator) {
		return this.getPath().equalsIgnoreCase(((JSONEngineEvaluator) inEngineEvaluator).getPath());
	}


	/* (non-Javadoc)
	 * @see notaql.extensions.advisor.statistics.Statistic#equalsExecutionType(notaql.engines.EngineEvaluator)
	 */
	@Override
	public boolean equalsExecutionType(EngineEvaluator inEngineEvaluator) {
		// This Engine has only one execution type
		return true;
	}
}
