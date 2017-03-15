package notaql.extensions.advisor.statistics;

import java.util.Date;
import java.util.Map;

import notaql.engines.EngineEvaluator;
import notaql.engines.csv.CSVEngine;
import notaql.engines.csv.CSVEngineEvaluator;

public class StatisticCsv extends Statistic {
	// Configuration
	private static final long serialVersionUID = 527174384901069039L;
	
	
	public StatisticCsv(long runtimeMillis, Date date, Map<String, String> inEngineParameters, double queryComplexity, long numberOfObjectsUnfiltered, long numberOfObjectsFiltered) {
		super(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
	}
	
	
	/**
	 * @return the path
	 */
	private String getPath() {
		return this.inEngineParameters.get(CSVEngine.PARAMETER_NAME_PATH);
	}


	/* (non-Javadoc)
	 * @see notaql.extensions.advisor.statistics.Statistic#equalsDatabase(notaql.engines.EngineEvaluator)
	 */
	@Override
	public boolean equalsDatabase(EngineEvaluator inEngineEvaluator) {
		return this.getPath().equalsIgnoreCase(((CSVEngineEvaluator) inEngineEvaluator).getPath());
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
