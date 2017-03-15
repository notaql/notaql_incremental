package notaql.extensions.advisor.statistics.container;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import notaql.engines.EngineEvaluator;
import notaql.engines.csv.CSVEngineEvaluator;
import notaql.engines.hbase.HBaseEngineEvaluator;
import notaql.engines.incremental.IncrementalEngineEvaluator;
import notaql.engines.json.JSONEngineEvaluator;
import notaql.engines.mongodb.MongoDBEngineEvaluator;
import notaql.engines.redis.RedisEngineEvaluator;
import notaql.extensions.advisor.statistics.RuntimePredictionResult;
import notaql.extensions.advisor.statistics.Statistic;
import notaql.extensions.advisor.statistics.StatisticCsv;
import notaql.extensions.advisor.statistics.StatisticGeneric;
import notaql.extensions.advisor.statistics.StatisticHbase;
import notaql.extensions.advisor.statistics.StatisticJson;
import notaql.extensions.advisor.statistics.StatisticMongodb;
import notaql.extensions.advisor.statistics.StatisticRedis;
import notaql.extensions.advisor.statistics.exceptions.NoStatisticsException;
import notaql.extensions.notaql.Configuration;
import notaql.model.Transformation;

/**
 * Container for the collected statistics (= case base).
 * 
 * The statistics are only stored in memory and lost when the program is restarted.
 */
public class StatisticsContainerInMemory extends StatisticsContainer {
	// Configuration variables
	private static final long serialVersionUID = 9054618853734783006L;
	private static final Integer SIZE_LIMIT = 1000;
	private static final int APPROX_UNFILTERED_MAX_NUMBER_OF_VALUES_FOR_AVERAGE = 10;
	
	
	
	// Object variables
	protected Map<Class<? extends Statistic>, List<Statistic>> statistics;
	
	
	/**
	 * Creates a new, empty collection.
	 * 
	 * @param configuration
	 */
	public StatisticsContainerInMemory(Configuration configuration) {
		super();
		this.statistics = new HashMap<Class<? extends Statistic>, List<Statistic>>();
	}
	

	/* (non-Javadoc)
	 * @see notaql.extensions.dashboard.advisor.statistics.container.StatisticsContainer#addStatistic(notaql.extensions.dashboard.advisor.statistics.Statistic)
	 */
	@Override
	public boolean addStatistic(Statistic statistic) {
		if (this.statistics.get(statistic.getClass()) == null)
			this.statistics.put(statistic.getClass(), new LinkedList<Statistic>());
		
		List<Statistic> list = this.statistics.get(statistic.getClass());
		list.add(statistic);
		
		if (SIZE_LIMIT != null && list.size() > SIZE_LIMIT) {
			list.remove(0);
			return true;
		}
		else
			return false;
	}
	
	
	/**
	 * Gets the statistics for this engine.
	 * 
	 * @param inEngine
	 * @return
	 */
	public List<Statistic> getStatisticsForEngineEvaluator(EngineEvaluator inEngine) {
		Class<? extends Statistic> statisticClass;
		if (inEngine instanceof HBaseEngineEvaluator)
			statisticClass = StatisticHbase.class;
		else if (inEngine instanceof CSVEngineEvaluator)
			statisticClass = StatisticCsv.class;
		else if (inEngine instanceof JSONEngineEvaluator)
			statisticClass = StatisticJson.class;
		else if (inEngine instanceof MongoDBEngineEvaluator)
			statisticClass = StatisticMongodb.class;
		else if (inEngine instanceof RedisEngineEvaluator)
			statisticClass = StatisticRedis.class;
		else {
			System.err.println("In-Engine " + inEngine.getClass().getSimpleName() + " unterstuetzt keine Statistiken! Verwende allgemeine Sammelklasse (schlechtere Ergebnisse zu erwarten)");
			statisticClass = StatisticGeneric.class;
		}
		

		List<Statistic> statisticsSameEngine = this.statistics.get(statisticClass);
		if (statisticsSameEngine == null)
			return Collections.emptyList();
		else
			return statisticsSameEngine;
	}


	/* (non-Javadoc)
	 * @see notaql.extensions.dashboard.advisor.statistics.container.StatisticsContainer#getApproxNumberOfObjectsUnfiltered(notaql.model.Transformation)
	 */
	@Override
	public long getApproxNumberOfObjectsUnfiltered(Transformation transformation) throws NoStatisticsException {
		// Some databases offer fast running commands for an approximations
		try {
			return super.getApproxNumberOfObjectsUnfiltered(transformation);
		} catch (IllegalArgumentException e) {
			// No fast-running command for this in-engine available
		}
		
		
		// Get the newest number of unfiltered objects from the data
		EngineEvaluator inEngine = transformation.getInEngineEvaluator();  
		List<Statistic> statisticsSameEngine = this.getStatisticsForEngineEvaluator(inEngine);
		ListIterator<Statistic> iterator = statisticsSameEngine.listIterator(statisticsSameEngine.size());
		
		while (iterator.hasPrevious()) {
			Statistic statistic = iterator.previous();
			
			if (statistic.equalsDatabase(inEngine) && statistic.equalsExecutionType(inEngine))
				return statistic.getNumberOfObjectsUnfiltered();
		}
		
		
		throw new NoStatisticsException("Keine passende Statistiken fuer diese Transformation vorhanden (#Unfiltered)");
	}


	/* (non-Javadoc)
	 * @see notaql.extensions.dashboard.advisor.statistics.container.StatisticsContainer#getApproxNumberOfObjectsFiltered(notaql.model.Transformation)
	 */
	@Override
	public long getApproxNumberOfObjectsFiltered(Transformation transformation) throws NoStatisticsException {
		// Get the newest numbers of filtered objects from the data
		List<Long> values = new ArrayList<Long>(APPROX_UNFILTERED_MAX_NUMBER_OF_VALUES_FOR_AVERAGE);
		
		EngineEvaluator inEngine = transformation.getInEngineEvaluator();  
		List<Statistic> statisticsSameEngine = this.getStatisticsForEngineEvaluator(inEngine);
		ListIterator<Statistic> iterator = statisticsSameEngine.listIterator(statisticsSameEngine.size());
		
		while (iterator.hasPrevious()) {
			Statistic statistic = iterator.previous();
			
			if (statistic.equalsDatabase(inEngine) && statistic.equalsExecutionType(inEngine)) {
				values.add(statistic.getNumberOfObjectsFiltered());
				
				if (values.size() == APPROX_UNFILTERED_MAX_NUMBER_OF_VALUES_FOR_AVERAGE)
					break;
			}
		}
		
		
		// Calculate the average
		if (!values.isEmpty())
			return (long) values
	            .stream()
	            .mapToDouble(a -> a)
	            .average()
	            .getAsDouble();
		else
			throw new NoStatisticsException("Keine passende Statistiken fuer diese Transformation vorhanden (#Filtered)");
	}


	/* (non-Javadoc)
	 * @see notaql.extensions.dashboard.advisor.statistics.container.StatisticsContainer#getRuntimePrediction(notaql.model.Transformation)
	 */
	@Override
	public RuntimePredictionResult getRuntimePrediction(Transformation transformation) throws NoStatisticsException {
		if (statistics.isEmpty())
			throw new NoStatisticsException("Keine Statistiken vorhanden");
		
		// Get the parameters
		EngineEvaluator inEngine = transformation.getInEngineEvaluator();
		Map<String, String> inEngineParameters = inEngine.getParamsAsStrings();
		double queryComplexity = Statistic.calculateQueryComplexity(transformation);
		long numberOfObjectsUnfiltered = getApproxNumberOfObjectsUnfiltered(transformation);

		// Check if this is a full recomputation. If this is the case then there will be no filtering of rows based on their delta-value and |unfiltered| equals roughly |filtered|.
		long numberOfObjectsFiltered;
		if (!(inEngine instanceof IncrementalEngineEvaluator))
			numberOfObjectsFiltered = numberOfObjectsUnfiltered;
		else
			numberOfObjectsFiltered = getApproxNumberOfObjectsFiltered(transformation);
		
		
		// Calculate the similarities until 2 reasonable similar cases are found (neweer cases are checked
		// first). Caluclate the similarity over these 2 cases.
		List<Statistic> statisticsSameEngine = this.getStatisticsForEngineEvaluator(inEngine);
		if (!statisticsSameEngine.isEmpty()) {
			// First the local min and max for all distances have to be calculated (so they can later be mapped to [0..1])
			double maxDistanceQueryComplexity = Double.MIN_VALUE;
			long maxDistanceNumberOfObjectsUnfiltered = Long.MIN_VALUE;
			long maxDistanceNumberOfObjectsFiltered = Long.MIN_VALUE;
			
			for (Statistic statistic : statisticsSameEngine) {
				double distanceQueryComplexity = statistic.calculateLocalDistanceQueryComplexity(queryComplexity);
				if (distanceQueryComplexity > maxDistanceQueryComplexity)
					maxDistanceQueryComplexity = distanceQueryComplexity;
				
				long distanceDistanceNumberOfObjectsUnfiltered = statistic.calculateLocalDistanceNumberOfObjectsUnfilterd(numberOfObjectsUnfiltered);
				if (distanceDistanceNumberOfObjectsUnfiltered > maxDistanceNumberOfObjectsUnfiltered)
					maxDistanceNumberOfObjectsUnfiltered = distanceDistanceNumberOfObjectsUnfiltered;
				
				long distanceDistanceNumberOfObjectsFiltered = statistic.calculateLocalDistanceNumberOfObjectsFilterd(numberOfObjectsFiltered);
				if (distanceDistanceNumberOfObjectsFiltered > maxDistanceNumberOfObjectsFiltered)
					maxDistanceNumberOfObjectsFiltered = distanceDistanceNumberOfObjectsFiltered;
			}
			
			
			// Calculate the global similarity until 2 reasonable similar cases are found (newer cases are checked first)
			Statistic mostSimilarStatistic = null;
			double mostSimilarStatisticDistance = Double.MAX_VALUE;
			
			Statistic sndMostSimilarStatistic = null;
			double sndMostSimilarStatisticDistance = Double.MAX_VALUE;

			// Going backwards through the list so that newer statistics are prefered
			ListIterator<Statistic> iterator = statisticsSameEngine.listIterator(statisticsSameEngine.size());
			while (iterator.hasPrevious()) {
				Statistic statistic = iterator.previous();
				double statisticDistance = statistic.calculateGlobalDistance(inEngineParameters, queryComplexity, maxDistanceQueryComplexity, numberOfObjectsUnfiltered, maxDistanceNumberOfObjectsUnfiltered, numberOfObjectsFiltered, maxDistanceNumberOfObjectsFiltered);
								
				// Check if the statistic is relevant for our transformation
				if (statisticDistance < 1) {
					if (statisticDistance < mostSimilarStatisticDistance) {
						// Copy 1. -> 2.
						if (mostSimilarStatistic != null) {
							sndMostSimilarStatistic = mostSimilarStatistic;
							sndMostSimilarStatisticDistance = mostSimilarStatisticDistance;
						}
						
						// Set 1.
						mostSimilarStatistic = statistic;
						mostSimilarStatisticDistance = statisticDistance;
						
						
						// Check if this distance is already below a specified threshold
						if (mostSimilarStatistic != null && isDistanceAcceptable(mostSimilarStatisticDistance) && sndMostSimilarStatistic != null && isDistanceAcceptable(sndMostSimilarStatisticDistance))
							break;
					}
					else if (statisticDistance < sndMostSimilarStatisticDistance) {
						// Set 2.
						sndMostSimilarStatistic = statistic;
						sndMostSimilarStatisticDistance = statisticDistance;
					}
				}
			}
			
			
			// Return a prediction with a similarity (= 1-distance)
			if (mostSimilarStatistic != null) {
				if (sndMostSimilarStatistic == null)
					return new RuntimePredictionResult(mostSimilarStatistic.getRuntimeMillis(), 1-mostSimilarStatisticDistance);
				else {
					long avgRuntime = (mostSimilarStatistic.getRuntimeMillis() + sndMostSimilarStatistic.getRuntimeMillis()) / 2;
					double avgDistance = (mostSimilarStatisticDistance + sndMostSimilarStatisticDistance) / 2;
					return new RuntimePredictionResult(avgRuntime, 1-avgDistance);
				}
			}
		}
		
		
		throw new NoStatisticsException("Keine passende Statistiken fuer diese Transformation vorhanden (" + statisticsSameEngine.size() + " Statistiken fuer diese Engine gesammelt)");
	}
}
