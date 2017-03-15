package notaql.extensions.advisor.statistics;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

import notaql.engines.EngineEvaluator;
import notaql.engines.csv.CSVEngineEvaluator;
import notaql.engines.hbase.HBaseEngineEvaluator;
import notaql.engines.json.JSONEngineEvaluator;
import notaql.engines.mongodb.MongoDBEngineEvaluator;
import notaql.engines.redis.RedisEngineEvaluator;
import notaql.extensions.Util;
import notaql.extensions.notaql.NotaQlWrapper;
import notaql.model.Transformation;
import notaql.model.vdata.VData;

/**
 * Statistics regarding the exeuction of a notaql-transformation.
 * 
 * Implement this class in a subclass so the statistics for the different engines can be differentiated.
 */
public abstract class Statistic implements Serializable {
	// Configuration
	private static final long serialVersionUID = 2L;
	
	
	// Weights for the similarity function (sum has to be 1)
	// High weights correspond to a high dependance of the runtime
	// Note that for full recomputations |unfiltered| = |filtered|...
	// The current weights have been determined using trial and error together with some testing
	private static final double weightQueryComplexity = 1.0/3.0;
	private static final double weightNumberOfObjectsUnfiltered = weightQueryComplexity; // Main tasks with the unfiltered data: Load it with spark and set the delta values 
	private static final double weightNumberOfObjectsFiltered = weightQueryComplexity; // Main tasks with the filtered data: Perform the transformations on it and store it with spark

	
	// Output
	private final long runtimeMillis;
	
	// Input
	private final Date date;
	protected final Map<String, String> inEngineParameters;
	private final double queryComplexity;
	private final long numberOfObjectsUnfiltered;
	private final long numberOfObjectsFiltered;
	
	
	public Statistic(long runtimeMillis, Date date, Map<String, String> inEngineParameters, double queryComplexity, long numberOfObjectsUnfiltered, long numberOfObjectsFiltered) {
		if (date == null)
			date = new Date();
		
		if (inEngineParameters == null)
			throw new IllegalArgumentException("inEngineParameters is null");
		
		
		this.runtimeMillis = runtimeMillis;
		this.date = date;
		this.inEngineParameters = inEngineParameters;
		this.queryComplexity = queryComplexity;
		this.numberOfObjectsUnfiltered = numberOfObjectsUnfiltered;
		this.numberOfObjectsFiltered = numberOfObjectsFiltered;
	}


	/**
	 * @return the runtimeMillis
	 */
	public long getRuntimeMillis() {
		return runtimeMillis;
	}


	/**
	 * @return the date
	 */
	public Date getDate() {
		return date;
	}


	/**
	 * @return the inEngineParameters
	 */
	public Map<String, String> getInEngineParameters() {
		return inEngineParameters;
	}


	/**
	 * @return the queryComplexity
	 */
	public double getQueryComplexity() {
		return queryComplexity;
	}


	/**
	 * @return the numberOfObjectsUnfiltered
	 */
	public long getNumberOfObjectsUnfiltered() {
		return numberOfObjectsUnfiltered;
	}


	/**
	 * @return the numberOfObjectsFiltered
	 */
	public long getNumberOfObjectsFiltered() {
		return numberOfObjectsFiltered;
	}
	
	
	/**
	 * Calculates the distance between this Statistic and the given arguments.
	 * 
	 * This class should be overwritten by subclasses and only be comparable within the results of this subclass.
	 * This default implementation is supossed to work always but not deliver the best results (e.g. it ignores if one case was snapshot based and the other one timestamp based).
	 * 
	 * @param inEngineParameters
	 * @param queryComplexity
	 * @param maxDistanceQueryComplexity
	 * @param numberOfObjectsUnfiltered
	 * @param maxDistanceNumberOfObjectsUnfiltered
	 * @param numberOfObjectsFiltered
	 * @param maxDistanceNumberOfObjectsFiltered
	 * @return a number in [0,1] with 0 meaning that they are equal in all relevant parts and 1 meaning that they have nothing in common and using this Statistic is a bad idea.
	 */
	public double calculateGlobalDistance(Map<String, String> inEngineParameters, double queryComplexity, double maxDistanceQueryComplexity, long numberOfObjectsUnfiltered, long maxDistanceNumberOfObjectsUnfiltered, long numberOfObjectsFiltered, long maxDistanceNumberOfObjectsFiltered) {
		if (this.queryComplexity == queryComplexity && this.numberOfObjectsFiltered == numberOfObjectsFiltered && this.numberOfObjectsUnfiltered == numberOfObjectsUnfiltered)
			return 0;
		
		else {
			double distanceQueryComplexity = maxDistanceQueryComplexity == 0 ? 0 : Util.normalize(calculateLocalDistanceQueryComplexity(queryComplexity), 0, maxDistanceQueryComplexity);
			long distanceObjectsUnfilterd = maxDistanceNumberOfObjectsUnfiltered == 0 ? 0 : Util.normalize(calculateLocalDistanceNumberOfObjectsUnfilterd(numberOfObjectsUnfiltered), 0, maxDistanceNumberOfObjectsUnfiltered);
			long distanceObjectsFiltered = maxDistanceNumberOfObjectsFiltered == 0 ? 0 : Util.normalize(calculateLocalDistanceNumberOfObjectsFilterd(numberOfObjectsFiltered), 0, maxDistanceNumberOfObjectsFiltered);
			
			// Weighted average
			return Math.max(0, Math.min(1, Math.sqrt(weightQueryComplexity*Math.pow(distanceQueryComplexity, 2) + weightNumberOfObjectsUnfiltered*Math.pow(distanceObjectsUnfilterd, 2) + weightNumberOfObjectsFiltered*Math.pow(distanceObjectsFiltered, 2))));
		}
	}
	
	
	/**
	 * Local similarity measure.
	 * 
	 * @param queryComplexity
	 * @return
	 */
	public final double calculateLocalDistanceQueryComplexity(double queryComplexity) {
		return Math.abs(queryComplexity - this.queryComplexity);
	}
	
	
	/**
	 * Local similarity measure.
	 * 
	 * @param numberOfObjectsUnfiltered
	 * @return
	 */
	public final long calculateLocalDistanceNumberOfObjectsUnfilterd(long numberOfObjectsUnfiltered) {
		return Math.abs(numberOfObjectsUnfiltered - this.numberOfObjectsUnfiltered);
	}
	
	
	/**
	 * Local similarity measure.
	 * 
	 * @param numberOfObjectsFiltered
	 * @return
	 */
	public final long calculateLocalDistanceNumberOfObjectsFilterd(long numberOfObjectsFiltered) {
		return Math.abs(numberOfObjectsFiltered - this.numberOfObjectsFiltered);
	}


	/**
	 * Creates a new Statistic-object from the given data.
	 * 
	 * @param runtimeMillis
	 * @param transformation
	 * @return
	 */
	public static Statistic create(long runtimeMillis, Transformation transformation) {
		EngineEvaluator inEngine = transformation.getInEngineEvaluator();
		
		// Get the parameters
		Date date = null; // This is new
		Map<String, String> inEngineParameters = inEngine.getParamsAsStrings();
		double queryComplexity = calculateQueryComplexity(transformation);
		long numberOfObjectsUnfiltered = inEngine.getNumberOfObjectsUnfiltered();
		long numberOfObjectsFiltered = inEngine.getNumberOfObjectsFiltered();
		
		
		if (inEngine instanceof HBaseEngineEvaluator)
			return new StatisticHbase(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
		
		else if (inEngine instanceof CSVEngineEvaluator)
			return new StatisticCsv(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
		
		else if (inEngine instanceof JSONEngineEvaluator)
			return new StatisticJson(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
		
		else if (inEngine instanceof MongoDBEngineEvaluator)
			return new StatisticMongodb(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
		
		else if (inEngine instanceof RedisEngineEvaluator)
			return new StatisticRedis(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
		
		else {
			System.err.println("In-Engine " + inEngine.getClass().getSimpleName() + " unterstuetzt keine Statistiken! Verwende allgemeine Sammelklasse (schlechtere Ergebnisse zu erwarten)");
			return new StatisticGeneric(runtimeMillis, date, inEngineParameters, queryComplexity, numberOfObjectsUnfiltered, numberOfObjectsFiltered);
		}
	}
	
	
	/**
	 * Calculates the complexity of the query.
	 * 
	 * @param transformation
	 * @return
	 */
	public static double calculateQueryComplexity(Transformation transformation) {
		return calculateQueryComplexity(transformation.getExpression());
	}
	
	
	/**
	 * Calculates the complexity of the query.
	 * 
	 * The main difference between query is wether there will be only a map-phase
	 * or a map- and a reduce-phase. 
	 * 
	 * Examples:
	 * OUT._r <- IN._r, OUT.$(IN._c) <- OBJECT(o <- SUM(IN._v))   = 1
	 * OUT._r <- IN._r, OUT.$(IN._c) <- SUM(IN._v)   			  = 1
	 * OUT._r <- IN._r, OUT.$(IN._c) <- IN._v        			  = 0
	 * OUT.x <- 1												  = 0
	 * 
	 * @param transformation
	 * @return
	 */
	private static int calculateQueryComplexity(VData queryExpression) {
		return notaql.engines.incremental.util.Util.isQueryAggregating(queryExpression) ? 1 : 0;
	}


	/**
	 * Checks if this statistic and the database of the inEngineEvaluator are the same (e.g. same csv-file, same hbase-table, ...)
	 * 
	 * @param inEngineEvaluator
	 * @return
	 */
	public abstract boolean equalsDatabase(EngineEvaluator inEngineEvaluator);


	/**
	 * Checks if this statistic and the type of the inEngineEvaluator belong to the same execution type (e.g. both are timestamp based)
	 * 
	 * @param inEngineEvaluator
	 * @return
	 */
	public abstract boolean equalsExecutionType(EngineEvaluator inEngineEvaluator);
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Statistic [runtimeMillis=" + runtimeMillis + ", date=" + date + ", inEngineParameters="
				+ inEngineParameters + ", queryComplexity=" + queryComplexity + ", numberOfObjectsUnfiltered="
				+ numberOfObjectsUnfiltered + ", numberOfObjectsFiltered=" + numberOfObjectsFiltered + "]";
	}
	
	
	public static void main(String[] args ) throws Exception {
		String query = "IN-ENGINE: hbase(table_id <- 'test_in', timestamp <- '20'), OUT-ENGINE: hbase(table_id <- 'test_out'),OUT._r <- IN._r, OUT.$(IN._c) <- SUM(IN._v)";
		Transformation transformation = NotaQlWrapper.parse(query).get(0);
		
		System.out.println(Statistic.calculateQueryComplexity(transformation));
	}
}
