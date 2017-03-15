package notaql.extensions.advisor.statistics.container;

import java.io.IOException;
import java.io.Serializable;

import com.mongodb.DBCollection;

import notaql.engines.EngineEvaluator;
import notaql.engines.mongodb.MongoApi;
import notaql.engines.mongodb.MongoDBEngineEvaluator;
import notaql.engines.redis.RedisEngineEvaluator;
import notaql.extensions.advisor.statistics.RuntimePredictionResult;
import notaql.extensions.advisor.statistics.Statistic;
import notaql.extensions.advisor.statistics.exceptions.NoStatisticsException;
import notaql.extensions.notaql.Configuration;
import notaql.model.Transformation;
import redis.clients.jedis.Jedis;

/**
 * Container for the collected statistics.
 * 
 * This class shall be implemented by a class with a database backend.
 */
public abstract class StatisticsContainer implements Serializable{
	// Class variables
	private static final long serialVersionUID = 2L;
	
	
	/**
	 * Creates a new collection.
	 * 
	 * If the implementing class is able to load previously stored statistics all Statistics will be loaded.
	 */
	public StatisticsContainer() {
		this(null);
	}
	
	
	/**
	 * Creates a new collection.
	 * 
	 * If the implementing class is able to load previously stored statistics the passed Configuration is used for loading only 
	 * those Statistics which were created with this Configuration.
	 * 
	 * @param configuration
	 */
	public StatisticsContainer(Configuration configuration) {}
	
	
	/**
	 * Adds a statistic to the collection.
	 * 
	 * @param statistic
	 * @return true if this new statistic replaced an old statistic, false if there was still enough space left for the new statistic
	 */
	public abstract boolean addStatistic(Statistic statistic);
	
	
	/**
	 * Calculates a runtime-prediction for the given transformation.
	 * 
	 * @param transformation
	 * @return
	 * @throws NoStatisticsException 
	 */
	public abstract RuntimePredictionResult getRuntimePrediction(Transformation transformation) throws NoStatisticsException;
	
	
	/**
	 * Approximates the number of unfiltered objects for the in-engine of a transformation. 
	 * 
	 * @param transformation
	 * @return
	 * @throws NoStatisticsException
	 */
	public long getApproxNumberOfObjectsUnfiltered(Transformation transformation) throws NoStatisticsException {
		EngineEvaluator inEngine = transformation.getInEngineEvaluator();
		
		// Some databases offer count-commands which run fast
		if (inEngine instanceof RedisEngineEvaluator) {
			try (Jedis jedis = RedisEngineEvaluator.getJedis()) {
				jedis.select((((RedisEngineEvaluator) inEngine).getDatabaseId()));
				return jedis.dbSize();
			}
		}
		
		else if (inEngine instanceof MongoDBEngineEvaluator) {
			try {
				MongoApi mongoApi = null;
				try {
					mongoApi = new MongoApi(((MongoDBEngineEvaluator) inEngine).getHost(), ((MongoDBEngineEvaluator) inEngine).getPort());
					DBCollection dbcollection = mongoApi.getCollection(((MongoDBEngineEvaluator) inEngine).getDatabaseName(), ((MongoDBEngineEvaluator) inEngine).getCollectionName());
					return dbcollection.count();
				} finally {
					if (mongoApi != null)
						mongoApi.close();
				}
			} catch (IOException e) {
				// If this fails the subclass shall implement another variant to calculate an approximation
				e.printStackTrace();
			} 
		}
		
		
		// Probabily has to be implemented in a subclass if this exception is thrown
		throw new IllegalArgumentException("In-Engine " + inEngine.getClass().getSimpleName() + " unterstuetzt keine Statistiken (#Unfiltered)");
	}
	

	/**
	 * Approximates the number of filtered objects for the in-engine of a transformation. 
	 * 
	 * @param transformation
	 * @return
	 * @throws NoStatisticsException
	 */
	public abstract long getApproxNumberOfObjectsFiltered(Transformation transformation) throws NoStatisticsException;
	
	
	/**
	 * Use this method to check whether a specific distance is acceptable (= bellow a specified threshold).
	 * 
	 * @param distance
	 * @return
	 */
	public static final boolean isDistanceAcceptable(double distance) {
		if (distance < 0.01)
			return true;
		else
			return false;
	}
}
