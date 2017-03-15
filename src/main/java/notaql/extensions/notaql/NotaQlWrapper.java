package notaql.extensions.notaql;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import notaql.NotaQL;
import notaql.SparkFactory;
import notaql.extensions.advisor.statistics.RuntimePredictionResult;
import notaql.extensions.advisor.statistics.Statistic;
import notaql.extensions.advisor.statistics.container.StatisticsContainer;
import notaql.extensions.advisor.statistics.container.StatisticsContainerFile;
import notaql.extensions.advisor.statistics.container.StatisticsContainerInMemory;
import notaql.extensions.advisor.statistics.exceptions.NoStatisticsException;
import notaql.extensions.dashboard.http.servlets.fromServer.JsonPollFromServerServlet;
import notaql.model.Transformation;

/**
 * Encapsulates NotaQL, which currently is supposed to be called on the command line (e.g. pass the config as a parameter).
 */
public class NotaQlWrapper {
	// Class variables
	private static Boolean isNotaqlInitialized = false;
	private static StatisticsContainer statisticsContainer;
	private static Configuration configuration = new Configuration();
	

	/**
	 * @return a statistic container
	 */
	private static StatisticsContainer getStatisticsContainer() {
		if (statisticsContainer == null)
			try {
				statisticsContainer = new StatisticsContainerFile(configuration);
			} catch (IOException e) {
				e.printStackTrace();
				JsonPollFromServerServlet.addMessage("Could not create the statistics container... Using in-memory container");
				statisticsContainer = new StatisticsContainerInMemory(configuration);
			}
		
		return statisticsContainer;
	}
	

	/**
	 * Inits everything.
	 * 
	 * @throws IOException
	 */
	public static void init() throws IOException {
		synchronized (isNotaqlInitialized) {
			if (!isNotaqlInitialized) {
				// Load the config
				loadConfig();
				
				isNotaqlInitialized = true;
			}
		}
	}

	
	/**
	 * Gets the loaded config
     * 
     * @return Properties
	 * @throws IOException 
	 */
	public static Map<String, String> getConfig() throws IOException {
		init();
		
		// Generate the map to return
		Properties config = NotaQL.getConfig();
		
		Map<String, String> configMap = new HashMap<String, String>();
		for (String key : config.stringPropertyNames())
			configMap.put(key, config.getProperty(key));
		
		return configMap;
	}
	
	
	/**
	 * Wrapper for the loadConfig() method from NotaQl.
	 * 
	 * @throws IOException
	 */
	private static void loadConfig() throws IOException {
		InputStream inputStream = null;
		
		try { 
			inputStream = configuration.getStream();
			NotaQL.loadConfig(inputStream);
		} finally {
			if (inputStream != null)
				inputStream.close();
		}
	}


	/**
	 * Changes the config and reloads it into the NotaQl-processor
	 * 
	 * @param configMap
	 * @throws IllegalArgumentException if the configMap didn't contain all keys from the original config
	 * @throws IOException 
	 */
	public static void setConfig(Map<String, String> configMap) throws IllegalArgumentException, IOException {
		// Check if the new config is valid
		for (String originalKey : getConfig().keySet())
			if (!configMap.containsKey(originalKey))
				throw new IllegalArgumentException("Key '" + originalKey + "' is missing in the new configuration");
		
		
		// Store and load the config
		configuration.setConfig(configMap);
		loadConfig();
		
		
		// Invalidate the statistics container (it is based on the configuration because it has an impact on the runtimes) 
		statisticsContainer = null;
	}
	
	
	/**
	 * Calculates a runtime-prediction for the given transformation.
	 * 
	 * @param transformation
	 * @return predicted runtime in milliseconds
	 * @throws FileNotFoundException 
	 * @throws NoStatisticsException 
	 */
	public static RuntimePredictionResult getRuntimePrediction(Transformation transformation) throws NoStatisticsException {
		return getStatisticsContainer().getRuntimePrediction(transformation);
	}


	/**
	 * Wrapper for the evalue() method from NotaQl.
	 * 
	 * Additionaly collects statistics to be used for predicting the runtime of future querys.
	 * 
	 * @param script
	 * @return the transformations from the script
	 * @throws IOException
	 */
	public static List<Transformation> evaluateAndCollectStatistics(String script) throws IOException {
		List<Transformation> transformations = NotaQlWrapper.parse(script);
		evaluateAndCollectStatistics(transformations);
		
		return transformations;
	}


	/**
	 * Wrapper for the evalue() method from NotaQl.
	 * 
	 * Additionaly collects statistics to be used for predicting the runtime of future querys.
	 * 
	 * @param script
	 * @throws IOException
	 */
	public static void evaluateAndCollectStatistics(Transformation transformation) throws IOException {
		evaluateAndCollectStatistics(Arrays.asList(transformation));
	}


	/**
	 * Wrapper for the evalue() method from NotaQl.
	 * 
	 * Additionaly collects statistics to be used for predicting the runtime of future querys.
	 * 
	 * @param script
	 * @throws IOException
	 */
	public static void evaluateAndCollectStatistics(List<Transformation> transformations) throws IOException {
		// Check if spark will ruin our runtime statistic
		boolean collectStatistics = true;
		if (!SparkFactory.isSparkInitialized())
			collectStatistics = false;
		
		
		// Evaluate the transformations
		final long starttime = System.currentTimeMillis();
		for (Transformation transformation : transformations)
			NotaQlWrapper.evaluate(transformation);
		
		
		// Collect the statistics
		if (collectStatistics) {
			final long runtimeMillis = System.currentTimeMillis() - starttime;
			
			if (transformations.size() == 1) {
				try {
					getStatisticsContainer().addStatistic(Statistic.create(runtimeMillis, transformations.get(0)));
				} catch (IllegalArgumentException e) {
					// Don't throw errors associated with the statistics... They are not critical to the user, only to a developer.
					JsonPollFromServerServlet.addMessage(e.getMessage());
					e.printStackTrace();
				}
			}
		}
	}


	/**
	 * Wrapper for the parse() method from NotaQl
	 * 
	 * @param script
	 * @return the transformations from the script
	 * @throws IOException
	 */
	public static List<Transformation> parse(String script) throws IOException {
		init();
		return NotaQL.parse(script);
	}


	/**
	 * Wrapper for the evalue() method from NotaQl
	 * 
	 * @param script
	 * @return the transformations from the script
	 * @throws IOException
	 */
	public static List<Transformation> evaluate(String script) throws IOException {
		init();
		return NotaQL.evaluate(script);
	}


	/**
	 * Wrapper for the evaluateTransformation() method from NotaQl
	 * 
	 * @param transformation
	 * @throws IOException
	 */
	public static void evaluate(Transformation transformation) throws IOException {
		init();
		NotaQL.evaluateTransformation(transformation);
	}


	/**
	 * Wrapper for the evaluateTransformation() method from NotaQl to be used by trigger-based Evaluators
	 * 
	 * @param transformation
	 * @param data_new
	 * @param data_old
	 */
	public static void evaluateTransformationTrigger(Transformation transformation, String data_new, String data_old) throws IOException {
		init();
		NotaQL.evaluateTransformationTrigger(transformation, data_new, data_old);
	}


	
	/**
	 * Kills all spark jobs
	 */
	public static void killAll() {
		NotaQL.killAllEvaluations();
	}
}
