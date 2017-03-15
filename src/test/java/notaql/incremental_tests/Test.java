package notaql.incremental_tests;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import notaql.NotaQL;
import notaql.engines.Engine;
import notaql.engines.EngineService;
import notaql.extensions.advisor.statistics.RuntimePredictionResult;
import notaql.extensions.advisor.statistics.exceptions.NoStatisticsException;
import notaql.extensions.dashboard.http.servlets.fromServer.JsonPollFromServerServlet;
import notaql.extensions.notaql.NotaQlWrapper;
import notaql.model.Transformation;

public abstract class Test {
	class RuntimePredictionTestResult {
		public final Long prediction;
		public final long result;
		public final String query;
		public final Double similarity;
		
		public RuntimePredictionTestResult(String query, Long prediction, long result, Double similarity) {
			this.prediction = prediction;
			this.result = result;
			this.query = query;
			this.similarity = similarity;
		}
		
		public String toString() {
			String diff = this.prediction != null ? " | Diff: " + (this.result-this.prediction) : "" ;
			String similarityString = this.similarity != null ? " | Similarity: " + ((int) (this.similarity*100)) + "%" : "";
			return "Prediction: " + this.prediction + " | Result: " + this.result + diff + similarityString + " | " +  this.query;
		}
	}
	
	
	// Configuration
	protected static final String TEST_PREFIX = "junit_" + (System.currentTimeMillis()/1000) + "_";
	private static final boolean testRuntimePrediction = false;


	// Class variables
	public static final Loglevel MINLEVEL = Loglevel.DEBUG;
	public static final DateFormat DATEFORMAT = new SimpleDateFormat("HH:mm:ss");
	private static List<RuntimePredictionTestResult> testRuntimePredicitionResults;
	
	
	/**
	 * Sets up NotaQL
	 */
	@BeforeClass
	public static void beforeClass() throws Exception {
		logStatic(Test.class, "beforeClass()", Loglevel.DEBUG);
		
		// NotaQL
		NotaQlWrapper.init();
		
		
		// Log all available engines
		for (Engine engine : EngineService.getInstance().getEngines())
			logStatic(Test.class, "Engine: " + engine.getEngineName(), Loglevel.DEBUG);
		
		logStatic(Test.class, EngineService.getInstance().getEngines().size() + " engines", Loglevel.DEBUG);
		
		
		// Prepare Spark
		if (testRuntimePrediction)
			testRuntimePredicitionResults = new ArrayList<RuntimePredictionTestResult>();
	}
	
	
	/**
	 * Shutdown
	 */
	@AfterClass
	public static void afterClass() throws Exception {
		logStatic(Test.class, "afterClass()", Loglevel.DEBUG);
		
		if (testRuntimePrediction) {
			long totalRuntime = 0;
			long totalDiff = 0;
			List<Integer> diffPercs = new ArrayList<Integer>(testRuntimePredicitionResults.size());
			
			// Ignore the first result (spark was not started)
			for (RuntimePredictionTestResult result : testRuntimePredicitionResults) {
				if (result.prediction != null) {
					totalRuntime += result.result;
					long diff = Math.abs(result.result - result.prediction);
					totalDiff += diff;
					
					diffPercs.add((int) ((((double) diff/result.result))*100));
				}
				
				System.out.println(result);
			}
			
			if (totalDiff > 0) {
				Integer[] diffsPercsArr = diffPercs.toArray(new Integer[diffPercs.size()]);
				Arrays.sort(diffsPercsArr);
				System.out.println("Total difference from real runtimes: "  + totalDiff + " ms (" + (int) ((((double) totalDiff)/totalRuntime)*100) + "%)... Median: " + diffsPercsArr[diffsPercsArr.length/2] + "%");
			}
		}
	}
	
	
	/**
	 * Evaluates the query with NotaQl.
	 * 
	 * @param query
	 * @return the transformations
	 * @throws IOException 
	 */
	protected List<Transformation> notaqlEvaluate(String query) throws IOException {
		log("notaqlEvaluate('" + query + "')", Loglevel.DEBUG);
		
		List<Transformation> transformations;
		if (!testRuntimePrediction)
			transformations = NotaQlWrapper.evaluate(query);
		else {
			// Pre-execution prediction
			Transformation transformation = NotaQL.parse(query).get(0);
			
			Double similarity = null;
			Long runtimePrediction = null;
			try {
				RuntimePredictionResult prediction = NotaQlWrapper.getRuntimePrediction(transformation);
				runtimePrediction = prediction.runtimeMillis;
				similarity = prediction.similarity;
			} catch (NoStatisticsException e) {
				e.printStackTrace();
			}
					

			// Execution
			final long starttime = System.currentTimeMillis();
			transformations = NotaQlWrapper.evaluateAndCollectStatistics(query);
			final long runtimeResult = System.currentTimeMillis() - starttime;
			
			testRuntimePredicitionResults.add(new RuntimePredictionTestResult(query, runtimePrediction, runtimeResult, similarity));
		}
			

		JsonPollFromServerServlet.printTextMessages();
		
		return transformations;
	}
	
	
	/**
	 * Logs something with the name of the calling class as prefix using the Loglevel DEBUG.
	 * 
	 * @param o
	 */
	protected void log(Object o) {
		log(o, Loglevel.DEBUG);
	}
	
	
	/**
	 * Logs something with the name of the calling class as prefix.
	 * 
	 * @param o
	 */
	protected void log(Object o, Loglevel level) {
		logStatic("[" + this.getClass().getSimpleName() + "] " + o, level);
	}
	
	
	/**
	 * Logs something with the name of the calling class as prefix.
	 * 
	 * @param callingClass
	 * @param o
	 */
	protected static void logStatic(@SuppressWarnings("rawtypes") Class callingClass, Object o, Loglevel level) {
		logStatic("[" + callingClass.getSimpleName() + "] " + o, level);
	}
	
	
	/**
	 * Logs something.
	 * 
	 * @param o
	 */
	private static void logStatic(Object o, Loglevel level) {
		if (level.getLevel() >= MINLEVEL.getLevel()) {
			String string = String.valueOf(o);
			
			if (level.getLevel() >= Loglevel.WARN.getLevel())
				string = string.toUpperCase();
			
			System.out.println("[" + DATEFORMAT.format(new Date()) + "] " + o);
		}
	}
}
