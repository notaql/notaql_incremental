package notaql.performance_tests;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import notaql.engines.Engine;
import notaql.engines.EngineService;
import notaql.extensions.dashboard.http.servlets.fromServer.JsonPollFromServerServlet;
import notaql.extensions.notaql.NotaQlWrapper;
import notaql.incremental_tests.Loglevel;
import notaql.model.Transformation;
import notaql.performance_tests.analyser.Analyser;

/**
 * Use this class for encapsulating a performance test.
 */
public abstract class PerformanceTest {
	// Configuration
	public static final String TIMESTAMP_LINE_PREFIX = "TIMESTAMP";
	protected static final String TEST_PREFIX = "performance_" + (System.currentTimeMillis()/1000) + "_";
	private static int NUMBER_OF_EXECUTIONS = 5;
	protected static int DATA_SIZE = 100;
	protected static double DATA_CHANGE_PERCENTAGE = 0.01;


	// Class variables
	private static final Loglevel MINLEVEL = Loglevel.DEBUG;
	private static final DateFormat DATEFORMAT = new SimpleDateFormat("HH:mm:ss");
	
	
	// Object variables
	private PrintWriter logWriter;
	private int executionCounter = 0;
	private String filePathString;
	
	
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
		transformations = NotaQlWrapper.evaluate(query);
			

		JsonPollFromServerServlet.printTextMessages();
		
		return transformations;
	}
	
	
	/**
	 * Logs something with the name of the calling class as prefix using the Loglevel DEBUG.
	 * 
	 * @param o
	 */
	protected final void log(Object o) {
		log(o, Loglevel.DEBUG);
	}
	
	
	/**
	 * Logs something with the name of the calling class as prefix.
	 * 
	 * @param o
	 */
	protected final void log(Object o, Loglevel level) {
		if (level.getLevel() >= MINLEVEL.getLevel()) {
			String string = String.valueOf(o);
			
			if (level.getLevel() >= Loglevel.WARN.getLevel())
				string = string.toUpperCase();
			
			String logString = "[" + DATEFORMAT.format(new Date()) + "] [" + this.getClass().getSimpleName() + "] " + o;
			System.out.println(logString);
			
			// Log to file
			if (logWriter != null) {
				logWriter.print(logString + "\n");
				logWriter.flush();
			}
		}
	}
	
	
	/**
	 * Stores the current timestamp together with a comment into a file.
	 * 
	 * This timestamp can afterwards be used for caluclating performance metrics.
	 * 
	 * @param comment
	 */
	protected final void logCurrentTimestampToFile(String comment) {
		if (logWriter == null)
			throw new IllegalStateException("logWriter is null");

		String logString = TIMESTAMP_LINE_PREFIX + " " + System.currentTimeMillis() + " " + executionCounter + " " + comment;
		System.out.println(logString);

		logWriter.print(logString + "\n");
		logWriter.flush();
	}
	
	
	/**
	 * Executes the test.
	 * 
	 * @throws Exception
	 */
	public final void execute() throws Exception {
		log("Starte Test '" + this.getClass().getSimpleName() + "' (" + NUMBER_OF_EXECUTIONS + " Ausfuehrungen, " + DATA_SIZE + " Datensaetzen, " + DATA_CHANGE_PERCENTAGE*100 + "% Aenderungen)", Loglevel.INFO);

		log("beforeClass()", Loglevel.DEBUG);
		beforeClass();
		
		try {
			do {
				executionCounter++; 
				
				log("setUp()", Loglevel.DEBUG);
				this.setUp();
				
				try {
					log("test()", Loglevel.DEBUG);
					this.test();
				}
				catch (Exception e) {
					e.printStackTrace();
					log("Aborting ...", Loglevel.ERROR);
					break;
				}
				
				finally {
					log("tearDown()", Loglevel.DEBUG);
					this.tearDown();
				}
			} while (executionCounter < NUMBER_OF_EXECUTIONS);
			

			log("analyze()", Loglevel.DEBUG);
			if (filePathString != null)
				Analyser.analyze(filePathString);
		}
		
		finally {
			log("afterClass()", Loglevel.DEBUG);
			afterClass();
			
			log("finished", Loglevel.DEBUG);
		}
	}
	
	
	/**
	 * Prepares the test by class.
	 * 
	 * @throws Exception
	 */
	protected void beforeClass() throws Exception {
		// Logfile
		try {
			this.filePathString = "performancetest_" + DATA_SIZE + "_" + DATA_CHANGE_PERCENTAGE +  "_" + this.getClass().getSimpleName() + ".log";
			logWriter = new PrintWriter(filePathString);
		} catch (FileNotFoundException e) {
			// Could not initialize the logWriter
			e.printStackTrace();
			logWriter = null;
		}
		
		
		// NotaQL
		NotaQlWrapper.init();
		
		
		// Log all available engines
		for (Engine engine : EngineService.getInstance().getEngines())
			log("Engine: " + engine.getEngineName(), Loglevel.DEBUG);
		
		log(EngineService.getInstance().getEngines().size() + " engines", Loglevel.DEBUG); 
	}
	
	
	/**
	 * Prepares the test.
	 * 
	 * @throws Exception
	 */
	protected void setUp() throws Exception {
		// Implement in a subclass
	}
	
	
	/**
	 * The test.
	 * 
	 * @throws Exception
	 */
	protected abstract void test() throws Exception;
	
	
	/**
	 * Clears the test.
	 * 
	 * All side-effects will be cleared.
	 * 
	 * @throws Exception
	 */
	protected void tearDown() throws Exception {
		// Implement in a subclass
	}
	
	
	/**
	 * Clears the test by class.
	 * 
	 * @throws Exception
	 */
	protected void afterClass() throws Exception {
		// Logfile
		logWriter.close();
	}
	
	
	/**
	 * Starts a test
	 * 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Kein Parameter gegeben. Bitte den vollen Namen der Testklasse angeben (z.B. notaql.performance_tests.hbase.sum.PerformanceTestHBaseNormalSnapshot). 2. Parameter ist NUMBER_OF_EXECUTIONS, 3. Parameter ist DATA_SIZE, 4. Parameter ist DATA_CHANGE_PERCENTAGE.");
			System.exit(1);
		}
			
		else {
			try {
				@SuppressWarnings("unchecked")
				Class<PerformanceTest> TestClass = (Class<PerformanceTest>) Class.forName(args[0]);
				
				if (args.length >= 2)
					NUMBER_OF_EXECUTIONS = Integer.parseInt(args[1]);
				
				if (args.length >= 3)
					DATA_SIZE = Integer.parseInt(args[2]);
				
				if (args.length >= 4)
					DATA_CHANGE_PERCENTAGE = Double.parseDouble(args[3]);
				
				PerformanceTest test = TestClass.newInstance();
				test.execute();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				
				System.err.println("Konnte gegebene Klasse nicht finden");
				System.exit(1);
			}
		}
	}
}
