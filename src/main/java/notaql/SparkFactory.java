package notaql;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Used for providing an access to Spark.
 */
public class SparkFactory {
	final private static String SPARK_APP_NAME = "NotaQL";
	final private static String MEMORY_IN_LOCAL_MODE = "1G";
	
    private static JavaSparkContext javaSparkContext = null;
    
    
    /**
     * Kills the current JavaSparkContext. This will also kill all currently running transformations (which terminate with an IllegalStateException)
     */
    public static synchronized void stop() {
    	if (javaSparkContext != null) {
    		try {
    			javaSparkContext.cancelAllJobs();
    		} catch (Exception e) {}
    		
    		try {
    			javaSparkContext.stop();
    		} catch (Exception e) {}
    		
    		try {
    			javaSparkContext.close();
    		} catch (Exception e) {}
    		
    		javaSparkContext = null;
    	}	
    }
    
    
    /**
     * Use this method to check if the spark context was already initialized or if the runtime will be longer because it wasn't.
     * 
     * @return true if the spark context was already initialized
     */
    public static boolean isSparkInitialized() {
    	return javaSparkContext != null;
    }
    
    
    /**
     * Creates an empty RDD
     * 
     * @return
     */
    public static <T> JavaRDD<T> createEmptyRDD() {
        JavaSparkContext spark = SparkFactory.getSparkContext();
        return spark.emptyRDD();
    }
    

    /**
     * @return access to Spark via a JavaSparkContext
     */
    public static synchronized JavaSparkContext getSparkContext() {    	
        if (javaSparkContext != null)
            return javaSparkContext;

        
        // Init the spark-configuration
        final String sparkMaster = NotaQL.getProperties().getProperty("spark_master", "local");
    	System.out.println("Starting Spark '" + SPARK_APP_NAME + "', master is '" + sparkMaster + "'");
        SparkConf sparkConfiguration = new SparkConf()
        		.setAppName(SPARK_APP_NAME)
        		.setMaster(sparkMaster);
        
        if (sparkMaster.equals("local")) {
        	System.out.println("Setting memory to " + MEMORY_IN_LOCAL_MODE);
        	sparkConfiguration.set("spark.executor.memory", MEMORY_IN_LOCAL_MODE);
        	sparkConfiguration.set("spark.driver.memory", MEMORY_IN_LOCAL_MODE);
        }
        
        if (NotaQL.getProperties().getProperty("parallelism") != null)
            sparkConfiguration.set("spark.default.parallelism", NotaQL.getProperties().getProperty("parallelism"));
        
        
        if (NotaQL.getProperties().getProperty("kryo") != null && NotaQL.getProperties().getProperty("kryo").equalsIgnoreCase("true")) {
        	System.out.println("Using Kyro");
            sparkConfiguration.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }

        
        // Init the spark-context
        final SparkContext sparkContext = new SparkContext(sparkConfiguration);

        
        // TODO: Add progress/timing magic here!
        /*sparkContext.addSparkListener(new SparkListener() {
            @Override
            public void onStageCompleted(SparkListenerStageCompleted sparkListenerStageCompleted) {
                System.out.println("Stage completed!");
            }

            @Override
            public void onStageSubmitted(SparkListenerStageSubmitted sparkListenerStageSubmitted) {
                System.out.println("Stage submitted!");
            }

            @Override
            public void onTaskStart(SparkListenerTaskStart sparkListenerTaskStart) {
                System.out.println("Task started!");
            }

            @Override
            public void onTaskGettingResult(SparkListenerTaskGettingResult sparkListenerTaskGettingResult) {
                System.out.println("Task getting Result!");
            }

            @Override
            public void onTaskEnd(SparkListenerTaskEnd sparkListenerTaskEnd) {
                System.out.println("Task end!");
            }

            @Override
            public void onJobStart(SparkListenerJobStart sparkListenerJobStart) {
                System.out.println("Job start!");
            }

            @Override
            public void onJobEnd(SparkListenerJobEnd sparkListenerJobEnd) {
                System.out.println("Job end!");
            }

            @Override
            public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate sparkListenerEnvironmentUpdate) {
                System.out.println("Env update!");
            }

            @Override
            public void onBlockManagerAdded(SparkListenerBlockManagerAdded sparkListenerBlockManagerAdded) {
                System.out.println("block manager added!");
            }

            @Override
            public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved sparkListenerBlockManagerRemoved) {
                System.out.println("block manager removed!");
            }

            @Override
            public void onUnpersistRDD(SparkListenerUnpersistRDD sparkListenerUnpersistRDD) {
                System.out.println("unpersist rdd!");
            }

            @Override
            public void onApplicationStart(SparkListenerApplicationStart sparkListenerApplicationStart) {
                System.out.println("app start!");
            }

            @Override
            public void onApplicationEnd(SparkListenerApplicationEnd sparkListenerApplicationEnd) {
                System.out.println("app end!");
            }

            @Override
            public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate sparkListenerExecutorMetricsUpdate) {
                System.out.println("metrics update!");
            }
        });*/

        
        
        // Cache the context
        javaSparkContext = new JavaSparkContext(sparkContext);

        
        /*
         * Upload the dependencies to Spark
         * 
         * This jar has to be build using the `mvn package` command (the big jar file with all dependencies)
         */
        final String jarPath = NotaQL.getProperties().getProperty("dependency_jar");
        
        
        if (jarPath == null || jarPath.startsWith("/PATH/TO/BUILD/"))
        	System.out.println("dependency_jar is not specified... Ignoring the dependencies will probably not work.");
        
        else {
            File datamodelJar = new File(jarPath);
            
            if (datamodelJar.exists())
            	// local file
            	javaSparkContext.addJar(datamodelJar.getAbsolutePath());
            else
            	// something else ...
            	javaSparkContext.addJar(jarPath);
        }

        
        return javaSparkContext;
    }
}