/*
 * Copyright 2015 by Thomas Lottermann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package notaql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.spark.api.java.JavaRDD;

import notaql.model.EvaluationException;

import notaql.datamodel.ObjectValue;
import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.IncrementalEngineEvaluator;
import notaql.engines.incremental.resultcombiner.CombiningEngineEvaluator;
import notaql.engines.incremental.trigger.TriggerEngineEvaluator;
import notaql.engines.incremental.util.Util;
import notaql.model.NotaQLExpression;
import notaql.model.Transformation;
import notaql.parser.NotaQLExpressionParser;

/**
 * The NotaQL main class. Here is where the magic starts.
 */
public class NotaQL {
    private static Properties properties;
    private static boolean propertiesLoaded = false;
    private static int currentJobGroup = 0;

    /**
     * This is a simple program which expects a NotaQL query to execute.
     * It also allows you to specify a config file.
     *
     * The config file is sometimes necessary to provide information about the whereabouts of databases.
     *
     * @param args
     * @throws IOException
     */
    public static void main(String... args) throws IOException {
        BasicConfigurator.configure();

        // Check if the arguments are missing
        if (args.length < 1)
            throw new IllegalArgumentException("The input must be provided as follows: [--config=PATH/TO/settings.config] notaql_query");

        
        // Load the config
        if (args[0].startsWith("--config=")) {
            loadConfig(args[0].substring(9));
        }

        
        // Load the notaql_query
        final StringBuilder sbNotaqlQuery = new StringBuilder();
        for(int i = (propertiesLoaded ? 1 : 0); i < args.length; i++) {
            sbNotaqlQuery.append(args[i]);
            if(i < args.length-1)
                sbNotaqlQuery.append(" ");
        }

        
        // Pass the notaql_query to the evaluator-method
        evaluate(sbNotaqlQuery.toString());
    }

    
    /**
     * Load the config file and store it in prop
     *
     * @param path
     * @throws IOException
     */
    public static void loadConfig(String path) throws IOException {
    	InputStream stream = null;
    	
    	// Load the config
    	try {
    		// Get the stream
    		try {
    			stream = new FileInputStream(new File(path));
    		} catch(FileNotFoundException e) {
    			stream = NotaQL.class.getClassLoader().getResourceAsStream(path);
    		}

    		if(stream == null) {
    			throw new FileNotFoundException("Unable to find " + path + " in classpath.");
    		}

    		
    		// load the properties
    		loadConfig(stream);

    	} finally {
    		// Always close the stream if any
    		if (stream != null)
    			stream.close();
		}
    }


	public static void loadConfig(InputStream stream) throws IOException {
		// load the properties
    	if (properties == null || propertiesLoaded)
    		properties = new Properties();
    	properties.load(stream);
    	
    	
    	// remember the successful loading
        propertiesLoaded = true;
	}
    
    
    /**
     * Gets the loaded config
     * 
     * @return Properties
	 * @throws IllegalStateException when the config was not loaded 
     */
    public static Properties getConfig() throws IllegalStateException {
    	if (!propertiesLoaded)
    		throw new IllegalStateException("Call loadConfig() first");
    	
    	return properties;
    }

    
    /**
     * Parses a query into the Notaql datamodel.
     *
     * @param query
     * @throws ConnectException
     * @return a list with all executed transformations 
     */
    public static List<Transformation> parse(String query) throws ConnectException {
    	return evaluate(query, true);
    }

    
    /**
     * Evaluates a query, transformation after transformation.
     *
     * @param query
     * @throws ConnectException
     * @return a list with all executed transformations 
     */
    public static List<Transformation> evaluate(String query) throws ConnectException {
    	return evaluate(query, false);
    }

    
    /**
     * Evaluates a query, transformation after transformation.
     *
     * @param query
     * @param isOnlyParsing set to true if you just want the parsed queries which shall not be evaluated
     * @throws ConnectException
     * @return a list with all executed transformations 
     */
    private static List<Transformation> evaluate(String query, boolean isOnlyParsing) throws ConnectException {
    	// Parse the query
        final NotaQLExpression expression = NotaQLExpressionParser.parse(query);
        List<Transformation> transformations = expression.getTransformations();
        
        
        // If this is a trigger based transformation only return the transformation and don't evaluate anything
        // => The calling method has to handle the execution of the transformations
        if (!isOnlyParsing && (!(transformations.size() == 1 && transformations.get(0).getInEngineEvaluator() instanceof TriggerEngineEvaluator))) {
        	// Perform all transformations from the query
	        for (Transformation transformation : transformations) 
	        	evaluateTransformation(transformation);
        }
        
        
        return transformations;
    }
    
    
    /**
     * Executes a transformation (= reading the input and storing the output)
     * 
     * @param transformation
     */
    public static void evaluateTransformation(Transformation transformation) {
    	// If the job group changes no further actions will be performed 
    	int evaluationJobGroup = currentJobGroup;


    	// Get the correct engines
    	final EngineEvaluator inEngineEvaluator = transformation.getInEngineEvaluator();


    	// Generate the result
    	JavaRDD<ObjectValue> result = null;
    	if (evaluationJobGroup == currentJobGroup)
    		result = inEngineEvaluator.evaluate(transformation);

    	
    	// Store the result
    	evaluateTransformationStore(transformation, evaluationJobGroup, result);
    }
    
    
    /**
     * Executes a transformation (= reading the input and storing the output).
     * 
     * To be used with trigger-based evaluations.
     * 
     * @param transformation
     * @param data_new
     * @param data_old
     */
    public static void evaluateTransformationTrigger(Transformation transformation, String data_new, String data_old) {
    	// If the job group changes no further actions will be performed 
    	int evaluationJobGroup = currentJobGroup;


    	// Get the correct engines
    	final EngineEvaluator inEngineEvaluator = transformation.getInEngineEvaluator();
    	
    	
    	// Check if this is really a trigger-based engine
    	if (!(inEngineEvaluator instanceof TriggerEngineEvaluator))
    		throw new IllegalArgumentException("IN-ENGINE is not trigger-based");
    		

    	// Generate the result
    	JavaRDD<ObjectValue> result = null;
    	if (evaluationJobGroup == currentJobGroup)
    		result = ((TriggerEngineEvaluator) inEngineEvaluator).evaluate(transformation, data_new, data_old);

    	
    	// Store the result
    	evaluateTransformationStore(transformation, evaluationJobGroup, result);

    }
    
    
    /**
     * @param transformation
     * @param evaluationJobGroup
     */
    private static void evaluateTransformationStore(Transformation transformation, int evaluationJobGroup, JavaRDD<ObjectValue> result) {
    	final EngineEvaluator inEngineEvaluator = transformation.getInEngineEvaluator();
    	final EngineEvaluator outEngineEvaluator = transformation.getOutEngineEvaluator();
    	
    	
    	// Synchronized to avoid lost updates with transformations running in threads
    	synchronized (outEngineEvaluator) {
    		// If this is a timestamp based transformation => merge the previous result and the new result
	    	if (evaluationJobGroup == currentJobGroup) {
	    		if (inEngineEvaluator instanceof IncrementalEngineEvaluator) {
	    			// Check if the oldresult is needed
	    			// If there were no aggregating functions (e.g. only copying the values) we don't need the previous result 
	    			if (!result.isEmpty() && Util.isQueryAggregating(transformation.getExpression())) {
		    			if (!(outEngineEvaluator instanceof CombiningEngineEvaluator))
		    				throw new EvaluationException("IN-engine is incremental but OUT-engine does not provide combining capabilities");
		
		    			CombiningEngineEvaluator outCombiningEngineEvaluator = (CombiningEngineEvaluator) outEngineEvaluator;
		
		    			JavaRDD<ObjectValue> resultsOld = (JavaRDD<ObjectValue>) outEngineEvaluator
		    					.getData()
		    					.map(value -> (ObjectValue) value);
		    			
		    			result = outCombiningEngineEvaluator.combine(result, resultsOld);
	    			}
	    		}
	    	}
	
	
	    	// Store the result
	    	if (!result.isEmpty() && evaluationJobGroup == currentJobGroup)
	    		outEngineEvaluator.store(result);
    	}
    }
    
    
    /**
     * Kills all currently running evaluations.
     */
    public static void killAllEvaluations() {
    	if (currentJobGroup+1 == Integer.MAX_VALUE)
    		currentJobGroup = 0;
    	else
    		currentJobGroup++;
		
    	SparkFactory.stop();		
    }
    
    
    /**
     * The config/properties contains:
     * 
     * mongodb_host (Host/IP)
     * hbase_host (Host/IP)
     * redis_host (Host/IP)
     * 
     * spark_master (Property for Spark)
     * dependency_jar (Contains notaql.datamodel in a jar and will be uploaded to Spark) 
     * parallelism (Property for Spark)
     * kryo (Property for Spark - use the Kryo-Serialization (More efficient))
     * 
     * @return the properties with all configuration-variables
     */
    public static Properties getProperties() {
    	if (!propertiesLoaded)
    		System.err.println("Warning: Default properties loaded");
    	
    	
    	if (properties == null)
    		properties = new Properties();
    	
    	
    	return properties;
    }
}
