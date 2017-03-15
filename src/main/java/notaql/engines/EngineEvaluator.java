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

package notaql.engines;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.AtomValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Step;
import notaql.datamodel.Value;
import notaql.engines.incremental.IncrementalEngineEvaluator;
import notaql.engines.incremental.util.Util;
import notaql.evaluation.SparkTransformationEvaluator;
import notaql.extensions.dashboard.http.servlets.fromServer.JsonPollFromServerServlet;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.model.vdata.ConstructorVData;
import notaql.model.vdata.FunctionVData;
import notaql.parser.TransformationParser;
import notaql.parser.path.InputPathParser;
import notaql.parser.path.OutputPathParser;


/**
 * An EngineEvaluator is where the engines specify how transformations should be evaluated for the given store.
 *
 * Here we define the ways to access data and store it as well as influence the evaluation process.
 */
public abstract class EngineEvaluator {
	// Configuration
	public static final String ROW_ID_IDENTIFIER = "_id";
	
	
	// Object variables
	private final Engine engine;
    private final TransformationParser parser;
    private final Map<String, AtomValue<?>> params;
    private Map<String, String> paramsAsStrings;

    private final long dataRetrievalTimestamp;
    private long numberOfObjectsUnfiltered;
    private long numberOfObjectsFiltered;
    
    private Boolean userExpectsUpdates = null;
    private Boolean userExpectsDeletes = null;
	
    
    public EngineEvaluator(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) throws EvaluationException {
    	this.engine = engine;
    	this.parser = parser;
    	this.params = params;
    	
    	// Check if all required parameters are passed
        if (!params.keySet().containsAll(engine.getRequiredArguments())) {
        	throw new EvaluationException(engine.getEngineName() +" engine expects the following parameters on initialization: " + String.join(", ", engine.getRequiredArguments()));
        }
        
        
        // User expects deletes or updates?
        if (params.keySet().contains(Engine.PARAMETER_NAME_USER_EXPECTS_UPDATES))
        	this.userExpectsUpdates = params.get(Engine.PARAMETER_NAME_USER_EXPECTS_UPDATES).getValue().toString().equalsIgnoreCase("true");
        if (params.keySet().contains(Engine.PARAMETER_NAME_USER_EXPECTS_DELETES))
        	this.userExpectsDeletes = params.get(Engine.PARAMETER_NAME_USER_EXPECTS_DELETES).getValue().toString().equalsIgnoreCase("true");
        
        // Check the compatiblity
        if (this instanceof IncrementalEngineEvaluator) {
        	// Expected operations (deletes or updates)
        	if (!((IncrementalEngineEvaluator) this).expectedOperationsAndSupportedOperationsCompatible())
        		throw new EvaluationException(this.getClass().getSimpleName() +" doesn't support all expected operations");
        }
        
        
        // Set the start-time
        this.dataRetrievalTimestamp = System.currentTimeMillis();
    }
    

    /**
     * @return true if the user expects updates inside the database. may be null.
     */
    public Boolean userExpectsUpdates() {
    	return this.userExpectsUpdates;
    }

    
    /**
     * @return true if the user expects deletes inside the database. may be null.
     */
    public Boolean userExpectsDeletes() {
    	return this.userExpectsDeletes;
    }
    
	
	/**
	 * @return the unix-timestamp at which this evaluator retrieved its data from the database
	 */
	public long getCurrentTimestamp() {
		return this.dataRetrievalTimestamp;
	}
	
	
	public long getNumberOfObjectsUnfiltered() {
		return this.numberOfObjectsUnfiltered;
	}
	
	
	public long getNumberOfObjectsFiltered() {
		return this.numberOfObjectsFiltered;
	}
	
	
	/**
	 * Checks if this instance is an instance of the base type (= full recomputation)
	 * 
	 * @return
	 */
	public abstract boolean isBaseType();
	
	
	public Map<String, AtomValue<?>> getParams() {
		return this.params;
	}
	
	
	public Map<String, String> getParamsAsStrings() {
		if (this.paramsAsStrings == null) {
			this.paramsAsStrings = new HashMap<String, String>(params.size()*2);
			
			for (Entry<String, AtomValue<?>> entry : this.params.entrySet())
				this.paramsAsStrings.put(entry.getKey(), String.valueOf(entry.getValue().getValue()));
		}
		
		return this.paramsAsStrings;
	}
    
    
	/**
	 * @return the step used by all Engine Evaluators to identify the key/row-id/object-id
	 */
	public static Step<String> getRowIdentifierStep() {
		return new Step<String>(ROW_ID_IDENTIFIER);
	}
    
    
    public Engine getEngine() {
    	return this.engine;
    }
    
    
    public TransformationParser getParser() {
    	return this.parser;
    }
    
    
    /**
     * Provides the inputpath parser
     */
    public abstract InputPathParser getInputPathParser();

    
    /**
     * Provides the outputpath parser
     */
    public abstract OutputPathParser getOutputPathParser();

    
    /**
     * Retrieves the complete (= unfiltered) dataset from the engine in the internal Notaql-format (= converted). 
     *
     * @return the converted data
     */
    public final JavaRDD<Value> getData() {
    	return this.getDataFiltered(null);
    }
    
    
    /**
     * Retrieves the dataset from the engine which matches the IN-FILTER predicate (given by the transformation) in the internal Notaql-format (= converted).
     * 
     * This method is different from filterData, because the filtering already takes place at the engine (e.g. with a special query).
     * 
     * @param transformation for engine specific filtering. If it is null the engine has to retrieve the data unfiltered.
     * @return the converted and filtered data
     */
    public abstract JavaRDD<Value> getDataFiltered(Transformation transformation);

    
    /**
     * Filters the converted data. Data which does not satisfy the IN-FILTER predicate will be removed. 
     *
     * @return the filtered data
     */
    public JavaRDD<Value> filterData(final Transformation transformation, JavaRDD<Value> dataConverted) {
    	return dataConverted.filter(value -> transformation.satisfiesInPredicate((ObjectValue) value));
    }

    
    /**
     * Evaluates the transformation using Spark.
     *
     * @param transformation
     * @return resultRaw the result of the transformation in the inner format of the notaql framework
     */
    public JavaRDD<ObjectValue> evaluate(Transformation transformation) {
        // Check the compatiblity
        if (this instanceof IncrementalEngineEvaluator) {
        	// All needed functions are implemented for incremental operations
        	if (!Util.isQuerySupportingIncrementalComputation(transformation.getExpression()))
        		throw new EvaluationException("The query contains functions which can't be evaluated incrementally");
        }
        
        
		// The user needs to know the timestampEnd so he can start the next transformation with this timestamp (if he wants to do timestamp-CDC)
        JsonPollFromServerServlet.addMessage(this.getClass().getSimpleName() + " started at " + this.getCurrentTimestamp());
        
        
        // Retrieve the data
    	JavaRDD<Value> dataConverted = this.getDataFiltered(transformation);
    	numberOfObjectsUnfiltered = dataConverted.count();
//    	for (Value value : dataConverted.collect()) {
//    		JsonPollFromServerServlet.addMessage("-------------\nInput (unfiltered): " + value + "\n-------------");
//    	}
		JsonPollFromServerServlet.addMessage("Input (unfiltered): " + numberOfObjectsUnfiltered + " values");
    	    	
		
        JavaRDD<Value> dataFiltered = this.filterData(transformation, dataConverted);
    	numberOfObjectsFiltered = dataFiltered.count();
//    	for (Value value : dataFiltered.collect()) {
//    		JsonPollFromServerServlet.addMessage("-------------\nInput (filtered): " + value + "\n-------------");
//    	}
		JsonPollFromServerServlet.addMessage("Input (filtered): " + numberOfObjectsFiltered + " values");
        
        
        // Process the data according to the given notaql-transformation
        final SparkTransformationEvaluator transformationEvaluator = new SparkTransformationEvaluator(transformation);
        return transformationEvaluator.process(dataFiltered);
    }

    
    /**
     * Stores the result in the database using spark.
     * 
     * Implement this in the subclass.
     *
     * @param resultRaw the result to store (in the inner format of the notaql framework)
     */
    public void store(JavaRDD<ObjectValue> resultRaw) {
//    	for (Value value : resultRaw.collect()) {
//    		JsonPollFromServerServlet.addMessage("-------------\nResult: " + value + "\n-------------");
//    	}
		JsonPollFromServerServlet.addMessage("Result: " + resultRaw.count() + " values");
    }

    
    /**
     * Provides an instance of a constructor of the given name
     *
     * @param name
     * @return a new instance or null if unsupported
     */
    public ConstructorVData getConstructor(String name) {
    	return null;
    }

    
    /**
     * Provides an instance of a function of the given name
     *
     * @param name
     * @return a new instance or null if unsupported
     */
    public FunctionVData getFunction(String name) {
    	return null;
    }
    
    
    /**
     * @return Messages which are emited by the engines (e.g. optimizing-suggestions)
     */
    public List<String> getMessages() {
    	return Collections.emptyList();
    }
    
    
    /**
     * @return the filesystem to use
     */
    public static FileSystem getFileSystem() {
    	// FIXME Allow the usage of the hdfs
    	return FileSystems.getDefault();    			
    }
}
