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

package notaql.engines.json;

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONObject;

import notaql.SparkFactory;
import notaql.datamodel.AtomValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Value;
import notaql.engines.Engine;
import notaql.engines.EngineEvaluator;
import notaql.engines.incremental.resultcombiner.CombiningEngineEvaluator;
import notaql.engines.json.datamodel.ValueConverter;
import notaql.engines.json.path.JSONInputPathParser;
import notaql.engines.json.path.JSONOutputPathParser;
import notaql.model.Transformation;
import notaql.parser.TransformationParser;
import notaql.parser.path.InputPathParser;
import notaql.parser.path.OutputPathParser;

/**
 * Created by thomas on 23.02.15.
 */
public class JSONEngineEvaluator extends EngineEvaluator implements CombiningEngineEvaluator {
	// Object variables
    private final String path;
    

    public JSONEngineEvaluator(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {
        super(engine, parser, params);

        this.path = params.get(JSONEngine.PARAMETER_NAME_PATH).getValue().toString();
    }
    
    
    /**
     * @return the path
     */
    public String getPath() {
    	return this.path;
    }


    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getInputPathParser()
     */
    @Override
    public InputPathParser getInputPathParser() {
        return new JSONInputPathParser(this.getParser());
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getOutputPathParser()
     */
    @Override
    public OutputPathParser getOutputPathParser() {
        return new JSONOutputPathParser(this.getParser());
    }
    
    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#getDataFiltered(notaql.model.Transformation)
     */
    // No engine specific filtering (using the transformation) is currently applied
    public JavaRDD<Value> getDataFiltered(Transformation transformation) {
    	// Use spark to retrieve the raw input rdds from the json-file or the json-folder
        // These raw input rdds are then converted to the inner format used by this notaql-framework
        // After that the entries which do not fulfill the input filter will be filtered
    	JavaSparkContext spark = SparkFactory.getSparkContext();
    	
    	// Check if the path exists
    	// Return an empty RDD if not (to avoid errors with the timestamp-based calculations)
    	try {
    		if (!getFileSystem().getPath(path).toFile().exists())
    			return spark.emptyRDD();
    	} catch (Exception e) {
    		// Catch file-system errors
    		e.printStackTrace();
    	}
	
    	JavaRDD<Object> dataRaw = spark.textFile(path)
                .filter(s -> !s.equals("[") && !s.equals("]")) // if it consists of an array containing objects: just ignore the array TODO: this is kind of Q&D
                .map(JSONObject::new);
        
        return dataRaw.map(ValueConverter::convertToNotaQL);
    }

    
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#store(org.apache.spark.api.java.JavaRDD)
     */
    @Override
    public void store(JavaRDD<ObjectValue> resultRaw) {
		super.store(resultRaw);
		
		
        // Generate the resulting objects using spark
    	// These objects may contain JSONObjects, but maybe also Strings, Numbers, etc.. It depends on the input.
        JavaRDD<Object> resultObjects = resultRaw.map(ValueConverter::convertFromNotaQL);

        
        // Check if the folder already exists and delete it if this is the case
        try {
        	File pathFile = new File(path);
    		if (pathFile.exists()) {
    			FileUtils.forceDelete(pathFile);
    		}
        } catch (Exception e) {
    		// Catch file-system errors
        	e.printStackTrace();
        }
        
        
        // Store the data using spark
    	resultObjects.map(Object::toString).saveAsTextFile(this.path);
    }


	/* (non-Javadoc)
	 * @see notaql.engines.EngineEvaluator#isBaseType()
	 */
	@Override
	public boolean isBaseType() {
		return this.getClass().equals(JSONEngineEvaluator.class);
	}
}
