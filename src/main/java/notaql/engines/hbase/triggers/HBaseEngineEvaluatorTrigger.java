package notaql.engines.hbase.triggers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import notaql.NotaQL;
import notaql.SparkFactory;
import notaql.datamodel.AtomValue;
import notaql.datamodel.ObjectValue;
import notaql.datamodel.Value;
import notaql.engines.Engine;
import notaql.engines.hbase.HBaseEngine;
import notaql.engines.hbase.HBaseEngineEvaluator;
import notaql.engines.hbase.datamodel.ValueConverter;
import notaql.engines.hbase.triggers.coprocessor.Coprocessor;
import notaql.engines.hbase.triggers.coprocessor.SerializableResult;
import notaql.engines.incremental.DeltaFilter;
import notaql.engines.incremental.trigger.TriggerEngineEvaluator;
import notaql.evaluation.SparkTransformationEvaluator;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.parser.TransformationParser;

/**
 * Used for trigger-based incremental transformations.
 */
public class HBaseEngineEvaluatorTrigger extends HBaseEngineEvaluator implements TriggerEngineEvaluator {
	// Configuration
	private static final String PROPERTY_TRIGGER_JAHR = "hbase_trigger_jar";
	private static final String PROPERTY_COPROCESSOR_URL = "hbase_trigger_coprocessor_url";
	
	// Object variables
	private String coprocessor_url;
	
	
	public HBaseEngineEvaluatorTrigger(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {		
		super(engine, parser, params);
		
        String triggerString = params.get(HBaseEngine.PARAMETER_NAME_TRIGGERBASED).getValue().toString();
        
        if (triggerString.startsWith("http:"))
        	this.coprocessor_url = triggerString;
        else
        	this.coprocessor_url = NotaQL.getProperties().getProperty(PROPERTY_COPROCESSOR_URL, "http://localhost:8080/thesis/to_server");
	}

	
    /* (non-Javadoc)
     * @see notaql.engines.EngineEvaluator#evaluate(notaql.model.Transformation)
     */
    @Override
    public JavaRDD<ObjectValue> evaluate(Transformation transformation) {
    	throw new IllegalStateException("Don't use evaluate(Transformation) for trigger-based evaluations (the data has to be passed to the EngineEvaluator)");
    }
    
    
    /* (non-Javadoc)
     * @see notaql.engines.incremental.trigger.TriggerEngineEvaluator#evaluate(notaql.model.Transformation, java.lang.String, java.lang.String)
     */
    @Override
    public JavaRDD<ObjectValue> evaluate(Transformation transformation, String data_new, String data_old) {
        // Retrieve the data
    	JavaRDD<Value> dataConverted = this.combineWithFlags(data_new, data_old);
        JavaRDD<Value> dataFiltered = this.filterData(transformation, dataConverted);
        
        
        // Process the data according to the given notaql-transformation
        final SparkTransformationEvaluator transformationEvaluator = new SparkTransformationEvaluator(transformation);
        return transformationEvaluator.process(dataFiltered);
    }
	
	
	/* (non-Javadoc)
	 * @see notaql.engines.incremental.trigger.TriggerEngineEvaluator#stringToDataConverted(java.lang.String)
	 */
	@Override
	public JavaRDD<Value> stringToDataConverted(String input) {
		// Check if the input is null
		if (input == null)
			return SparkFactory.createEmptyRDD();
		
		
		// Convert the string back into a Result
		SerializableResult serializableResult = null;
		try {
			serializableResult = SerializableResult.fromSerializedString(input);
		} catch (IOException e) {
			e.printStackTrace();
			throw new EvaluationException("Couldn't deserialize the result");
		}
		Result result = serializableResult.toResult();
		
		
		// Convert
		if (result.isEmpty())
	        return SparkFactory.createEmptyRDD();
		else {
			// Create a NotaQL-value
	        ValueConverter valueConverter = new ValueConverter();
	        Value value = valueConverter.convertToNotaQL(result);
	        
	        
	        // Create RDD
	        JavaSparkContext spark = SparkFactory.getSparkContext();
	        return spark.parallelize(Arrays.asList(value));
		}
	}
	

	/* (non-Javadoc)
	 * @see notaql.engines.EngineEvaluator#filterData(notaql.model.Transformation, org.apache.spark.api.java.JavaRDD)
	 */
	@Override
	public JavaRDD<Value> filterData(Transformation transformation, JavaRDD<Value> dataConverted) {
		return DeltaFilter.filterData(transformation, dataConverted);
	}
	

    /**
     * Connects to hbase and adds the coprocessor to this table.
     */
    public void addCoprocessor() {
    	// Configuration for the following steps;
    	Path coprocessorTriggerJar = new Path(NotaQL.getProperties().getProperty(PROPERTY_TRIGGER_JAHR));
        
        Map<String, String> coprocessorEnvironment = new HashMap<String, String>();
        coprocessorEnvironment.put(Coprocessor.ENVIRONMENT_KEY_TABLE_ID, this.table_id);
        coprocessorEnvironment.put(Coprocessor.ENVIRONMENT_KEY_HTTP_SERVER_URL, coprocessor_url);
        
        try {
        	this.getHBaseApi().addCoprocessor(this.table_id, Coprocessor.class.getName(), coprocessorTriggerJar, coprocessorEnvironment);
        } catch (IOException e) {
        	e.printStackTrace();
        	throw new EvaluationException("Couldn't add the coprocessor to table '" + this.table_id + "'. Exception: " + e.toString());
        }
    }
    
    
    /**
     * Connects to hbase and removes the coprocessor from this table.
     */
    public void removeCoprocessor() {
    	try {
    		this.getHBaseApi().removeCoprocessor(this.table_id, Coprocessor.class.getName());
        } catch (IOException e) {
        	e.printStackTrace();
        	throw new EvaluationException("Couldn't remove the coprocessor from table '" + this.table_id + "'. Exception: " + e.toString());
        }
    }


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.IncrementalEngineEvaluator#supportsUpdates()
	 */
	@Override
	public boolean supportsUpdates() {
		return true;
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.IncrementalEngineEvaluator#supportsDeletes()
	 */
	@Override
	public boolean supportsDeletes() {
		return true;
	}
}
