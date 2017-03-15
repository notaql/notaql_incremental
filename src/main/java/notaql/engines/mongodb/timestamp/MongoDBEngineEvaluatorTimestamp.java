package notaql.engines.mongodb.timestamp;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

import notaql.datamodel.AtomValue;
import notaql.datamodel.Value;
import notaql.engines.Engine;
import notaql.engines.incremental.DeltaFilter;
import notaql.engines.incremental.timestamp.TimestampEngineEvaluator;
import notaql.engines.mongodb.MongoDBEngine;
import notaql.engines.mongodb.MongoDBEngineEvaluator;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.parser.TransformationParser;

/**
 * Used for timestamp-based incremental transformations.
 * 
 * Currently this implementation has the following limitations:
 * 
 * 1.) As Mongo does not provide us with old values only inserts are possible. Changing previous values
 * will make a full recomputation needed.
 * 
 * 2.) Because the generated object-ids from MongoDB are used (they contain the timestamp) different object-ids
 * will break this Evaluator.
 */
public class MongoDBEngineEvaluatorTimestamp extends MongoDBEngineEvaluator implements TimestampEngineEvaluator  {
	// Object variables
	private final long timestampStart;	
	private boolean isInefficientIncremental = false; // will be true after filtering if using batch would have been better (because there are too many changes in the data)
	

	public MongoDBEngineEvaluatorTimestamp(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {
		super(engine, parser, params);
        
        if (!params.keySet().contains(MongoDBEngine.PARAMETER_NAME_TIMESTAMP))
            throw new EvaluationException(engine.getEngineName() +" engine (with timestamps) expects the following parameters on initialization: timestamp, " + String.join(", ", engine.getArguments()));

        this.timestampStart = Long.valueOf(params.get(MongoDBEngine.PARAMETER_NAME_TIMESTAMP).getValue().toString());
	}
	
	
	@Override
	public long getPreviousTimestamp() {
		return this.timestampStart;
	}
	
	
	/**
	 * Retrieves the delta (insert, preserved (delete is not possible).
	 * 
	 * The end of the timerange is *not* the current time because this could lead to indeterministic results in the data
	 * (after another incremental update). To prevent this, the end of the timerange is the time of the initialization of
	 * this EngineEvaluator. If the user then wants to perform an incremental update he sets this end of the timerange as
	 * the new this.timestampStart and will get consistent results. 
	 * 
	 * @param transformation
	 * @return the timestamped data
	 */
	@Override
	public JavaRDD<Value> getDataFiltered(Transformation transformation) {
		return this.getDataFiltered(transformation, this.database_name, this.collection_name, this.getPreviousTimestamp());
    }
	

	/** 
	 * Entries which do not fulfill the input filter will be filtered  (queries of MongoDB are less expressive than NotaQL so we have to this again even if there was a mongodb-filter defined)
	 * 
	 * @see notaql.engines.EngineEvaluator#filterData(notaql.model.Transformation, org.apache.spark.api.java.JavaRDD)
	 */
	@Override
    public JavaRDD<Value> filterData(Transformation transformation, JavaRDD<Value> dataConverted) {
		return DeltaFilter.filterDataOnlyInserts(transformation, dataConverted);
    }
	
	
	/* (non-Javadoc)
	 * @see notaql.engines.EngineEvaluator#getMessages()
	 */
	@Override
	public List<String> getMessages() {
		if (isInefficientIncremental)
			return Arrays.asList("Sehr viele Änderungen. Normale Ausführung vermutlich effizienter.");
		else
			return Collections.emptyList();
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.IncrementalEngineEvaluator#supportsUpdates()
	 */
	@Override
	public boolean supportsUpdates() {
		return false;
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.IncrementalEngineEvaluator#supportsDeletes()
	 */
	@Override
	public boolean supportsDeletes() {
		return false;
	}
}
