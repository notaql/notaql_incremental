package notaql.engines.hbase.timestamp;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import notaql.SparkFactory;
import notaql.datamodel.AtomValue;
import notaql.datamodel.Value;
import notaql.engines.Engine;
import notaql.engines.hbase.HBaseEngine;
import notaql.engines.hbase.HBaseEngineEvaluator;
import notaql.engines.hbase.exceptions.TableHasNoVersionsException;
import notaql.engines.incremental.DeltaFilter;
import notaql.engines.incremental.timestamp.TimestampEngineEvaluator;
import notaql.model.EvaluationException;
import notaql.model.Transformation;
import notaql.parser.TransformationParser;

/**
 * Used for timestamp-based incremental transformations.
 * 
 * Currently this implementation has the following limitations:
 * 
 * 1.) Using delete directly in the HBase-shell will break everything. Don't do it. Just set the value to zero. (e.g.
 * for SUM() the old value will be substracted and zero will be added)
 * 
 * 2.) Be sure to set a high number of maxversions at table creation time (we have to detect a version before the
 * timestampStart) or it will break.
 */
public class HBaseEngineEvaluatorTimestamp extends HBaseEngineEvaluator implements TimestampEngineEvaluator {
	// Configuration
	private static final int MIN_NUMBER_OF_REQUIRED_VERSIONS = 2;
	
	
	// Object variables
	protected long timestampStart;	
	private boolean isInefficientIncremental = false; // will be true after filtering if using batch would have been better (because there are too many changes in the data)
	

	public HBaseEngineEvaluatorTimestamp(Engine engine, TransformationParser parser, Map<String, AtomValue<?>> params) {
		super(engine, parser, params);
        
        if (!params.keySet().contains(HBaseEngine.PARAMETER_NAME_TIMESTAMP))
            throw new EvaluationException(engine.getEngineName() +" engine (with timestamps) expects the following parameters on initialization: timestamp," + String.join(", ", engine.getArguments()));

        this.timestampStart = Long.valueOf(params.get(HBaseEngine.PARAMETER_NAME_TIMESTAMP).getValue().toString());
	}
	
	
	@Override
	public long getPreviousTimestamp() {
		return this.timestampStart;
	}
	
	
	/**
	 * Retrieves the delta (delete, insert, preserved).
	 * 
	 * The timerange has to start at zero in order to also retrieve the preserved values. Also we need all versions in order to
	 * detect deletes (last value before this.timestampStart) and insertions (last value before this.timestampEnd).
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
		// Check if this is an insert only request
		boolean insertsOnly = userExpectsDeletes() != null && !userExpectsDeletes() && userExpectsUpdates() != null && !userExpectsUpdates();
		
		
		// Check if the table supports versions
		if (!insertsOnly) {
			try {
				HTable table = this.getHBaseApi().getOrCreateTable(this.table_id);
				if (this.getHBaseApi().getMaxNumberOfVersions(table) < MIN_NUMBER_OF_REQUIRED_VERSIONS)
					throw new TableHasNoVersionsException("The table has to store at least " + MIN_NUMBER_OF_REQUIRED_VERSIONS + " versions to support timestamps");
			} catch (IOException e) {
				e.printStackTrace();
				throw new EvaluationException("Could not check the number of versions for table '" + this.table_id + "': " + e.getMessage());
			}
		}
		
		
    	// Configuration for the following steps
        final Configuration sparkConfigurationInput = this.getHBaseApi().getConfiguration();
        sparkConfigurationInput.set(TableInputFormat.INPUT_TABLE, this.table_id);
        
        if (insertsOnly) {
        	sparkConfigurationInput.set(TableInputFormat.SCAN_TIMERANGE_START, String.valueOf(this.getPreviousTimestamp()));
            sparkConfigurationInput.set(TableInputFormat.SCAN_MAXVERSIONS, "1");
        } else {
        	sparkConfigurationInput.set(TableInputFormat.SCAN_TIMERANGE_START, "0");
            sparkConfigurationInput.set(TableInputFormat.SCAN_MAXVERSIONS, String.valueOf(Integer.MAX_VALUE));
        }
        
    	sparkConfigurationInput.set(TableInputFormat.SCAN_TIMERANGE_END, String.valueOf(this.getCurrentTimestamp())); // This is exclusive (<)
        
          
    	// Retrieve the data using spark and convert it to timestamped data afterwards.
        final JavaSparkContext spark = SparkFactory.getSparkContext();
        final JavaPairRDD<ImmutableBytesWritable, Result> dataRaw = spark.newAPIHadoopRDD(sparkConfigurationInput, TableInputFormat.class, ImmutableBytesWritable.class, org.apache.hadoop.hbase.client.Result.class);
        
        
		// Convert the data
        if (insertsOnly) {
        	final TimestampValueConverterInsertsOnly valueConverter = new TimestampValueConverterInsertsOnly(this.getPreviousTimestamp());
        	return dataRaw.map(tuple -> valueConverter.convertToNotaQL(tuple._2));
        }
        else {
        	final TimestampValueConverter valueConverter = new TimestampValueConverter(this.getPreviousTimestamp());
        	return dataRaw.flatMap(tuple -> valueConverter.convertToNotaQLList(tuple._2));
        }
	}
	
	
	/**
	 * In contrast to the normal HBaseEngineEvaluator there maybe is more than one Value with the same row-id. Because of this filtering
	 * the rows which don't match the row-predicate is more difficult.
	 * Also the rows which don't contain changed informations are dropped.
	 */
	@Override
	public JavaRDD<Value> filterData(Transformation transformation, JavaRDD<Value> dataConverted) {
		// Check if this is an insert only request
		boolean insertsOnly = userExpectsDeletes() != null && !userExpectsDeletes() && userExpectsUpdates() != null && !userExpectsUpdates();
		
		if (insertsOnly) 
			return DeltaFilter.filterDataOnlyInserts(transformation, dataConverted);
		else
			return DeltaFilter.filterData(transformation, dataConverted);
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
		return true;
	}


	/* (non-Javadoc)
	 * @see notaql.engines.incremental.IncrementalEngineEvaluator#supportsDeletes()
	 */
	@Override
	public boolean supportsDeletes() {
		return false;
	}
}
