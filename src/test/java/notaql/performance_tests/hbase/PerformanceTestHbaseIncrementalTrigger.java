package notaql.performance_tests.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import notaql.NotaQL;
import notaql.engines.hbase.HBaseApi;
import notaql.engines.hbase.triggers.coprocessor.Coprocessor;
import notaql.engines.incremental.trigger.TriggerEngineEvaluator;
import notaql.model.Transformation;

public abstract class PerformanceTestHbaseIncrementalTrigger extends PerformanceTestHbaseFullrecomputation {
	// Object variables
	private Transformation triggerTransformation;
	
	
	/**
	 * Truncates the tables.
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();

		triggerTransformation = null;
	}
	
	
	/**
	 * Generates the NotaQL-Query.
	 * 
	 * @param queryBody
	 * @param isTimestampQuery
	 * @return
	 */
	protected static String generateQueryTrigger(String queryBody) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("IN-ENGINE: hbase(table_id <- '" + input.getName() + "', triggerbased <- 'true'),");
		sb.append("OUT-ENGINE: hbase(table_id <- '" + output.getName() + "'),");
		sb.append(queryBody);
		

		return sb.toString();
	}
	
	
	/**
	 * Re-evaluates the transformation (blocking operation).
	 * 
	 * @param dataOld
	 * @param putNew
	 * @throws IOException 
	 * @throws Exception
	 */
	private void evaluateTransformationTrigger(Result dataOld, Put putNew) throws IOException {
		if (triggerTransformation != null) {
			// Serialize the data
			String stringDataNew = putNew == null ? null : Coprocessor.serializeHbase(dataOld, putNew).toSerializedString();
			String stringDataOld = dataOld == null ? null : Coprocessor.serializeHbase(dataOld).toSerializedString();
			
			
			// Re-Evaluate the transformation
//			log("evaluateTransformationTrigger('" + stringDataOld + "', '" + stringDataNew + "')", Loglevel.DEBUG);
			NotaQL.evaluateTransformationTrigger(triggerTransformation, stringDataNew, stringDataOld);
		}
//		else
//			log("evaluateTransformationTrigger() called but triggerTransformation is null... Not re-evaluating the query...", Loglevel.WARN);
	}
	
	
	/* (non-Javadoc)
	 * @see notaql_extensions.tests.Test#notaqlEvaluate(java.lang.String)
	 */
	protected List<Transformation> notaqlEvaluateTrigger(String query) throws IOException {
		List<Transformation> transformations = super.notaqlEvaluate(query);
		
		if (transformations.size() == 1 && transformations.get(0).getInEngineEvaluator() instanceof TriggerEngineEvaluator)
			triggerTransformation = transformations.get(0);
		else
			throw new IllegalArgumentException("Query is not trigger-based");
		
		return transformations;
	}
	
	
	/* (non-Javadoc)
	 * @see notaql_extensions.tests.hbase.TestHbase#put(org.apache.hadoop.hbase.client.HTable, java.lang.String, java.lang.String, java.lang.Object)
	 */
	@Override
	protected void put(HTable table, String rowId, String column, Object value) throws IOException {
		// To many puts, commented out
		// log("put('" + table.getName() + "', '" + rowId + "', '" + column + "', '" + value + "')", Loglevel.DEBUG);


		// Create the put
		Put put = new Put(Bytes.toBytes(rowId));
		put.add(Bytes.toBytes(HBaseApi.DEFAULT_COLUMN_FAMILY), Bytes.toBytes(column), Bytes.toBytes(String.valueOf(value)));
		
		
		// Re-evaluating the query
    	Result dataOld = table.get(new Get(put.getRow()));
		evaluateTransformationTrigger(dataOld, put);
		
		
		// Execute the put
		hbaseApi.put(table, put);
	}
}
