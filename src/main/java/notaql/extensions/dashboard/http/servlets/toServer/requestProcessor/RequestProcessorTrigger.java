package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.JSONObject;

import notaql.engines.hbase.triggers.HBaseEngineEvaluatorTrigger;
import notaql.extensions.dashboard.http.servlets.fromServer.JsonPollFromServerServlet;
import notaql.extensions.notaql.NotaQlWrapper;
import notaql.model.Transformation;

/**
 * Processes the message with the request_type "trigger_hbase".
 */
public class RequestProcessorTrigger implements RequestProcessor {
	// Class variables
	public static final String REQUEST_TYPE = "trigger_hbase";
	private static Map<String, List<Transformation>> transformationsByTable = new HashMap<String, List<Transformation>>();
	private static int transformationsCounter = 0;
	private static Map<Integer, Transformation> transformationById = new HashMap<Integer, Transformation>();
	private ExecutorService executor = Executors.newFixedThreadPool(2); // Using 2 threads because 1 will be able to evaluate the notaql-query while the other stores the result of the previous query.
	
	
	/**
	 * Registers a transformation to be executed by a trigger. 
	 * 
	 * @param transformation
	 * @return
	 */
	public static int registerTransformation(Transformation transformation) {
		// Store the transformation
		final HBaseEngineEvaluatorTrigger inEngine = (HBaseEngineEvaluatorTrigger) transformation.getInEngineEvaluator();
		final String table_id = inEngine.getTableId().toLowerCase();
		final int id = transformationsCounter++;
		
		
		// Table -> Transformations
		synchronized (transformationsByTable) {
			if (transformationsByTable.containsKey(table_id))
				transformationsByTable.get(table_id).add(transformation);
			
			else {
				JsonPollFromServerServlet.addMessage("Haenge Coprocessor an Tabelle '" + table_id + "' an... Dies kann u.U. mehrere Minuten dauern.");
				inEngine.addCoprocessor();
				JsonPollFromServerServlet.addMessage("Coprocessor an Tabelle '" + table_id + "' angehaengt");
				
				List<Transformation> list = new ArrayList<Transformation>();
				list.add(transformation);
				
				transformationsByTable.put(table_id, list);
			}
		}
		

		// Id -> Transformation (needed for unregistering the transformation)
		transformationById.put(id, transformation);
		
		return id;
	}
	
	
	/**
	 * Unregisters all registered transformations
	 */
	public static void unregisterAllTransformations() {
		for (int id : new HashSet<Integer>(transformationById.keySet())) // Iterate over a copy to avoid concurrency exceptions
			unregisterTransformation(id);
	}
	
	
	/**
	 * Unregisters a transformation, so it won't be executed any more by a trigger
	 * 
	 * @param id
	 */
	public static void unregisterTransformation(int id) {
		Transformation transformation = transformationById.get(id);
		
		if (transformation == null)
			throw new IllegalArgumentException("Invalid transformation-id: " + id);
		else {
			// ID Map
			transformationById.remove(id);
			

			// Table_ID Map
			final HBaseEngineEvaluatorTrigger inEngine = (HBaseEngineEvaluatorTrigger) transformation.getInEngineEvaluator();
			final String table_id = inEngine.getTableId();
			
			synchronized (transformationsByTable) {
				transformationsByTable.get(table_id).remove(transformation);
				
				// Remove the coprocessor if it is not needed any more
				if (transformationsByTable.get(table_id).isEmpty()) {
					transformationsByTable.remove(table_id);
					
					inEngine.removeCoprocessor();
					JsonPollFromServerServlet.addMessage("Coprocessor von Tabelle '" + table_id + "' entfernt");
				}
			}
			
			
			// Stored Transformation
			RequestProcessorTransformationStore.removeTransformation("trigger_" + id);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see pt.http.servlets.toServer.requestProcessor.RequestProcessor#process(org.json.simple.JSONObject)
	 */
	@Override
	public String process(JSONObject json) throws Exception {
		// Get the data
		JSONObject jsonRequest = (JSONObject) json.get("request");
		String table_id = ((String) jsonRequest.get("table_id")).toLowerCase();
		
		final String dataNew;
		if (jsonRequest.containsKey("data_new"))
			dataNew = (String) jsonRequest.get("data_new");
		else
			dataNew = null;
		
		final String dataOld;
		if (jsonRequest.containsKey("data_old"))
			dataOld = (String) jsonRequest.get("data_old");
		else
			dataOld = null;
		
		
		// Enqueue the execution of the transformations in the Executor
		List<Transformation> transformations = transformationsByTable.get(table_id);
		
		if (transformations == null) {
			JsonPollFromServerServlet.addMessage("Trigger with wrong table_id ('" + table_id + "') executed");
			throw new IllegalStateException("table_id '" + table_id + "' is not registered");
		}
		

		for (Transformation transformation : transformations) {
			executor.submit(() -> {
				try {
					NotaQlWrapper.evaluateTransformationTrigger(transformation, dataNew, dataOld);
				} catch (Exception e) {
					JsonPollFromServerServlet.addMessage("Trigger-execution failed: " + e.toString());
				}
			});
		}
		
		
		return null;
	}
}
