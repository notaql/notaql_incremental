package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import com.google.gson.Gson;

import notaql.engines.EngineEvaluator;
import notaql.engines.hbase.HBaseEngineEvaluator;
import notaql.engines.incremental.trigger.TriggerEngineEvaluator;
import notaql.extensions.advisor.Advisor;
import notaql.extensions.advisor.statistics.RuntimePredictionResult;
import notaql.extensions.advisor.statistics.exceptions.NoStatisticsException;
import notaql.extensions.dashboard.http.servlets.fromServer.JsonPollFromServerServlet;
import notaql.extensions.dashboard.http.servlets.toServer.response.Response;
import notaql.extensions.dashboard.http.servlets.toServer.response.ResponseMap;
import notaql.extensions.notaql.NotaQlWrapper;
import notaql.model.EvaluationException;
import notaql.model.Transformation;

/**
 * Processes the message with the request_type "transformation_processor".
 */
public class RequestProcessorTransformationProcessor implements RequestProcessor {
	// Class variables
	public static final String REQUEST_TYPE = "transformation_processor";
	private static Gson gson;
	
	
	/* (non-Javadoc)
	 * @see pt.http.servlets.toServer.requestProcessor.RequestProcessor#process(org.json.simple.JSONObject)
	 */
	@Override
	public String process(JSONObject json) throws Exception {
		JSONObject jsonRequest = (JSONObject) json.get("request");
		String transformationId = (String) jsonRequest.get("id");
		String script_header = (String) jsonRequest.get("script_header");
		String script_body = (String) jsonRequest.get("script_body");
		String script = script_header + "\n" + script_body;


		// Generate the response
		Map<String, Object> responseMap = null;
		
		if (jsonRequest.containsKey("forecast"))
			// User wants just a prediction of the expected runtime
			responseMap = forecastTransformation(script);
		else if (jsonRequest.containsKey("resetAdvisor"))
			invalidateAdvisor(transformationId);
		else if (jsonRequest.containsKey("advisor"))
			executeTransformationAutomated(script, transformationId);
		else {
			invalidateAdvisor(transformationId);
			responseMap = executeTransformation(script);
		}
			
		
		// Render the response
		if (responseMap == null || responseMap.isEmpty()) 
			return null;
		else {
			Response response = new ResponseMap<String, Object>(responseMap); 
			
			// New gson-object if necessary
			if (gson == null)
				gson = new Gson();
			
			// Generate return-json
			return gson.toJson(response);
		}
	}
	
	
	/**
	 * Invalidate the information which the advisor has collected as they are only
	 * valid as long as only the advisor executes the transformation
	 * 
	 * @param transformationId
	 */
	private static void invalidateAdvisor(String transformationId) {
		try {
			Advisor.invalidateTransformation(transformationId);
		} catch (Exception e) {
			e.printStackTrace();
			JsonPollFromServerServlet.addMessage("Could not invalidate the transformation: " + e.getMessage());
		}
	}
		
		
	/**
	 * Predicts the expected runtime for a transformation.
	 * 
	 * @param script
	 * @return
	 * @throws IOException
	 * @throws NoStatisticsException 
	 */
	private static Map<String, Object> forecastTransformation(String script) throws IOException {
		// Pass the query to the notaql-framework
		List<Transformation> parsedTransformations = NotaQlWrapper.parse(script);

		
		// Check the input
		if (parsedTransformations.size() != 1)
			throw new IllegalArgumentException("Pass exactly 1 transformation");
		

		// Generate the reseponse
		Map<String, Object> responseMap = new HashMap<String, Object>(4);
		
		try {
			RuntimePredictionResult result = NotaQlWrapper.getRuntimePrediction(parsedTransformations.get(0));
			responseMap.put("forecast", "ok");
			responseMap.put("forecast_result", result);
		} catch (Exception e) {
			responseMap.put("forecast", "exception");
			responseMap.put("error_message", e.getClass().getName() + ": " + (e.getMessage() == null ? "" : e.getMessage().replace("\"", "\\\"")));
		}
			
		return responseMap;
	}
	
	
	/**
	 * Executes a transformation.
	 * 
	 * @param script
	 * @param transformationId 
	 * @return
	 * @throws IOException
	 */
	private static Map<String, Object> executeTransformation(String script) throws IOException {
		// Pass the query to the notaql-framework
		List<Transformation> evaluatedTransformations = NotaQlWrapper.evaluateAndCollectStatistics(script);
				
		
		// If this was a trigger-based transformation it has to be handled differently
		if (evaluatedTransformations.size() == 1 && evaluatedTransformations.get(0).getInEngineEvaluator() instanceof TriggerEngineEvaluator)
			return handleTriggerbasedTransformation(evaluatedTransformations.get(0));
		else
			// Generate the response
			// The response contains for example the new timestamps for timestamp based engines
			return handleNormalTransformation(evaluatedTransformations);
	}
	
	
	/**
	 * Automates the exeuction of a transformation (the system decides which incremental algorithm to choose).
	 * 
	 * @param script
	 * @param transformationId 
	 * @return
	 * @throws IOException
	 */
	public static void executeTransformationAutomated(String script, String transformationId) throws IOException {
		// Parse the query
		List<Transformation> transformations = NotaQlWrapper.parse(script);
				
		if (transformations.size() != 1)
			throw new EvaluationException("Exactly one transformation may be automated");
		
		
		// Execute the query
		Advisor.evaluate(transformations.get(0), transformationId);
	}
	
	
	/**
	 * Handles the initialization of a triggerbased transformation.
	 * 
	 * The transformations will be registered at the server so the their evaluation can be triggerd by calling a specific url on the server.
	 * 
	 * @param transformation
	 * @return the id (needed for un-registering the transformation)
	 */
	private static Map<String, Object> handleTriggerbasedTransformation(Transformation transformation) {
		Map<String, Object> responseMap = new HashMap<String, Object>(4);
		
		try {
			// Check the input
			EngineEvaluator inEngine = transformation.getInEngineEvaluator();
			if (!(inEngine instanceof TriggerEngineEvaluator))
				throw new IllegalArgumentException("Transformation is not trigger-based");
	
			if (!(inEngine instanceof HBaseEngineEvaluator))
				throw new IllegalArgumentException("Triggerbased transformations are currently only implemented for HBase");
			
			
			// Register the transformation
			int triggerId = RequestProcessorTrigger.registerTransformation(transformation);
			
			responseMap.put("trigger", "ok");
			responseMap.put("trigger_id", triggerId);
		}
		catch (RuntimeException e) {
			e.printStackTrace();
			responseMap.put("trigger", "exception");
			responseMap.put("error_message", e.getClass().getName() + ": " + (e.getMessage() == null ? "" : e.getMessage().replace("\"", "\\\"")));
		}
		
		
		return responseMap;
	}
	
	
	/**
	 * Generates the response for a normal (not trigger-based) transformation.
	 * 
	 * @param transformations
	 * @return the map to return in the final response or null if there is nothing to return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" }) // Because multiple different types are stored in the resultMap
	private static Map<String, Object> handleNormalTransformation(List<Transformation> transformations) {
		// The following values are only added to the return-map if there is only 1 transformation (because no identifier is used)
		// This is no problem at the moment because the dashboard only supports 1 transformation anyway (1 window = 1 transformation) 
		if (transformations.size() != 1)
			return null;
		
		else {
			EngineEvaluator inEngine = transformations.get(0).getInEngineEvaluator();
			EngineEvaluator outEngine = transformations.get(0).getOutEngineEvaluator();
						
			Map<String, Object> responseMap = new HashMap<String, Object>(4);
			
			
			// Put the current timestamp so the user can start the next transformation as timestamp-based transformation
			responseMap.put("new_values", new HashMap<String, Map<String, Object>>(4));
			((HashMap) responseMap.get("new_values")).put("in_engine", new HashMap<String, Object>(3));
			((HashMap) ((HashMap) responseMap.get("new_values")).get("in_engine")).put("timestamp", inEngine.getCurrentTimestamp());
			
			
			// Add the messages from the engines to the return-map
			List<String> messagesInEngine = inEngine.getMessages();
			List<String> messagesOutEngine = outEngine.getMessages();
			
			if (!(messagesInEngine.isEmpty() && messagesOutEngine.isEmpty())) {
				responseMap.put("messages", new HashMap<String, List<String>>(4));
				
				if (!messagesInEngine.isEmpty())
					((HashMap) responseMap.get("messages")).put("in_engine", messagesInEngine);
				
				if (!messagesOutEngine.isEmpty())
					((HashMap) responseMap.get("messages")).put("out_engine", messagesOutEngine);
			}
			
			
			return responseMap;
		}
	}
}
