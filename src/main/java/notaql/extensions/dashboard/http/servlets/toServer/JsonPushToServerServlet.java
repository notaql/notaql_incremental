package notaql.extensions.dashboard.http.servlets.toServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import notaql.extensions.dashboard.http.servlets.JsonServlet;
import notaql.extensions.dashboard.http.servlets.exceptions.JsonProcessingException;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessor;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessorTransformationProcessor;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessorGetAllEngines;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessorGetConfiguration;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessorKill;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessorKillAll;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessorSetConfiguration;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessorTransformationLoad;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessorTransformationStore;
import notaql.extensions.dashboard.http.servlets.toServer.requestProcessor.RequestProcessorTrigger;

/**
 * Used for pushing requests from the client (=browser) to the server.
 */
@WebServlet({"/to_server"})
public class JsonPushToServerServlet extends JsonServlet {
	// Constants
	private static final long serialVersionUID = 1L;
	
	
	// Class variables
	private static JSONParser parser;
	private static Map<String, RequestProcessor> requestProcessors;
	
    
    /**
     * @throws IOException if the html-template could not be loaded 
     * @throws ServletException if init fails
     */
    public JsonPushToServerServlet() throws IOException, ServletException {
        super();
        initClass();
    }
    
    
    /**
     * Initializes the class variables.
     */
    private static void initClass() {
    	// Init the request processors
    	if (requestProcessors == null) {
    		requestProcessors = new HashMap<String, RequestProcessor>();
    		
    		requestProcessors.put(RequestProcessorGetAllEngines.REQUEST_TYPE, new RequestProcessorGetAllEngines());
    		requestProcessors.put(RequestProcessorTransformationProcessor.REQUEST_TYPE, new RequestProcessorTransformationProcessor());
    		requestProcessors.put(RequestProcessorGetConfiguration.REQUEST_TYPE, new RequestProcessorGetConfiguration());
    		requestProcessors.put(RequestProcessorSetConfiguration.REQUEST_TYPE, new RequestProcessorSetConfiguration());
    		requestProcessors.put(RequestProcessorKillAll.REQUEST_TYPE, new RequestProcessorKillAll());
    		requestProcessors.put(RequestProcessorKill.REQUEST_TYPE, new RequestProcessorKill());
    		requestProcessors.put(RequestProcessorTransformationStore.REQUEST_TYPE, new RequestProcessorTransformationStore());
    		requestProcessors.put(RequestProcessorTransformationLoad.REQUEST_TYPE, new RequestProcessorTransformationLoad());
    		requestProcessors.put(RequestProcessorTrigger.REQUEST_TYPE, new RequestProcessorTrigger());
    	}
    }
    
    
	/* (non-Javadoc)
	 * @see pt.http.servlets.JsonServlet#renderResponse(javax.servlet.http.HttpServletRequest, java.lang.String)
	 */
	@Override
	protected String renderResponse(HttpServletRequest request, String jsonString) throws Exception {
		// Check the jsonString
		if (jsonString == null || jsonString.length() == 0)
			throw new JsonProcessingException("No JSON was passed");
		
		else {
			// Parse the json
			if (parser == null)
				parser = new JSONParser();
			
			
			JSONObject json = null;
			json = (JSONObject) parser.parse(jsonString);
			
			// Process the request
			return processJsonRequest(json);
		}
	}
	
	
	/**
	 * Processes a json-request from the client (=browser).
	 * 
	 * @param json the input-request
	 * @return a json-string to return to the client or null (for the default one to be returned)
	 * @throws JsonProcessingException if the input-request could not be processed
	 */
	private static String processJsonRequest(JSONObject json) throws Exception {
		// Get the type of the request
		final String requestType = ((String) json.get("request_type"));
		
		
		// Pass the request to the correct processor
		if (!requestProcessors.containsKey(requestType))
			throw new JsonProcessingException("invalid request_type");
		else
			return requestProcessors.get(requestType).process(json);
	}
}