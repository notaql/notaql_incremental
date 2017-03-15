package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import java.io.IOException;

import org.json.simple.JSONObject;

import notaql.extensions.notaql.NotaQlWrapper;

/**
 * Processes the message with the request_type "kill_all".
 */
public class RequestProcessorKillAll implements RequestProcessor {
	// Class variables
	public static final String REQUEST_TYPE = "kill_all";
	
	
	@Override
	public String process(JSONObject json) throws IOException {
		// Unregister all trigger-based transformations
		RequestProcessorTrigger.unregisterAllTransformations();
		
		
		// Kill all running NotaQL-Executions
		NotaQlWrapper.killAll();
		
		return null;
	}
}
