package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import java.io.IOException;

import org.json.simple.JSONObject;

/**
 * Processes the message with the request_type "kill".
 */
public class RequestProcessorKill implements RequestProcessor {
	// Class variables
	public static final String REQUEST_TYPE = "kill";
	
	
	@Override
	public String process(JSONObject json) throws IOException {
		JSONObject jsonRequest = (JSONObject) json.get("request");
		Integer transformationId = (int) ((long) jsonRequest.get("id"));
				
		
		// Unregister the trigger-based transformation
		RequestProcessorTrigger.unregisterTransformation(transformationId);
		return null;
	}
}
