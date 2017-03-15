package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import java.io.IOException;

import org.json.simple.JSONObject;

import com.google.gson.Gson;

import notaql.extensions.dashboard.http.servlets.toServer.response.ResponseList;

/**
 * Processes the message with the request_type "transformation_load".
 */
public class RequestProcessorTransformationLoad implements RequestProcessor {
	// Class variables
	public static final String REQUEST_TYPE = "transformation_load";
	private static Gson gson;
	
	
	@Override
	public String process(JSONObject json) throws IOException {
		// New gson-object
		if (gson == null)
			gson = new Gson();
		
		
		// Generate return-json
		return gson.toJson(new ResponseList<String>(RequestProcessorTransformationStore.getTransformations()));
	}
}
