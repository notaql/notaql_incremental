package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

/**
 * Processes the message with the request_type "transformation_store".
 */
public class RequestProcessorTransformationStore implements RequestProcessor {
	// Class variables
	public static final String REQUEST_TYPE = "transformation_store";
	private static final Map<String, String> transformationsBase64 = new HashMap<String, String>();
	
	
	@Override
	public String process(JSONObject json) throws IOException {
		JSONObject jsonRequest = (JSONObject) json.get("request");
		String key = String.valueOf(jsonRequest.get("key"));
		String transformationBase64 = (String) jsonRequest.get("transformation_base64");
		
		if (transformationBase64 != null)
			transformationsBase64.put(key, transformationBase64);
		else
			transformationsBase64.remove(key);
		
		return null;
	}
	
	
	public static List<String> getTransformations() {
		return new ArrayList<String>(transformationsBase64.values());
	}
	
	
	public static void removeTransformation(String key) {
		transformationsBase64.remove(key);
	}
}
