package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;

import notaql.extensions.notaql.NotaQlWrapper;

/**
 * Processes the message with the request_type "set_configuration".
 */
public class RequestProcessorSetConfiguration implements RequestProcessor {
	// Class variables
	public static final String REQUEST_TYPE = "set_configuration";
	
	
	@SuppressWarnings("unchecked")
	@Override
	public String process(JSONObject json) throws IOException {
		JSONObject jsonRequest = (JSONObject) json.get("request");
		
		Map<String, String> newConfigMap = new HashMap<String, String>();
		for (Map.Entry<String, String> entry : (Set<Map.Entry<String, String>>) jsonRequest.entrySet())
			newConfigMap.put(entry.getKey(), entry.getValue());
		
		NotaQlWrapper.setConfig(newConfigMap);
		
		return null;
	}
}
