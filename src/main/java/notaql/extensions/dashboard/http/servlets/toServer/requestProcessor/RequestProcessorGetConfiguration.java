package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import java.io.IOException;

import org.json.simple.JSONObject;

import com.google.gson.Gson;

import notaql.extensions.dashboard.http.servlets.toServer.response.Response;
import notaql.extensions.dashboard.http.servlets.toServer.response.ResponseMap;
import notaql.extensions.notaql.NotaQlWrapper;

/**
 * Processes the message with the request_type "get_configuration".
 */
public class RequestProcessorGetConfiguration implements RequestProcessor {
	// Class variables
	public static final String REQUEST_TYPE = "get_configuration";
	private static Gson gson;
	

	@Override
	public String process(JSONObject json) throws IOException {
		Response response = new ResponseMap<String, String>(NotaQlWrapper.getConfig()); 
		
		
		// New gson-object
		if (gson == null)
			gson = new Gson();
		
		
		// Generate return-json
		return gson.toJson(response);
	}

}
