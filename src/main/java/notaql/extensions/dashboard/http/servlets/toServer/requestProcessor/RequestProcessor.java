package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import org.json.simple.JSONObject;

/**
 * Implemented by the classes which process single requests incoming from the browser.
 */
public interface RequestProcessor {
	/**
	 * Processes the request.
	 * 
	 * @param json the json-object containing the request.
	 * @return null or the response
	 */
	public String process(JSONObject json) throws Exception;
}
