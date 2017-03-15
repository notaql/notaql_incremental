package notaql.extensions.dashboard.http.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import notaql.extensions.dashboard.http.requestlog.RequestLog;
import notaql.extensions.dashboard.util.Util;

/**
 * Base-class for all *json* servlets.
 */
public abstract class JsonServlet extends HttpServlet {
	// Constants
	private static final long serialVersionUID = 1L;


	/**
     * @throws ServletException if init fails 
     */
	public JsonServlet() throws ServletException {
        super();
        super.init();
	}

    
	/**
	 * Gets called on a GET-Requests
	 *
	 * @param request the request
	 * @param response the response (base64 encoded)
	 * 
	 * @throws ServletException the servlet exception
	 * @throws IOException Signals that an I/O exception has occurreds.
	 * 
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// Log the request
		RequestLog.logMessage("[" + this.getClass().getSimpleName() + "] Incoming JSON-Request");
				
		
		// Various variables
		response.setCharacterEncoding("UTF-8");
		response.setContentType("application/json"); // jQuery needs this
		
		
		// Generate the response
		String rawResponseString;
		try {
			// Get the json
			String jsonString;
			
			Map<String, String[]> parameterMap = request.getParameterMap();
			
			if (parameterMap.containsKey("jsonBase64")) {
				// Decode the base64-string
				final String jsonBase64 = request.getParameter("jsonBase64");
				jsonString = Util.base64decodeString(jsonBase64);
			}
			else if (parameterMap.containsKey("json"))
				jsonString = request.getParameter("json");
			else
				throw new IllegalStateException("No json passed as parameter");

			RequestLog.logMessage("\tRequest: " + jsonString);
			
			
			// Generate the response string
			rawResponseString = this.renderResponse(request, jsonString);
			if (rawResponseString == null)
				rawResponseString = "{\"response\":\"ok\"}";
			
		} catch (Exception e) {
			e.printStackTrace();
			RequestLog.logError("[" + this.getClass().getSimpleName() + "] Exception while serving the request: '" + e.getClass().getName() + ": " + e.getMessage() + "'");
			rawResponseString = "{\"response\":\"error\",\"error\":\"exception\",\"error_message\":\"" + e.getClass().getName() + ": " + (e.getMessage() == null ? "" : e.getMessage().replace("\"", "\\\"")) + "\"}";
		}

		
		// Remove some json-pitfalls
		rawResponseString = rawResponseString.replace("\n", "\\n")
			.replace("\t", "\\t")
			.replace("\r", "\\r");
		
		RequestLog.logMessage("\tResponse: " + rawResponseString);
		
		
		// Return the response
		final String responseString = URLEncoder.encode("data:application/json;base64," + Util.base64encodeString(rawResponseString), "UTF-8");
		
		PrintWriter writer = response.getWriter();
		writer.write(responseString);
		writer.close();
	}

    
	/*
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		request.setCharacterEncoding("UTF-8");
		
		this.doGet(request, response);
	}
	
	
	/**
	 * Renders the response for a single request (overwrite this in the subclass).
	 * 
	 * @param request
	 * @param jsonString the decoded jsonString from the request
	 * @return
	 */
	protected abstract String renderResponse(HttpServletRequest request, String jsonString) throws Exception;
}
