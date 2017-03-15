package notaql.extensions.dashboard.http.servlets.toServer.requestProcessor;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

import com.google.gson.Gson;

import notaql.engines.EngineService;
import notaql.extensions.dashboard.http.servlets.fromServer.JsonPollFromServerServlet;
import notaql.extensions.dashboard.http.servlets.fromServer.MessageConsoleAppend;
import notaql.extensions.dashboard.http.servlets.toServer.response.Response;
import notaql.extensions.dashboard.http.servlets.toServer.response.ResponseList;

/**
 * Processes the message with the request_type "get_all_engines".
 */
public class RequestProcessorGetAllEngines implements RequestProcessor {
	/**
	 * POJO-Class for GSON
	 */
	@SuppressWarnings("unused")
	class Engine {
		private String typeName;
		private List<String> arguments;
		private List<String> requiredArguments;
		private List<String> advisorArguments;
		
		
		// Needed by GSON
		private Engine() {}
		
		
		public Engine(notaql.engines.Engine engine) {
			this.typeName = engine.getEngineName();
			this.arguments = engine.getArguments();
			this.requiredArguments = engine.getRequiredArguments();
			this.advisorArguments = engine.getAdvisorArguments();
		}
	}
	
	
	// Class variables
	public static final String REQUEST_TYPE = "get_all_engines";
	private static Gson gson;
	
	
	// Object variables
	private Response response;
	

	@Override
	public String process(JSONObject json) {
		// If the response was not already generated we have to generate it
		if (this.response == null) {
			List<notaql.engines.Engine> engines = EngineService.getInstance().getEngines();
			
			if (engines.isEmpty())
				JsonPollFromServerServlet.addMessage(new MessageConsoleAppend("Warning: No engines found (Try Maven Install)"));
			
			
			List<Engine> enginesResponse = new ArrayList<Engine>(engines.size());
			for (notaql.engines.Engine engine : engines)
				enginesResponse.add(new Engine(engine));
			
			this.response = new ResponseList<Engine>(enginesResponse);
		}
		
		
		// New gson-object
		if (gson == null)
			gson = new Gson();
		
		
		// Generate return-json
		return gson.toJson(this.response);
	}
}
