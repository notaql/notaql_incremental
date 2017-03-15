package notaql.extensions.dashboard.http.servlets.toServer.response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *  POJO-Class for GSON.
 */
@SuppressWarnings("unused")
public class ResponseMap<T1, T2> extends Response {
	private Map<T1, T2> map;
	

	// Needed by GSON
	private ResponseMap() { }
	
	
	public ResponseMap(Map<T1, T2> map) {
		this.map = map;
	}
}
