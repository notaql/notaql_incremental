package notaql.extensions.dashboard.http.servlets.toServer.response;

import java.util.ArrayList;
import java.util.List;

/**
 *  POJO-Class for GSON.
 */
@SuppressWarnings("unused")
public class ResponseList<T> extends Response {
	private List<T> list;
	

	// Needed by GSON
	private ResponseList() { }
	
	
	public ResponseList(List<T> list) {
		this.list = list;
	}
}
