package notaql.extensions.dashboard.http.servlets.exceptions;

/**
 * Thrown if a json-request could not be processed.
 */
public class JsonProcessingException extends Exception {
	private static final long serialVersionUID = 1L;
	

	public JsonProcessingException() {
		super();
	}

	public JsonProcessingException(String message) {
		super(message);
	}

	public JsonProcessingException(Throwable cause) {
		super(cause);
	}

	public JsonProcessingException(String message, Throwable cause) {
		super(message, cause);
	}

	public JsonProcessingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
