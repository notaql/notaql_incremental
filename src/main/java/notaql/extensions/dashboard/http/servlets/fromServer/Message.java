package notaql.extensions.dashboard.http.servlets.fromServer;

/**
 * General type for messages.
 */
@SuppressWarnings("unused")
public abstract class Message {
	private String message_type;

	
	// Needed by GSON
	protected Message() { }
	
	
	public Message(String message_type) {
		this.message_type = message_type;
	}
}
