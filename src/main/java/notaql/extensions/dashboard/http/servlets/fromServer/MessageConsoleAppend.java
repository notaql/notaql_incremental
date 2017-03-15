package notaql.extensions.dashboard.http.servlets.fromServer;

/**
 * A text to be sent to the browser-console.
 */
@SuppressWarnings("unused")
public class MessageConsoleAppend extends Message {
	// Configuration
	private static final transient String message_type = "console_append";
	
	
	// Object variables
	private String text;
	
	
	// Needed by GSON
	private MessageConsoleAppend() { }
	
	
	public MessageConsoleAppend(String text) {
		super(message_type);
		
		this.text = text;
	}
	
	
	public String getText() {
		return this.text;
	}
}
