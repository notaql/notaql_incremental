package notaql.extensions.dashboard.http.servlets.fromServer;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects the messages to be send to the browser.
 */
public class Messages {
	private List<Message> messages = new ArrayList<Message>();
	
	
	// Needed by GSON
	public Messages() {	}
	
	
	/**
	 * Clears the message-list from the old messages.
	 * 
	 * To be called after JSON-serialization.
	 */
	public void clearMessages() {
		this.messages.clear();
	}
	
	
	/**
	 * Adds a message to the container.
	 * 
	 * @param message
	 */
	public void addMessage(Message message) {
		this.messages.add(message);
	}
    
    
    /**
     * Gets all messages
     * 
     * @return
     */
	public List<Message> getMessages() {
		return this.messages;
	}
}
