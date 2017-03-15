package notaql.extensions.dashboard.http.servlets.fromServer;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

import com.google.gson.Gson;

import notaql.extensions.dashboard.http.servlets.JsonServlet;

/**
 * Used for polling data from the server to the client (=browser).
 * 
 * All updates for the browser-view are packed into one json-message (e.g. {"messages":[{"text":"1462961109","message_type":"console_append"}]} ).
 */
@WebServlet({"/from_server"})
public class JsonPollFromServerServlet extends JsonServlet {
	// Constants
	private static final long serialVersionUID = 1L;
	
	
	// Class variables
	private static Gson gson;
	private static Messages messagesContainer = new Messages();
	
    
    /**
     * @throws IOException
     * @throws ServletException if init fails
     */
    public JsonPollFromServerServlet() throws IOException, ServletException {
        super();
    }
    
    
    /**
     * Gets all messages.
     * 
     * @return
     */
    public static List<Message> getMessages() {
    	return messagesContainer.getMessages();
    }
    
    
    /**
     * Prints all text-messages.
     */
    public static void printTextMessages() {
		for (Message message : getMessages()) {
			if (message instanceof MessageConsoleAppend)
				System.out.println(((MessageConsoleAppend) message).getText());
		}
		messagesContainer.clearMessages();
    }
    
    
    /**
     * Adds a message to the message container.
     * 
     * @param message A general message
     */
    public static void addMessage(Message message) {
    	messagesContainer.addMessage(message);
    }
    
    
    /**
     * Adds a message to the message container.
     * 
     * @param message A text
     */
    public static void addMessage(Object message) {
    	try {
    		messagesContainer.addMessage(new MessageConsoleAppend(message.toString()));
    	} catch (Exception e) {
    		messagesContainer.addMessage(new MessageConsoleAppend("An object failed at being converted into a message"));
    	}
    }


	/* (non-Javadoc)
	 * @see pt.http.servlets.JsonServlet#renderResponse(javax.servlet.http.HttpServletRequest, java.lang.String)
	 */
	@Override
	protected synchronized String renderResponse(HttpServletRequest request, String jsonString) {
		// Check if gson is initialized
		if (gson == null) {
			gson = new Gson();
			
			addMessage("Server gestartet");
		}
		
		
		// Get the message container, generate the json and then clear the container
		final String returnJson = gson.toJson(messagesContainer);
		messagesContainer.clearMessages();
		
		
		return returnJson;
	}
}