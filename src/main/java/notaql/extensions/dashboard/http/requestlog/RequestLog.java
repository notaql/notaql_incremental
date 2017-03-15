package notaql.extensions.dashboard.http.requestlog;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;


/**
 * May be used later for logging requests.
 */
public class RequestLog {
	// Class variables
	private static List<LogEntry> logEntries = new ArrayList<LogEntry>(1000);
	
	
	public static void logMessage(String text, HttpServletRequest request) {
		final String requestIp = (request.getRemoteAddr() == null ? "" : request.getRemoteAddr());
		
		final String subText = !(requestIp.equals("") || requestIp.equals("localhost") || requestIp.equals("127.0.0.1") || requestIp.equals("::1") || requestIp.equals("0:0:0:0:0:0:0:1")) ? "IP: " + requestIp + ", " : "";
		
		logMessage(text, subText);
	}
	
	
	/**
	 * Log something.
	 * 
	 * @param text
	 * @param subText
	 */
	public static void logMessage(String text, String subText) {
		logEntries.add(new LogEntry(text, subText));
		
		System.out.println(text + " - " + subText);
	}
	
	
	/**
	 * Log something.
	 * 
	 * @param text
	 */
	public static void logMessage(String text) {
		logEntries.add(new LogEntry(text));
		
		System.out.println(text);
	}	
	
	
	/**
	 * Log an error.
	 * 
	 * @param text
	 * @param request
	 */
	public static void logError(String text, HttpServletRequest request) {
		final String requestIp = (request.getRemoteAddr() == null ? "" : request.getRemoteAddr());
		
		final String subText = !(requestIp.equals("") || requestIp.equals("localhost") || requestIp.equals("127.0.0.1") || requestIp.equals("::1") || requestIp.equals("0:0:0:0:0:0:0:1")) ? "IP: " + requestIp + ", " : "";
		
		logError(text, subText);
	}
	
	
	
	/**
	 * Log an error.
	 * 
	 * @param text
	 * @param subText
	 */
	public static void logError(String text, String subText) {
		logEntries.add(new LogEntry(text, subText, true));
		
		System.err.println(text + " - " + subText);
	}
	
	
	
	/**
	 * Log an error.
	 * 
	 * @param text
	 */
	public static void logError(String text) {
		logEntries.add(new LogEntry(text, null, true));
		
		System.err.println(text);
	}
}
