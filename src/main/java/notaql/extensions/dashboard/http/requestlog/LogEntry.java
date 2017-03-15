package notaql.extensions.dashboard.http.requestlog;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class LogEntry {
	// Class variables
	private static final DateFormat dateFormat = new SimpleDateFormat("EEE, MM/dd/yyyy HH:mm:ss");
	
	
	// Object variables
	final private Date date;
	final private boolean isError;
	final private String text;
	final private String subText;
	private String cachedToString;
	
	
	public LogEntry(String text) {
		this(text, null, false);
	}
	
	
	public LogEntry(String text, String subtext) {
		this(text, subtext, false);
	}
	
	
	public LogEntry(String text, String subtext, boolean isError) {
		this.date = Calendar.getInstance().getTime();
		this.isError = isError;
		this.text = text;
		this.subText = subtext;
	}


	public Date getDate() {
		return date;
	}
	
	
	public String getDateString() {
		return dateFormat.format(this.date);
	}


	public boolean isError() {
		return isError;
	}


	public String getText() {
		return text;
	}
	
	public String toString() {
		if (this.cachedToString == null) {
			this.cachedToString = this.getDateString() + " - " + this.text;
			
			if (this.subText != null)
				this.cachedToString += " (" + this.subText + ")";
		}
		
		return this.cachedToString;
	}
}
