package notaql.extensions.dashboard.http.template;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import notaql.extensions.dashboard.util.Util;

/**
 * Implementation of a simple template engine.
 */
public class TemplateEngine {
	private static final String startString = "$replace$";
	private static final String endString = "$/replace$";
	private static final Pattern regexPatternReplace = Pattern.compile("(?<=\\$replace\\$)[^\\$]+(?=\\$/replace\\$)");
	private static final String emptyString = "";
	
	
	String templateString;

	/**
	 * @param templateInputStream
	 * @throws IOException if the inputStream could not be read
	 */
	public TemplateEngine(Path pathTemplate) throws IOException {
		// Read the template from the stream
		this.templateString = new String(Files.readAllBytes(pathTemplate));
	}
	
	
	/**
	 * Returns the unrendered template.
	 * 
	 * @return the template without any modifications
	 */
	public String getTemplate() {
		return this.templateString;
	}
	
	
	/**
	 * Renders the template.
	 * 
	 * @param replacements maps each replacementVariable to a new String. If the new String is null the replacement will be empty. In the template replacements are encapsulated by "$replace$" and "$/replace$" and should not contain a "$"
	 * @return
	 */
	public String getRenderedTemplate(Map<String, String> replacements) {
		// Make a new copy of the map so we can alter the values without changing the original
		Map<String, String> replacementsNew = new HashMap<String, String>(replacements.size()*2);
		
		
		// Find all keys in the template
		Matcher keyMatcher = regexPatternReplace.matcher(templateString);
		while (keyMatcher.find()) {
			String key = keyMatcher.group();
			
			// If the key is not in the replacements-map we insert it with an empty string
			if (!replacements.containsKey(key))
				replacementsNew.put(startString + key + endString, emptyString);
			
			else {
				// If the mapping for the key is null we replace this mapping with an empty string
				String oldValue = replacements.get(key) == null ? emptyString : replacements.get(key);
			
				// We also have to remove this key from the replacements and insert a new key enclosed by the replacement-strings
				replacementsNew.put(startString + key + endString, oldValue);
			}
		}
		
		
		// Apply the replacements
		return Util.batchReplaceAll(replacementsNew, templateString);
	}
}