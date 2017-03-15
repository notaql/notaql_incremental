package notaql.extensions.dashboard.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class Util {
	/**
	 * Implementation of a fast replacement algorithm.
	 * 
	 * @param replacements
	 * @param inputString
	 * @return
	 */
	public static String batchReplaceAll(Map<String,String> replacements, String inputString) {
		// Use a string buffer for fast string-operations
		StringBuilder stringBuilder = new StringBuilder(inputString);
		
		for (String pattern : replacements.keySet())
			stringBuilderReplaceAll(stringBuilder, pattern, replacements.get(pattern));
		
		
		return stringBuilder.toString();
	}
	
	
	/**
	 * Replaces all occurences of 'pattern' with 'replacement'.
	 * 
	 * @param stringBuilder
	 * @param pattern
	 * @param replacement
	 * @return
	 */
	public static StringBuilder stringBuilderReplaceAll(StringBuilder stringBuilder, String pattern, String replacement) {
		int offset = 0;
		int indexOf;
		
		while ((indexOf = stringBuilder.indexOf(pattern, offset)) != -1) {
			offset = indexOf + pattern.length();
			
			stringBuilder.replace(indexOf, offset, replacement);
			offset = offset - (offset-indexOf);
		}		
		
		return stringBuilder;
	}
	

	/**
	 * Decodes a String which is base64 encoded. 
	 * 
	 * @param inputString the base64-string
	 * @return the string
	 */
	public static String base64decodeString(String inputString) {
		if (inputString == null)
			return "";
		
		return new String(Base64.getDecoder().decode(inputString));
	}
	

	/**
	 * Encodes a String with base64. 
	 * 
	 * @param inputString
	 * @return the base64-string
	 */
	public static String base64encodeString(String inputString) {
		if (inputString == null)
			return "";
		
		return new String(Base64.getEncoder().encode(inputString.getBytes(StandardCharsets.UTF_8)));
	}
}
