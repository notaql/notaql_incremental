/*
 * Copyright 2015 by Thomas Lottermann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package notaql.datamodel;

import java.util.regex.Pattern;

/**
 * Utility class which provides tools which should help handling values
 */
public class ValueUtils {
	public static final Pattern REGEX_PATTERN_NUMBER_VALUE_INTEGER = Pattern.compile("^-?\\d+$");
	public static final Pattern REGEX_PATTERN_NUMBER_VALUE_DOUBLE = Pattern.compile("^-?\\d+\\.\\d*$");
	public static final Pattern REGEX_PATTERN_NUMBER_VALUE_E_NOTATION= Pattern.compile("^-?\\d+\\.\\d+[Ee]\\d+$"); // HBase
	


	/**
	 * Parses the input as Integer, Double or String.
	 *
	 * @param input
	 * @return
	 */
	public static AtomValue<?> parse(String input) {
		if (input.equalsIgnoreCase("true"))
			return new BooleanValue(true);

		else if (input.equalsIgnoreCase("false"))
			return new BooleanValue(false);

		else if (REGEX_PATTERN_NUMBER_VALUE_INTEGER.matcher(input).matches())
			return new NumberValue(Integer.parseInt(input));

		else if (REGEX_PATTERN_NUMBER_VALUE_DOUBLE.matcher(input).matches() || REGEX_PATTERN_NUMBER_VALUE_E_NOTATION.matcher(input).matches())
			return new NumberValue(Double.parseDouble(input));
		else
			return new StringValue(input);
	}


	/**
	 * Provides the root value of the provided value
	 * @param value
	 * @return
	 */
	public static Value getRoot(Value value) {
		final Value parent = value.getParent();

		if(parent == null)
			return value;

		return getRoot(parent);
	}
	
	
	/**
	 * Converts the string to a Number
	 * 
	 * @param string
	 * @return
	 */
	public static Number stringToNumber(String string) {
		if (string == null)
			return null;
		
		else if (REGEX_PATTERN_NUMBER_VALUE_INTEGER.matcher(string).matches())
			return Integer.parseInt(string);

		else if (REGEX_PATTERN_NUMBER_VALUE_DOUBLE.matcher(string).matches())
			return Double.parseDouble(string);
		
		else
			throw new IllegalStateException("String is no Number");
	}
	
	
	/**
	 * Converts the string to a Boolean
	 * 
	 * @param string
	 * @return
	 */
	public static Boolean stringToBoolean(String string) {
		if (string == null)
			return null;
		
		else if (string.equalsIgnoreCase("true"))
			return true;

		else if (string.equalsIgnoreCase("false"))
			return false;
		
		else
			throw new IllegalStateException("Value is no boolean");
	}
}
