package notaql.extensions.notaql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

public class Configuration {
	// Configuration
	
	// With Tomcat the first one works, with JUnits the second one works..
//	private static final String NOTAQL_SETTINGS_FILENAME_1 = "settings.config"; // Local test config
	private static final String NOTAQL_SETTINGS_FILENAME_1 = "settings.ahr.config"; // Distributed over hnodes
//	private static final String NOTAQL_SETTINGS_FILENAME_1 = "settings.ahr_local.config"; // Spark is not distributed
	private static final String NOTAQL_SETTINGS_FILENAME_2 = "resources/" + NOTAQL_SETTINGS_FILENAME_1;
	
	
	// Object variables
	private String filename;
	

	
	/**
	 * @return the path of the settings-file
	 * @throws FileNotFoundException 
	 */
	public String getPath() throws FileNotFoundException {
		if (filename != null)
			return getPath(filename);
		
		try {
			return getPath(NOTAQL_SETTINGS_FILENAME_1);
		} catch (FileNotFoundException e) {
			return getPath(NOTAQL_SETTINGS_FILENAME_2);
		}
	}

	
	/**
	 * @return the stream for the settings-file
	 * @throws FileNotFoundException 
	 */
	public InputStream getStream() throws FileNotFoundException {
		if (filename != null)
			return getStream(filename); 
		
		try {
			return getStream(NOTAQL_SETTINGS_FILENAME_1);
		} catch (FileNotFoundException e) {
			return getStream(NOTAQL_SETTINGS_FILENAME_2);
		}
	}


	/**
	 * Generates the config file from the map
	 * 
	 * @param configMap
	 * @throws IOException 
	 */
	public void setConfig(Map<String, String> configMap) throws IOException {
		// Generate the properties
		Properties newProperties = new Properties();
		newProperties.putAll(configMap);
		
		
		// Store the properties in the file
		FileOutputStream outStream = new FileOutputStream(new File(getPath()));
		try {
			newProperties.store(outStream, "NotaQL config file");
		}
		finally {
			outStream.close();
		}
	}
	
	
	/**
	 * Utility function for getting a filepath from inside a JAR.
	 * 
	 * @param filename
	 * @return
	 * @throws FileNotFoundException
	 */
	private String getPath(String filename) throws FileNotFoundException {
		URL resourceUrl = Configuration.class.getClassLoader().getResource(filename);
		
		if (resourceUrl == null)
			throw new FileNotFoundException("Could not find the notaql-settings-file");
		else {
			this.filename = filename;
			return resourceUrl.getPath();
		}
	}
	
	
	/**
	 * Utility function for getting a stream from inside a JAR.
	 * 
	 * @param filename
	 * @return
	 * @throws FileNotFoundException
	 */
	private InputStream getStream(String filename) throws FileNotFoundException {
		InputStream inputStream = Configuration.class.getClassLoader().getResourceAsStream(filename);
		
		if (inputStream == null)
			throw new FileNotFoundException("Could not find the notaql-settings-file");
		else {
			this.filename = filename;
			return inputStream;
		}
	}
}
