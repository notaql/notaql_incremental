package notaql.engines.hbase.triggers.coprocessor;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

public class Util {
    /**
     * Executes a HTTP-POST Request
     * 
     * @param url
     * @param requestBody
     * @throws IOException
     */
    public static void doHttpPost(URL url, String requestBody) throws IOException {
		HttpURLConnection connection = null;
		OutputStreamWriter writer = null;
    	
		try {
			// Open the connection
    		connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setDoOutput(true);
			connection.setRequestProperty("Content-Length", String.valueOf(requestBody.length()));
	
			
			// Open the writer and write the request-body 
			writer = new OutputStreamWriter(connection.getOutputStream());
			writer.write(requestBody);
			writer.flush();
			
			
			// Even if we are not interested in the returned data we open the input stream in order to
			// make sure the request gets processed
			connection.getInputStream().close();
		}
    	
    	finally {
			if (connection != null)
				connection.disconnect();
			
			if (writer != null)
				writer.close();
		}
    }
}
