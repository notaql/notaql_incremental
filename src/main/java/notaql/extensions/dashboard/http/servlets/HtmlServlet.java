package notaql.extensions.dashboard.http.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import notaql.extensions.dashboard.http.requestlog.RequestLog;
import notaql.extensions.dashboard.http.template.TemplateEngine;

/**
 * Base-class for all *html* servlets.
 */
public abstract class HtmlServlet extends HttpServlet {
	// Constants
	private static final long serialVersionUID = 1L;
	
	
	// Class variables
	protected static Map<String, String> basicRequestTemplateReplacements; // Used for requests with no parameters
	
	
	// Object variables
	protected TemplateEngine template;
	protected String basicRequestTemplaceRenderedHtml; // Used for requests with no parameters
	
	
	/**
	 * @return the path to the template file
	 * 
	 * 
	 * Shall be overwritten in the subclass.
	 */
	protected abstract String getTemplatePath();


	/**
     * @throws ServletException if init fails 
     */
	public HtmlServlet() throws ServletException {
        super();
        super.init();
	}
    
    
    /* (non-Javadoc)
     * @see javax.servlet.GenericServlet#init()
     */
    @Override
    public void init() {
		// Load the template
		if (this.template == null) {
			// Get the context
	        final ServletContext context = getServletContext();
	        
	        
	        // Load the template
	        try {
				this.template = new TemplateEngine(Paths.get(context.getRealPath(this.getTemplatePath())));
			} catch (IOException e) {
				// If this happens we have other problems
				e.printStackTrace();
				RequestLog.logError("[" + this.getClass().getSimpleName() + "] IO-Exception while loading the template: '" + e.getMessage() + "'");
			}

	        
	        // Init the map for the basic request. It has always the same values.
	        basicRequestTemplateReplacements = new HashMap<String, String>();
	        basicRequestTemplateReplacements.put("basepath", context.getContextPath() + "/");
	        
	        
	        // Render the basic html-page (without request-parameters)
	        Map<String, String> intialHtmlPage = new HashMap<String, String>(basicRequestTemplateReplacements);
	        this.basicRequestTemplaceRenderedHtml = this.template.getRenderedTemplate(intialHtmlPage);
		}
    }

    
	/**
	 * Gets called on a GET-Requests
	 *
	 * @param request the request
	 * @param response the response
	 * 
	 * @throws ServletException the servlet exception
	 * @throws IOException Signals that an I/O exception has occurreds.
	 * 
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// Log the request
		RequestLog.logMessage("[" + this.getClass().getSimpleName() + "] Incoming HTML-Request");
				
		
		// Various variables
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/html");
		
		
		// Generate the response
		String responseString;
		try {
			responseString = this.renderResponse(request);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			RequestLog.logError("[" + this.getClass().getSimpleName() + "] Exception while serving the request: '" + e.getMessage() + "'");
			responseString = e.getMessage();
		}
		
		
		// Return the response
		PrintWriter writer = response.getWriter();
		writer.write(responseString);
		writer.close();
	}

    
	/*
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		request.setCharacterEncoding("UTF-8");
		
		this.doGet(request, response);
	}
	
	
	/**
	 * Renders the response for a single request (overwrite this in the subclass).
	 * 
	 * @param request
	 * @return
	 */
	protected abstract String renderResponse(HttpServletRequest request);
}
