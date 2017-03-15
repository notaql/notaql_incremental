package notaql.extensions.dashboard.http.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;

/**
 * Implementation for the web-based GUI for the notaql-processor.
 */
@WebServlet({"/dashboard", ""})
public class HtmlDashboardServlet extends HtmlServlet {
	// Constants
	private static final long serialVersionUID = 1L;
	
	
	/**
	 * @return the path to the template file
	 */
	protected String getTemplatePath() {
		return "/WEB-INF/templates/TemplateDashboardServlet.html";
	}
	
    
    /**
     * @throws IOException if the html-template could not be loaded 
     * @throws ServletException if init fails
     */
    public HtmlDashboardServlet() throws IOException, ServletException {
        super();
    }


	/* (non-Javadoc)
	 * @see pt.http.servlets.HtmlServlet#htmlRenderResponse(javax.servlet.http.HttpServletRequest)
	 */
	@Override
	protected String renderResponse(HttpServletRequest request) {
		// At the moment there is no parameter implemented, so we always serve the already rendered "basic template"
		return this.basicRequestTemplaceRenderedHtml;
	}
}