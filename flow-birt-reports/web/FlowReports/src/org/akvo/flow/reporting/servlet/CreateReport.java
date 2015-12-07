package org.akvo.flow.reporting.servlet;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Servlet implementation class ListReports
 */
@WebServlet(
		description = "Output a list of defined reports", 
		urlPatterns = { 
				"/CreateReport", 
				"/create_report"
		})
public class CreateReport extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CreateReport() {
        super();
    }


    
    String expandTemplate(String metaTemplate) {
        File mt = new File(metaTemplate);
        
        return "Expansion!";
    }
    
    
    
    
    void parameterError(HttpServletResponse response) throws IOException {
        // Set response content type
        response.setStatus(400);
        response.setContentType("text/plain");
        PrintWriter out = response.getWriter();
        out.println("You must provide an integer value for the form_id parameter");
    }
    
    void getAsHtml(HttpServletResponse response, int id) throws IOException, SQLException {
        // Set response content type
        response.setContentType("text/html");//TODO make a constant
        PrintWriter out = response.getWriter();
        String title = "Report successfully defined for form " + id ;
        String docType = "<!doctype html public \"-//w3c//dtd html 4.0 "
                + "transitional//en\">\n";
        out.println(docType + "<html>\n" + "<head><title>" + title
                + "</title></head>\n" + "<body bgcolor=\"#f0f0f0\">\n"
                + "<h1 align=\"center\">" + title + "</h1>\n"); 
        
         out.println("</body></html>");
    }
    
    //JSON jar was fetched from Maven Central as described in https://github.com/eskatos/org.json-java
    
    void getAsJson(HttpServletResponse response, int id) throws IOException, SQLException {
        //build JSON structure
        JSONObject root = new JSONObject();
        root.put("id", id);
        
        // Set response content type
        response.setContentType("application/json");//TODO make a constant
        PrintWriter out = response.getWriter();
               
        out.print(root.toString());
    }
    
  
    public static Map<String, String> getQueryMap(String query)
    {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params)
        {
            String name = param.split("=")[0].toLowerCase();
            String value = param.split("=")[1];
            map.put(name, value);
        }
        return map;
    }
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	    
	    //form id parameter
	    int form_id = 0;
	    Map<String, String> params;
	    try {
	        params = getQueryMap(request.getQueryString());
	        String form_id_str = params.get("form_id");
	        form_id = Integer.parseInt(form_id_str);
	    }
	    catch (Exception e) {
	        parameterError(response);
	        return;
	    }

	    String metaTemplate = getServletContext().getInitParameter("meta_template");
	    
	    //expand the template
	    String template = expandTemplate(metaTemplate);
	    
        // JDBC driver name and database URL
        final String JDBC_DRIVER = "org.postgresql.Driver"; //
        final String DB_URL = "jdbc:postgresql://localhost:1234/flowtestrep"; //

        // Database credentials
        final String USER = "flowtestrep";
        final String PASS = "snippsnappsnurr";


        // Register JDBC driver
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

        // Open a connection
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            // Execute SQL query
            String sql;
            //Postgres specific: INSERT INTO distributors (did, dname) VALUES (DEFAULT, 'XYZ Widgets') RETURNING did;

            sql = "INSERT INTO birt_template (id, form_id, parameters, template) VALUES (DEFAULT, ?, ?) returning id"; 
            stmt = conn.prepareStatement(sql);
            stmt.setInt(0, form_id);
            stmt.setString(1, "Some parameters, later..."); //avoid injection attacks...
            stmt.setString(2, template); //avoid quoting problems
            
            ResultSet rs = stmt.executeQuery(sql);
            int report_id = rs.getInt(0); //there can be only one

            //pick output format
            if (params.get("format") != null && params.get("format").equalsIgnoreCase("json")) {
                getAsJson(response, report_id);
            } else {
                getAsHtml(response, report_id);
            }
            
            // Clean-up environment
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            // Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            // finally block used to close resources
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }// end finally try
        } // end try
    
	}

}
